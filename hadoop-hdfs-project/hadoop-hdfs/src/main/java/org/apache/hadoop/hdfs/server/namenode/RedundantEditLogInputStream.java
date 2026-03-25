// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Longs;
import org.apache.hadoop.log.LogThrottlingHelper;
import org.apache.hadoop.log.LogThrottlingHelper.LogAction;

/**
 * 文件说明: HDFS NameNode 冗余编辑日志合并输入流，支持在多个冗余编辑日志流之间进行故障转移，
 * 当当前读取的编辑日志流出现错误时，自动切换到下一个冗余流继续读取，保证编辑日志读取的可靠性。
 * 主要用于支持日志的多副本冗余，在JournalNode集群等多日志副本场景下，实现高可用的日志读取。
 *
 * A merged input stream that handles failover between different edit logs.
 *
 * We will currently try each edit log stream exactly once.  In other words, we
 * don't handle the "ping pong" scenario where different edit logs contain a
 * different subset of the available edits.
 */
/**
 * 冗余编辑日志输入流，整合多个相同起始事务ID的冗余日志流，支持故障切换继续读取
 */
class RedundantEditLogInputStream extends EditLogInputStream {
  public static final Logger LOG = LoggerFactory.getLogger(
      RedundantEditLogInputStream.class.getName());
  private int curIdx;
  private long prevTxId;
  private final EditLogInputStream[] streams;

  /** Limit logging about fast forwarding the stream to every 5 seconds max. */
  private static final long FAST_FORWARD_LOGGING_INTERVAL_MS = 5000;
  // 限流日志帮助类，限制快进日志的打印频率，最多每5秒打印一次
  private static final LogThrottlingHelper FAST_FORWARD_LOGGING_HELPER =
      new LogThrottlingHelper(FAST_FORWARD_LOGGING_INTERVAL_MS);

  /**
   * States that the RedundantEditLogInputStream can be in.
   *
   * <pre>
   *                   start (if no streams)
   *                           |
   *                           V
   * PrematureEOFException  +----------------+
   *        +-------------->| EOF            |<--------------+
   *        |               +----------------+               |
   *        |                                                |
   *        |          start (if there are streams)          |
   *        |                  |                             |
   *        |                  V                             | EOF
   *        |   resync      +----------------+ skipUntil  +---------+
   *        |   +---------->| SKIP_UNTIL     |----------->|  OK     |
   *        |   |           +----------------+            +---------+
   *        |   |                | IOE   ^ fail over to      | IOE
   *        |   |                V       | next stream       |
   * +----------------------+   +----------------+           |
   * | STREAM_FAILED_RESYNC |   | STREAM_FAILED  |<----------+
   * +----------------------+   +----------------+
   *                  ^   Recovery mode    |
   *                  +--------------------+
   * </pre>
   */
  /**
   * 流状态枚举，定义冗余输入流的所有可能状态和状态转换关系
   */
  static private enum State {
    /** 需要跳转到 prevTxId + 1 事务位置 */
    SKIP_UNTIL,
    /** 已就绪，可以从当前流读取操作码 */
    OK,
    /** 当前流读取失败，等待切换 */
    STREAM_FAILED,
    /** 当前流失败，已经调用过resync()尝试恢复 */
    STREAM_FAILED_RESYNC,
    /** 已经没有更多操作码可读取，到达流末尾 */
    EOF;
  }

  private State state;
  private IOException prevException;

  /**
   * 构造冗余编辑日志输入流，校验输入流合法性并按结束事务ID排序
   * @param streams 多个冗余编辑日志输入流集合，所有流必须有相同的起始事务ID
   * @param startTxId 开始读取的起始事务ID
   */
  RedundantEditLogInputStream(Collection<EditLogInputStream> streams,
      long startTxId) {
    this.curIdx = 0;
    this.prevTxId = (startTxId == HdfsServerConstants.INVALID_TXID) ?
      HdfsServerConstants.INVALID_TXID : (startTxId - 1);
    this.state = (streams.isEmpty()) ? State.EOF : State.SKIP_UNTIL;
    this.prevException = null;
    // EditLogInputStreams in a RedundantEditLogInputStream must be finalized,
    // and can't be pre-transactional.
    EditLogInputStream first = null;
    // 校验所有输入流，确保所有流起始事务ID一致
    for (EditLogInputStream s : streams) {
      Preconditions.checkArgument(s.getFirstTxId() !=
          HdfsServerConstants.INVALID_TXID, "invalid first txid in stream: %s", s);
      Preconditions.checkArgument(s.getLastTxId() !=
          HdfsServerConstants.INVALID_TXID, "invalid last txid in stream: %s", s);
      if (first == null) {
        first = s;
      } else {
        Preconditions.checkArgument(s.getFirstTxId() == first.getFirstTxId(),
          "All streams in the RedundantEditLogInputStream must have the same " +
          "start transaction ID!  " + first + " had start txId " +
          first.getFirstTxId() + ", but " + s + " had start txId " +
          s.getFirstTxId());
      }
    }

    this.streams = streams.toArray(new EditLogInputStream[0]);

    // 按结束事务ID从大到小排序，优先使用更长的流，保证能读到更多事务
    Arrays.sort(this.streams, new Comparator<EditLogInputStream>() {
      @Override
      public int compare(EditLogInputStream a, EditLogInputStream b) {
        return Longs.compare(b.getLastTxId(), a.getLastTxId());
      }
    });
  }

  @Override
  public String getCurrentStreamName() {
    return streams[curIdx].getCurrentStreamName();
  }

  @Override
  public String getName() {
    StringBuilder bld = new StringBuilder();
    String prefix = "";
    for (EditLogInputStream elis : streams) {
      bld.append(prefix)
          .append(elis.getName());
      prefix = ", ";
    }
    return bld.toString();
  }

  @Override
  public long getFirstTxId() {
    return streams[curIdx].getFirstTxId();
  }

  @Override
  public long getLastTxId() {
    return streams[curIdx].getLastTxId();
  }

  @Override
  public void close() throws IOException {
    IOUtils.cleanupWithLogger(LOG,  streams);
  }

  @Override
  protected FSEditLogOp nextValidOp() {
    try {
      if (state == State.STREAM_FAILED) {
        state = State.STREAM_FAILED_RESYNC;
      }
      return nextOp();
    } catch (IOException e) {
      LOG.warn("encountered an exception", e);
      return null;
    }
  }

  @Override
  protected FSEditLogOp nextOp() throws IOException {
    while (true) {
      switch (state) {
      case SKIP_UNTIL:
       try {
          if (prevTxId != HdfsServerConstants.INVALID_TXID) {
            // 记录日志，按限流规则判断是否需要打印
            LogAction logAction = FAST_FORWARD_LOGGING_HELPER.record();
            if (logAction.shouldLog()) {
              LOG.info("Fast-forwarding stream '" + streams[curIdx].getName() +
                  "' to transaction ID " + (prevTxId + 1) +
                  LogThrottlingHelper.getLogSupressionMessage(logAction));
            }
            // 跳转到目标事务位置
            streams[curIdx].skipUntil(prevTxId + 1);
          }
        } catch (IOException e) {
          // 跳转失败，标记当前流为失败
          prevException = e;
          state = State.STREAM_FAILED;
          LOG.warn("Got error skipUntil edit log input stream {}.", streams[curIdx].getName());
          break;
        }
        // 跳转成功，切换到就绪状态
        state = State.OK;
        break;
      case OK:
        try {
          // 从当前流读取下一个编辑操作
          FSEditLogOp op = streams[curIdx].readOp();
          if (op == null) {
            // 到达流末尾
            state = State.EOF;
            // 如果已经读到期望的最后事务，返回空表示读取完成
            if (streams[curIdx].getLastTxId() == prevTxId) {
              return null;
            } else {
              // 未到期望结束位置就提前结束，抛出提前EOF异常
              throw new PrematureEOFException("got premature end-of-file " +
                  "at txid " + prevTxId + "; expected file to go up to " +
                  streams[curIdx].getLastTxId());
            }
          }
          // 更新上一个读到的事务ID
          prevTxId = op.getTransactionId();
          return op;
        } catch (IOException e) {
          // 读取异常，标记当前流为失败
          prevException = e;
          state = State.STREAM_FAILED;
        }
        break;
      case STREAM_FAILED:
        // 已经没有更多流可用，抛出之前保存的异常
        if (curIdx + 1 == streams.length) {
          throw prevException;
        }
        long oldLast = streams[curIdx].getLastTxId();
        long newLast = streams[curIdx + 1].getLastTxId();
        // 检查下一个流是否比当前流更短，如果更短则意味着会丢失元数据，直接抛出异常
        if (newLast < oldLast) {
          throw new IOException("We encountered an error reading " +
              streams[curIdx].getName() + ".  During automatic edit log " +
              "failover, we noticed that all of the remaining edit log " +
              "streams are shorter than the current one!  The best " +
              "remaining edit log ends at transaction " +
              newLast + ", but we thought we could read up to transaction " +
              oldLast + ".  If you continue, metadata will be lost forever!",
              prevException);
        }
        LOG.error("Got error reading edit log input stream " +
          streams[curIdx].getName() + "; failing over to edit log " +
          streams[curIdx + 1].getName(), prevException);
        // 切换到下一个流，进入跳转状态
        curIdx++;
        state = State.SKIP_UNTIL;
        break;
      case STREAM_FAILED_RESYNC:
        if (curIdx + 1 == streams.length) {
          // 已经是最后一个流，如果是提前EOF则直接结束，否则尝试重新同步
          if (prevException instanceof PrematureEOFException) {
            // bypass early EOF check
            state = State.EOF;
          } else {
            streams[curIdx].resync();
            state = State.SKIP_UNTIL;
          }
        } else {
          // 还有剩余流，直接切换到下一个
          LOG.error("failing over to edit log " +
              streams[curIdx + 1].getName());
          curIdx++;
          state = State.SKIP_UNTIL;
        }
        break;
      case EOF:
        // 已经到达末尾，返回null
        return null;
      }
    }
  }

  @Override
  public int getVersion(boolean verifyVersion) throws IOException {
    return streams[curIdx].getVersion(verifyVersion);
  }

  @Override
  public long getPosition() {
    return streams[curIdx].getPosition();
  }

  @Override
  public long length() throws IOException {
    return streams[curIdx].length();
  }

  @Override
  public boolean isInProgress() {
    return streams[curIdx].isInProgress();
  }

  /**
   * 提前EOF异常，当编辑日志流未到达预期的最后事务ID就提前结束时抛出
   */
  static private final class PrematureEOFException extends IOException {
    private static final long serialVersionUID = 1L;
    PrematureEOFException(String msg) {
      super(msg);
    }
  }

  @Override
  public void setMaxOpSize(int maxOpSize) {
    for (EditLogInputStream elis : streams) {
      elis.setMaxOpSize(maxOpSize);
    }
  }

  @Override
  public boolean isLocalLog() {
    return streams[curIdx].isLocalLog();
  }
}