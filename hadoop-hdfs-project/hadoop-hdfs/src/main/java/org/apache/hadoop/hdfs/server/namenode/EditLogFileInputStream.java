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

import org.apache.hadoop.thirdparty.protobuf.ByteString;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.security.PrivilegedExceptionAction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.LayoutFlags;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HttpGetFailedException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.base.Throwables;

/**
 * 文件级编辑日志输入流实现，支持从本地磁盘文件、URL地址或内存ByteString读取HDFS名称节点编辑日志。
 * 是EditLogInputStream的具体实现类，负责提供编辑日志文件的读取能力。
 */
@InterfaceAudience.Private
public class EditLogFileInputStream extends EditLogInputStream {
  private final LogSource log;
  private final long firstTxId;
  private final long lastTxId;
  private final boolean isInProgress;
  private int maxOpSize;
  /**
   * 输入流状态枚举，标识当前流的生命周期阶段
   */
  static private enum State {
    UNINIT,
    OPEN,
    CLOSED
  }
  private State state = State.UNINIT;
  private int logVersion = 0;
  private FSEditLogOp.Reader reader = null;
  private FSEditLogLoader.PositionTrackingInputStream tracker = null;
  private DataInputStream dataIn = null;
  static final Logger LOG = LoggerFactory.getLogger(EditLogInputStream.class);
  
  /**
   * 打开指定文件的编辑日志输入流，适用于预事务格式（无事务ID标识）的日志文件
   * @param name 要打开的日志文件
   * @throws LogHeaderCorruptException 如果日志头缺失或损坏
   * @throws IOException 如果读取日志头时发生IO错误
   */
  EditLogFileInputStream(File name)
      throws LogHeaderCorruptException, IOException {
    this(name, HdfsServerConstants.INVALID_TXID, HdfsServerConstants.INVALID_TXID, false);
  }

  /**
   * 打开指定文件的编辑日志输入流，指定包含的事务ID范围
   * @param name 要打开的日志文件
   * @param firstTxId 文件中第一个事务的ID
   * @param lastTxId 文件中最后一个事务的ID
   * @param isInProgress 日志是否处于写入中状态
   */
  public EditLogFileInputStream(File name, long firstTxId, long lastTxId,
      boolean isInProgress) {
    this(new FileLog(name), firstTxId, lastTxId, isInProgress);
  }
  
  /**
   * 从指定URL创建编辑日志输入流
   *
   * @param connectionFactory 创建连接的URL连接工厂
   * @param url 日志文件的URL地址
   * @param startTxId 期望的起始事务ID
   * @param endTxId 期望的结束事务ID
   * @param inProgress 日志是否处于写入中状态
   * @return 可读取编辑日志的输入流
   */
  public static EditLogInputStream fromUrl(
      URLConnectionFactory connectionFactory, URL url, long startTxId,
      long endTxId, boolean inProgress) {
    return new EditLogFileInputStream(new URLLog(connectionFactory, url),
        startTxId, endTxId, inProgress);
  }

  /**
   * 从内存ByteString创建编辑日志输入流
   *
   * @param bytes 要读取的字节数据
   * @param startTxId 期望的起始事务ID
   * @param endTxId 期望的结束事务ID
   * @param inProgress 日志是否处于写入中状态
   * @return 可读取编辑日志的输入流
   */
  public static EditLogInputStream fromByteString(ByteString bytes,
      long startTxId, long endTxId, boolean inProgress) {
    return new EditLogFileInputStream(new ByteStringLog(bytes,
        String.format("ByteStringEditLog[%d, %d]", startTxId, endTxId)),
        startTxId, endTxId, inProgress);
  }
  
  /**
   * 通用构造函数，基于Log源创建编辑日志输入流
   * @param log 日志源实现
   * @param firstTxId 文件中第一个事务的ID
   * @param lastTxId 文件中最后一个事务的ID
   * @param isInProgress 日志是否处于写入中状态
   */
  private EditLogFileInputStream(LogSource log,
      long firstTxId, long lastTxId,
      boolean isInProgress) {
      
    this.log = log;
    this.firstTxId = firstTxId;
    this.lastTxId = lastTxId;
    this.isInProgress = isInProgress;
    this.maxOpSize = DFSConfigKeys.DFS_NAMENODE_MAX_OP_SIZE_DEFAULT;
  }

  /**
   * 初始化输入流，读取并验证日志头，准备读取编辑操作
   * @param verifyLayoutVersion 是否验证日志布局版本
   * @throws LogHeaderCorruptException 如果日志头损坏
   * @throws IOException 如果发生IO错误
   */
  private void init(boolean verifyLayoutVersion)
      throws LogHeaderCorruptException, IOException {
    Preconditions.checkState(state == State.UNINIT);
    BufferedInputStream bin = null;
    InputStream fStream = null;
    try {
      // 从日志源获取输入流
      fStream = log.getInputStream();
      bin = new BufferedInputStream(fStream);
      // 创建位置跟踪流，用于记录当前读取位置
      tracker = new FSEditLogLoader.PositionTrackingInputStream(bin);
      dataIn = new DataInputStream(tracker);
      try {
        // 读取日志版本号
        logVersion = readLogVersion(dataIn, verifyLayoutVersion);
      } catch (EOFException eofe) {
        throw new LogHeaderCorruptException("No header found in log");
      }
      // 日志版本为-1表示文件预分配后未写入头，视为损坏空日志
      if (logVersion == -1) {
        // 正在写入的编辑日志创建时会预分配1MB空间用-1填充，如果创建过程中出现异常，头不会被写入，此时整个日志是损坏空日志
        throw new LogHeaderCorruptException("No header present in log (value " +
            "is -1), probably due to disk space issues when it was created. " +
            "The log has no transactions and will be sidelined.");
      }
      // 支持布局标志特性的版本需要读取布局标志
      if (NameNodeLayoutVersion.supports(
          LayoutVersion.Feature.ADD_LAYOUT_FLAGS, logVersion) ||
          logVersion < NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION) {
        try {
          LayoutFlags.read(dataIn);
        } catch (EOFException eofe) {
          throw new LogHeaderCorruptException("EOF while reading layout " +
              "flags from log");
        }
      }
      // 创建编辑操作读取器
      reader = FSEditLogOp.Reader.create(dataIn, tracker, logVersion);
      reader.setMaxOpSize(maxOpSize);
      state = State.OPEN;
    } finally {
      // 初始化失败则清理所有打开的流
      if (reader == null) {
        IOUtils.cleanupWithLogger(LOG, dataIn, tracker, bin, fStream);
        state = State.CLOSED;
      }
    }
  }

  @Override
  public long getFirstTxId() {
    return firstTxId;
  }
  
  @Override
  public long getLastTxId() {
    return lastTxId;
  }

  @Override
  public String getName() {
    return log.getName();
  }

  /**
   * 读取下一个编辑操作的内部实现，支持跳过损坏的编辑
   * @param skipBrokenEdits 是否跳过损坏的编辑
   * @return 下一个编辑操作，到达末尾或跳过损坏时返回null
   * @throws IOException 如果读取发生错误
   */
  private FSEditLogOp nextOpImpl(boolean skipBrokenEdits) throws IOException {
    FSEditLogOp op = null;
    switch (state) {
    case UNINIT:
      try {
        // 未初始化则先初始化
        init(true);
      } catch (Throwable e) {
        LOG.error("caught exception initializing " + this, e);
        if (skipBrokenEdits) {
          return null;
        }
        Throwables.propagateIfPossible(e, IOException.class);
      }
      Preconditions.checkState(state != State.UNINIT);
      // 初始化完成后递归读取下一个操作
      return nextOpImpl(skipBrokenEdits);
    case OPEN:
      // 从读取器读取下一个操作
      op = reader.readOp(skipBrokenEdits);
      if ((op != null) && (op.hasTransactionId())) {
        long txId = op.getTransactionId();
        if ((txId >= lastTxId) &&
            (lastTxId != HdfsServerConstants.INVALID_TXID)) {
          // 名称节点崩溃可能导致未完成日志末尾有垃圾数据，JournalManager恢复后会设置正确的最后事务ID，此处跳过超出范围的垃圾数据
          long skipAmt = log.length() - tracker.getPos();
          if (skipAmt > 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("skipping " + skipAmt + " bytes at the end " +
                  "of edit log  '" + getName() + "': reached txid " + txId +
                  " out of " + lastTxId);
            }
            tracker.clearLimit();
            IOUtils.skipFully(tracker, skipAmt);
          }
        }
      }
      break;
      case CLOSED:
        break; // return null
    }
    return op;
  }

  @Override
  protected long scanNextOp() throws IOException {
    Preconditions.checkState(state == State.OPEN);
    FSEditLogOp cachedNext = getCachedOp();
    // 有缓存则直接返回缓存事务ID，否则扫描下一个操作
    return cachedNext == null ? reader.scanOp() : cachedNext.txid;
  }

  @Override
  protected FSEditLogOp nextOp() throws IOException {
    // 不跳过损坏编辑，遇到错误直接抛出
    return nextOpImpl(false);
  }

  @Override
  protected FSEditLogOp nextValidOp() {
    try {
      // 跳过损坏编辑，返回下一个有效操作
      return nextOpImpl(true);
    } catch (Throwable e) {
      LOG.error("nextValidOp: got exception while reading " + this, e);
      return null;
    }
  }

  @Override
  public int getVersion(boolean verifyVersion) throws IOException {
    if (state == State.UNINIT) {
      // 未初始化则先初始化
      init(verifyVersion);
    }
    return logVersion;
  }

  @Override
  public long getPosition() {
    if (state == State.OPEN) {
      // 返回当前读取位置
      return tracker.getPos();
    } else {
      return 0;
    }
  }

  @Override
  public void close() throws IOException {
    if (state == State.OPEN) {
      // 关闭输入流
      dataIn.close();
    }
    state = State.CLOSED;
  }

  @Override
  public long length() throws IOException {
    // 返回日志总长度
    return log.length();
  }
  
  @Override
  public boolean isInProgress() {
    return isInProgress;
  }
  
  @Override
  public String toString() {
    return "EditLogFileInputStream{" +
            "log=" + log.getName() +
            ", firstTxId=" + firstTxId +
            ", lastTxId=" + lastTxId +
            ", isInProgress=" + isInProgress +
            ", maxOpSize=" + maxOpSize +
            ", state=" + state +
            ", logVersion=" + logVersion +
            '}';
  }

  /**
   * 扫描并验证编辑日志文件，获取校验结果
   * @param file 要扫描验证的日志文件
   * @param maxTxIdToScan 最大扫描到的事务ID，读取到该ID或更高ID后停止扫描
   * @param verifyVersion 是否验证布局版本
   * @return 扫描验证结果
   * @throws IOException 如果发生IO错误
   */
  static FSEditLogLoader.EditLogValidation scanEditLog(File file,
      long maxTxIdToScan, boolean verifyVersion)
      throws IOException {
    EditLogFileInputStream in;
    try {
      in = new EditLogFileInputStream(file);
      // 读取头信息初始化流，不强制检查布局版本
      in.getVersion(verifyVersion);
    } catch (LogHeaderCorruptException e) {
      LOG.warn("Log file " + file + " has no valid header", e);
      return new FSEditLogLoader.EditLogValidation(0,
          HdfsServerConstants.INVALID_TXID, true);
    }

    try {
      // 调用通用扫描逻辑
      return FSEditLogLoader.scanEditLog(in, maxTxIdToScan);
    } finally {
      // 确保流被关闭
      IOUtils.closeStream(in);
    }
  }

  /**
   * 读取编辑日志头，获取日志版本号
   * @param in 编辑日志输入流
   * @param verifyLayoutVersion 是否验证布局版本合法性
   * @return 编辑日志版本号
   * @throws IOException 如果发生IO错误
   * @throws LogHeaderCorruptException 如果日志头损坏
   */
  @VisibleForTesting
  static int readLogVersion(DataInputStream in, boolean verifyLayoutVersion)
      throws IOException, LogHeaderCorruptException {
    int logVersion;
    try {
      // 读取版本号
      logVersion = in.readInt();
    } catch (EOFException eofe) {
      throw new LogHeaderCorruptException(
          "Reached EOF when reading log header");
    }
    if (verifyLayoutVersion &&
        (logVersion < HdfsServerConstants.NAMENODE_LAYOUT_VERSION || // 未来版本过低
         logVersion > Storage.LAST_UPGRADABLE_LAYOUT_VERSION)) { // 版本过高不支持
      throw new LogHeaderCorruptException(
          "Unexpected version of the file system log file: "
          + logVersion + ". Current version = "
          + HdfsServerConstants.NAMENODE_LAYOUT_VERSION + ".");
    }
    return logVersion;
  }
  
  /**
   * 编辑日志头损坏异常，指示日志头不存在或数据非法
   */
  static class LogHeaderCorruptException extends IOException {
    private static final long serialVersionUID = 1L;

    private LogHeaderCorruptException(String msg) {
      super(msg);
    }
  }
  
  /**
   * 日志源抽象接口，定义不同来源日志需要实现的能力
   */
  private interface LogSource {
    public InputStream getInputStream() throws IOException;
    public long length();
    public String getName();
  }

  /**
   * 内存ByteString日志源实现，从内存字节数据读取编辑日志
   */
  private static class ByteStringLog implements LogSource {
    private final ByteString bytes;
    private final String name;

    public ByteStringLog(ByteString bytes, String name) {
      this.bytes = bytes;
      this.name = name;
    }

    @Override
    public InputStream getInputStream() {
      return bytes.newInput();
    }

    @Override
    public long length() {
      return bytes.size();
    }

    @Override
    public String getName() {
      return name;
    }

  }
  
  /**
   * 本地文件日志源实现，从本地磁盘文件读取编辑日志
   */
  private static class FileLog implements LogSource {
    private final File file;
    
    public FileLog(File file) {
      this.file = file;
    }

    @Override
    public InputStream getInputStream() throws IOException {