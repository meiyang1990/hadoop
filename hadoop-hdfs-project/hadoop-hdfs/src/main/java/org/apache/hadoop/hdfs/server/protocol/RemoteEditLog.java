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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;
import java.util.function.Function;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

/**
 * 远程编辑日志条目，用于HDFS高可用场景中表示存储在远程共享存储上的一段编辑日志
 * 记录了一段编辑日志的起始事务ID、结束事务ID以及是否正在写入的状态
 */
public class RemoteEditLog implements Comparable<RemoteEditLog> {
  private long startTxId = HdfsServerConstants.INVALID_TXID;
  private long endTxId = HdfsServerConstants.INVALID_TXID;
  private boolean isInProgress = false;
  
  /**
   * 构造空的远程编辑日志对象
   */
  public RemoteEditLog() {
  }

  /**
   * 构造远程编辑日志对象，根据结束事务ID自动判断是否正在写入
   * @param startTxId 起始事务ID
   * @param endTxId 结束事务ID
   */
  public RemoteEditLog(long startTxId, long endTxId) {
    this.startTxId = startTxId;
    this.endTxId = endTxId;
    this.isInProgress = (endTxId == HdfsServerConstants.INVALID_TXID);
  }
  
  /**
   * 构造远程编辑日志对象，手动指定是否正在写入
   * @param startTxId 起始事务ID
   * @param endTxId 结束事务ID
   * @param inProgress 是否正在写入
   */
  public RemoteEditLog(long startTxId, long endTxId, boolean inProgress) {
    this.startTxId = startTxId;
    this.endTxId = endTxId;
    this.isInProgress = inProgress;
  }

  /**
   * 获取这段编辑日志的起始事务ID
   * @return 起始事务ID
   */
  public long getStartTxId() {
    return startTxId;
  }

  /**
   * 获取这段编辑日志的结束事务ID
   * @return 结束事务ID
   */
  public long getEndTxId() {
    return endTxId;
  }

  /**
   * 判断这段编辑日志是否正在被写入（未完成）
   * @return true表示正在写入，false表示已完成
   */
  public boolean isInProgress() {
    return isInProgress;
  }

  @Override
  public String toString() {
    if (!isInProgress) {
      return "[" + startTxId + "," + endTxId + "]";
    } else {
      return "[" + startTxId + "-? (in-progress)]";
    }
  }
  
  @Override
  public int compareTo(RemoteEditLog log) {
    return ComparisonChain.start()
      .compare(startTxId, log.startTxId)
      .compare(endTxId, log.endTxId)
      .result();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RemoteEditLog)) return false;
    return this.compareTo((RemoteEditLog)o) == 0;
  }
  
  @Override
  public int hashCode() {
    return (int) (startTxId * endTxId);
  }
  
  /**
   * Function函数实例，用于从RemoteEditLog对象中提取起始事务ID
   * 输入为null时返回无效事务ID
   */
  public static final Function<RemoteEditLog, Long> GET_START_TXID =
      log -> {
        if (null == log) {
          return HdfsServerConstants.INVALID_TXID;
        }
        return log.getStartTxId();
      };
}