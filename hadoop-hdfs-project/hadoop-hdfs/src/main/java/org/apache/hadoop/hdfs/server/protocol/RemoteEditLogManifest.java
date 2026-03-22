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

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

/**
 * 文件级注释：HDFS远程编辑日志清单，用于在NameNode之间同步数据时，
 * 描述远程NameNode上可用的编辑日志列表，是元数据同步协议中的数据传输载体。
 *
 * An enumeration of logs available on a remote NameNode.
 */
public class RemoteEditLogManifest {

  private List<RemoteEditLog> logs;

  private long committedTxnId = HdfsServerConstants.INVALID_TXID;

  /**
   * 无参构造函数，用于反序列化场景创建空清单。
   */
  public RemoteEditLogManifest() {
  }

  /**
   * 构造函数，使用编辑日志列表创建清单，不设置已提交事务ID。
   * @param logs 可用的远程编辑日志列表
   */
  public RemoteEditLogManifest(List<RemoteEditLog> logs) {
    this(logs, HdfsServerConstants.INVALID_TXID);
  }

  /**
   * 完整构造函数，使用编辑日志列表和已提交事务ID创建清单，并校验清单数据合法性。
   * @param logs 可用的远程编辑日志列表
   * @param committedTxnId 远程NameNode已提交的最大事务ID
   */
  public RemoteEditLogManifest(List<RemoteEditLog> logs, long committedTxnId) {
    this.logs = logs;
    this.committedTxnId = committedTxnId;
    checkState();
  }
  
  
  /**
   * 校验编辑日志清单的合法性：确保日志按事务ID升序排列，且不存在重叠。
   * 日志不需要连续，但必须保证顺序和不重叠。
   * @throws IllegalStateException 如果清单数据不符合要求则抛出异常
   */
  private void checkState()  {
    Preconditions.checkNotNull(logs);

    RemoteEditLog prev = null;
    // 遍历所有日志，校验顺序和重叠性
    for (RemoteEditLog log : logs) {
      if (prev != null) {
        // 当前日志起始事务ID必须大于前一个日志的结束事务ID，保证不重叠
        if (log.getStartTxId() <= prev.getEndTxId()) {
          throw new IllegalStateException(
              "Invalid log manifest (log " + log + " overlaps " + prev + ")\n"
              + this);
        }
      }
      prev = log;
    }
  }
  
  /**
   * 获取远程可用的编辑日志列表，返回不可修改视图保证数据安全。
   * @return 不可修改的远程编辑日志列表
   */
  public List<RemoteEditLog> getLogs() {
    return Collections.unmodifiableList(logs);
  }

  /**
   * 获取远程NameNode已提交的最大事务ID，标识该事务之前的所有编辑日志都已持久化。
   * @return 已提交的最大事务ID
   */
  public long getCommittedTxnId() {
    return committedTxnId;
  }

  @Override
  public String toString() {
    return "[" + Joiner.on(", ").join(logs) + "]" + " CommittedTxId: "
        + committedTxnId;
  }
}