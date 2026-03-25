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
package org.apache.hadoop.hdfs.server.namenode.sps;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 存储策略满足器(SPS)待处理文件信息类，用于记录需要满足存储策略要求的文件或目录信息
 * 保存待处理项的ID信息和重试次数，用于SPS的调度队列管理
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ItemInfo {
  private long startPathId;
  private long fileId;
  private int retryCount;

  /**
   * 构造待处理项信息，默认初始重试次数为0
   * @param startPathId 触发SPS处理的起始目录ID
   * @param fileId 当前待处理文件/目录ID
   */
  public ItemInfo(long startPathId, long fileId) {
    this.startPathId = startPathId;
    this.fileId = fileId;
    // set 0 when item is getting added first time in queue.
    this.retryCount = 0;
  }

  /**
   * 构造待处理项信息，指定初始重试次数
   * @param startPathId 触发SPS处理的起始目录ID
   * @param fileId 当前待处理文件/目录ID
   * @param retryCount 初始重试次数
   */
  public ItemInfo(final long startPathId, final long fileId,
      final int retryCount) {
    this.startPathId = startPathId;
    this.fileId = fileId;
    this.retryCount = retryCount;
  }

  /**
   * 获取触发当前SPS处理的起始路径ID，标识SPS是从该路径发起处理
   * @return 起始路径ID
   */
  public long getStartPath() {
    return startPathId;
  }

  /**
   * 获取当前需要满足存储策略的文件ID
   * @return 待处理文件ID
   */
  public long getFile() {
    return fileId;
  }

  /**
   * 判断当前跟踪处理的项是否是目录
   * @return true如果是目录，false如果是文件
   */
  public boolean isDir() {
    return !(startPathId == fileId);
  }

  /**
   * 获取当前项已经尝试处理的重试次数
   * @return 已重试次数
   */
  public int getRetryCount() {
    return retryCount;
  }

  /**
   * 增加重试次数计数，处理失败后调用
   */
  public void increRetryCount() {
    this.retryCount++;
  }
}