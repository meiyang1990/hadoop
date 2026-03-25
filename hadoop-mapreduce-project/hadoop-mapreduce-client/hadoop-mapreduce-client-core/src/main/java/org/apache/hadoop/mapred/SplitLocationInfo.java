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

package org.apache.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * 输入分片位置信息容器，存储分片所在节点位置以及存储介质类型信息
 * 用于MapReduce任务调度时的数据本地性判断，优先分配任务到数据所在节点
 */
@Public
@Evolving
public class SplitLocationInfo {
  private boolean inMemory;
  private String location;
  
  /**
   * 构造分片位置信息对象
   * @param location 数据所在节点主机名
   * @param inMemory 数据是否存储在节点内存中
   */
  public SplitLocationInfo(String location, boolean inMemory) {
    this.location = location;
    this.inMemory = inMemory;
  }
  
  /**
   * 判断数据是否存储在磁盘上
   * @return 固定返回true，表示该分片一定有磁盘存储副本
   */
  public boolean isOnDisk() {
    return true;
  }
  
  /**
   * 判断数据是否存储在节点内存中
   * @return true表示数据在内存中，false表示仅在磁盘
   */
  public boolean isInMemory() {
    return inMemory;
  }

  /**
   * 获取分片所在节点的主机名
   * @return 节点主机名
   */
  public String getLocation() {
    return location;
  }
}