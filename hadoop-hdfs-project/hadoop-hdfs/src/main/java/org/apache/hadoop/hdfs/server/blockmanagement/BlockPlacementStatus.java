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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 块放置状态接口，用于描述数据块副本放置是否满足HDFS块放置策略的要求
 * 是HDFS块放置策略校验结果的抽象，为块放置合规性检查提供统一查询接口
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface BlockPlacementStatus {

  /**
   * 检查当前块的所有副本放置是否满足块放置策略的要求
   * @return 如果满足放置策略要求返回true，否则返回false
   */
  public boolean isPlacementPolicySatisfied();
  
  /**
   * 获取块放置不满足策略要求时的错误描述信息，用于日志输出和用户展示
   * @return 块放置不符合要求的错误描述文本
   */
  public String getErrorDescription();

  /**
   * 获取满足块放置策略要求还需要额外添加的副本数量
   * @return 满足放置策略还需要新增的副本数量，不需要新增则返回0
   */
  int getAdditionalReplicasRequired();

}