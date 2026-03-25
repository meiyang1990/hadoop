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

/**
 * HDFS数据块块放置策略状态默认实现类，用于封装数据块副本跨机架放置的合规性状态信息
 * 用于检查数据块副本是否满足块放置策略的机架分布要求，并提供错误信息和所需额外副本数量
 */
public class BlockPlacementStatusDefault implements BlockPlacementStatus {

  private int requiredRacks = 0;
  private int currentRacks = 0;
  private final int totalRacks;
  
  /**
   * 构造块放置状态对象，保存当前机架分布信息与策略要求
   * @param currentRacks 当前数据块副本已分布到的机架数量
   * @param requiredRacks 块放置策略要求的最小机架数量
   * @param totalRacks 集群当前总机架数量
   */
  public BlockPlacementStatusDefault(int currentRacks, int requiredRacks,
      int totalRacks){
    this.requiredRacks = requiredRacks;
    this.currentRacks = currentRacks;
    this.totalRacks = totalRacks;
  }
  
  @Override
  /**
   * 检查当前数据块的机架分布是否满足块放置策略要求
   * @return true表示满足要求，false表示不满足
   */
  public boolean isPlacementPolicySatisfied() {
    // 两种满足情况：1. 当前机架数已达到要求 2. 当前机架数已经等于集群总机架数（无法再扩展）
    return requiredRacks <= currentRacks || currentRacks >= totalRacks;
  }

  @Override
  /**
   * 获取不满足放置策略时的错误描述信息
   * @return 错误描述字符串，满足策略时返回null
   */
  public String getErrorDescription() {
    if (isPlacementPolicySatisfied()) {
      return null;
    }
    return "Block should be additionally replicated on " + 
        (requiredRacks - currentRacks) +
        " more rack(s). Total number of racks in the cluster: " + totalRacks;
  }

  @Override
  /**
   * 获取满足放置策略还需要额外添加的副本数量
   * @return 需要新增的副本数量，满足策略时返回0
   */
  public int getAdditionalReplicasRequired() {
    if (isPlacementPolicySatisfied()) {
      return 0;
    } else {
      return requiredRacks - currentRacks;
    }
  }
}