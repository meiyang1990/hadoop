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

import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 基于节点组的块放置策略的放置状态实现类，为支持节点组感知的块放置策略提供状态检查能力
 * 用于配合 {@link BlockPlacementPolicyWithNodeGroup} 实现节点组层面的块放置校验
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockPlacementStatusWithNodeGroup implements BlockPlacementStatus {

  private final BlockPlacementStatus parentBlockPlacementStatus;
  private final Set<String> currentNodeGroups;
  private final int requiredNodeGroups;

  /**
   * 构造基于节点组的块放置状态对象
   * @param parentBlockPlacementStatus 父级放置策略的状态对象，用于基础放置策略校验
   * @param currentNodeGroups 当前块副本分布所在的节点组集合
   * @param requiredNodeGroups 满足放置策略需要的最少节点组数量
   */
  public BlockPlacementStatusWithNodeGroup(
      BlockPlacementStatus parentBlockPlacementStatus,
      Set<String> currentNodeGroups, int requiredNodeGroups) {
    this.parentBlockPlacementStatus = parentBlockPlacementStatus;
    this.currentNodeGroups = currentNodeGroups;
    this.requiredNodeGroups = requiredNodeGroups;
  }

  /**
   * 检查块放置是否满足整体放置策略要求
   * @return 基础策略和节点组策略都满足时返回true，否则返回false
   */
  @Override
  public boolean isPlacementPolicySatisfied() {
    return parentBlockPlacementStatus.isPlacementPolicySatisfied()
        && isNodeGroupPolicySatisfied();
  }

  /**
   * 检查是否满足节点组层面的放置策略要求
   * @return 当前节点组数量满足要求返回true，否则返回false
   */
  private boolean isNodeGroupPolicySatisfied() {
    return requiredNodeGroups <= currentNodeGroups.size();
  }

  /**
   * 获取放置不满足要求的错误描述信息
   * @return 错误描述字符串，满足要求时返回null
   */
  @Override
  public String getErrorDescription() {
    if (isPlacementPolicySatisfied()) {
      return null;
    }

    StringBuilder errorDescription = new StringBuilder();
    // 拼接父级基础策略的错误信息
    if (!parentBlockPlacementStatus.isPlacementPolicySatisfied()) {
      errorDescription.append(parentBlockPlacementStatus.getErrorDescription());
    }

    // 拼接节点组策略的错误信息
    if (!isNodeGroupPolicySatisfied()) {
      // 如果已有错误信息，添加空格分隔
      if (errorDescription.length() != 0) {
        errorDescription.append(" ");
      }
      errorDescription.append("The block has " + requiredNodeGroups
          + " replicas. But it only has " + currentNodeGroups.size()
          + " node groups " + currentNodeGroups + ".");
    }
    return errorDescription.toString();
  }

  /**
   * 计算满足放置策略还需要额外添加的副本数量
   * @return 需要新增的副本数量
   */
  @Override
  public int getAdditionalReplicasRequired() {
    if (isPlacementPolicySatisfied()) {
      return 0;
    } else {
      // 获取父级策略要求的新增副本数
      int parent = parentBlockPlacementStatus.getAdditionalReplicasRequired();
      // 计算节点组策略要求的新增副本数
      int child = requiredNodeGroups - currentNodeGroups.size();
      // 返回两者中较大值，保证同时满足两种策略要求
      return Math.max(parent, child);
    }
  }
}