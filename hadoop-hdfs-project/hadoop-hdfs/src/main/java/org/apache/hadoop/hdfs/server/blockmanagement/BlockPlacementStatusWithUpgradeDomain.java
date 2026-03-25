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

import java.util.Set;

/**
 * 基于升级域的块放置状态实现类，为{@link BlockPlacementPolicyWithUpgradeDomain}提供块放置合规性检查能力
 * 该类扩展基础块放置检查，增加了升级域分布约束的检查逻辑，用于支持滚动升级场景下的数据高可用
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockPlacementStatusWithUpgradeDomain implements
    BlockPlacementStatus {

  private final BlockPlacementStatus parentBlockPlacementStatus;
  private final Set<String> upgradeDomains;
  private final int numberOfReplicas;
  private final int upgradeDomainFactor;

  /**
   * 构造基于升级域的块放置状态对象
   * @param parentBlockPlacementStatus 基础块放置策略的状态对象
   * @param upgradeDomains 当前块所有副本所在的升级域集合
   * @param numberOfReplicas 当前块的副本总数
   * @param upgradeDomainFactor 配置要求的最小升级域数量
   */
  public BlockPlacementStatusWithUpgradeDomain(
      BlockPlacementStatus parentBlockPlacementStatus,
      Set<String> upgradeDomains, int numberOfReplicas,
      int upgradeDomainFactor){
    this.parentBlockPlacementStatus = parentBlockPlacementStatus;
    this.upgradeDomains = upgradeDomains;
    this.numberOfReplicas = numberOfReplicas;
    this.upgradeDomainFactor = upgradeDomainFactor;
  }

  @Override
  public boolean isPlacementPolicySatisfied() {
    // 需要同时满足基础放置策略和升级域放置策略的要求
    return parentBlockPlacementStatus.isPlacementPolicySatisfied() &&
        isUpgradeDomainPolicySatisfied();
  }

  /**
   * 检查当前块副本是否满足升级域分布约束
   * @return true表示满足约束，false表示不满足
   */
  private boolean isUpgradeDomainPolicySatisfied() {
    if (numberOfReplicas <= upgradeDomainFactor) {
      // 副本数小于要求的升级域数量时，每个副本应分布在不同升级域
      return (numberOfReplicas <= upgradeDomains.size());
    } else {
      // 副本数大于要求的升级域数量时，至少需要满足配置的最小升级域数量
      return upgradeDomains.size() >= upgradeDomainFactor;
    }
  }

  @Override
  public String getErrorDescription() {
    if (isPlacementPolicySatisfied()) {
      return null;
    }
    StringBuilder errorDescription = new StringBuilder();
    // 拼接基础放置策略不满足的错误信息
    if (!parentBlockPlacementStatus.isPlacementPolicySatisfied()) {
      errorDescription.append(parentBlockPlacementStatus.getErrorDescription());
    }
    // 拼接升级域策略不满足的错误信息
    if (!isUpgradeDomainPolicySatisfied()) {
      if (errorDescription.length() != 0) {
        errorDescription.append(" ");
      }
      errorDescription.append("The block has " + numberOfReplicas +
          " replicas. But it only has " + upgradeDomains.size() +
              " upgrade domains " + upgradeDomains +".");
    }
    return errorDescription.toString();
  }

  @Override
  public int getAdditionalReplicasRequired() {
    if (isPlacementPolicySatisfied()) {
      return 0;
    } else {
      // 块可能同时在基础放置（机架分布）和升级域分布其中一个或多个不满足要求，需要取最大值
      int parent = parentBlockPlacementStatus.getAdditionalReplicasRequired();
      int child;

      // 计算满足升级域约束还需要增加的副本数
      if (numberOfReplicas <= upgradeDomainFactor) {
        child = numberOfReplicas - upgradeDomains.size();
      } else {
        child = upgradeDomainFactor - upgradeDomains.size();
      }
      // 返回基础策略和升级域策略中需要的最大额外副本数
      return Math.max(parent, child);
    }
  }
}