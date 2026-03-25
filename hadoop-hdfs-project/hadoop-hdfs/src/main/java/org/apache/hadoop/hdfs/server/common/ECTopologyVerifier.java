// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.common;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ECTopologyVerifierResult;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 纠删码集群拓扑校验工具，验证当前集群拓扑是否满足所有已启用纠删码策略的部署要求
 * 
 * 校验不通过的两种场景：
 * 1. 集群数据节点总数少于纠删码策略要求的最大数据块+校验块总数
 * 2. 集群机架总数不足，无法满足机架容错块放置策略的要求
 */
@InterfaceAudience.Private
public final class ECTopologyVerifier {

  public static final Logger LOG =
      LoggerFactory.getLogger(ECTopologyVerifier.class);

  private ECTopologyVerifier() {}

  /**
   * 根据集群现有数据节点信息，验证是否满足指定纠删码策略的拓扑要求
   * 
   * @param report 所有数据节点的描述信息数组
   * @param policies 需要校验的纠删码策略集合
   * @return 校验结果，包含是否通过和对应说明信息
   */
  public static ECTopologyVerifierResult getECTopologyVerifierResult(
      final DatanodeInfo[] report,
      final Collection<ErasureCodingPolicy> policies) {
    // 统计集群现有机架数量
    final int numOfRacks = getNumberOfRacks(report);
    return getECTopologyVerifierResult(numOfRacks, report.length, policies);
  }

  /**
   * 根据给定的机架和数据节点数量，验证是否满足指定纠删码策略的拓扑要求
   * 
   * @param numOfRacks 集群现有机架数量
   * @param numOfDataNodes 集群现有数据节点数量
   * @param policies 需要校验的纠删码策略集合
   * @return 校验结果，包含是否通过和对应说明信息
   */
  public static ECTopologyVerifierResult getECTopologyVerifierResult(
      final int numOfRacks, final int numOfDataNodes,
      final Collection<ErasureCodingPolicy> policies) {
    int minDN = 0;
    int minRack = 0;
    // 遍历所有策略，计算所需的最小数据节点数和最小机架数
    for (ErasureCodingPolicy policy: policies) {
      final int policyDN =
          policy.getNumDataUnits() + policy
              .getNumParityUnits();
      // 更新所有策略中要求的最大数据节点数，即满足所有策略所需的最小节点数
      minDN = Math.max(minDN, policyDN);
      // 根据纠删码放置规则，计算该策略所需最少机架数：向上取整(总块数/校验块数)
      final int policyRack = (int) Math.ceil(
          policyDN / (double) policy.getNumParityUnits());
      // 更新所有策略中要求的最大机架数，即满足所有策略所需的最小机架数
      minRack = Math.max(minRack, policyRack);
    }
    // 没有传入任何策略的情况，判定为通过
    if (minDN == 0 || minRack == 0) {
      String resultMessage = "No erasure coding policy is given.";
      LOG.trace(resultMessage);
      return new ECTopologyVerifierResult(true, resultMessage);
    }
    // 执行拓扑校验并返回结果
    return verifyECWithTopology(minDN, minRack, numOfRacks, numOfDataNodes,
        getReadablePolicies(policies));
  }

  /**
   * 对比要求和实际拓扑，执行校验并生成结果
   */
  private static ECTopologyVerifierResult verifyECWithTopology(
      final int minDN, final int minRack,
      final int numOfRacks, final int numOfDataNodes, String readablePolicies) {
    String resultMessage;
    // 数据节点数量不足，校验不通过
    if (numOfDataNodes < minDN) {
      resultMessage = String.format("%d DataNodes are required for " +
              "the erasure coding policies: %s. " +
              "The number of DataNodes is only %d.",
          minDN, readablePolicies, numOfDataNodes);
      LOG.debug(resultMessage);
      return new ECTopologyVerifierResult(false, resultMessage);
    }

    // 机架数量不足，校验不通过
    if (numOfRacks < minRack) {
      resultMessage = String.format("%d racks are required for " +
          "the erasure coding policies: %s. " +
              "The number of racks is only %d.",
          minRack, readablePolicies, numOfRacks);
      LOG.debug(resultMessage);
      return new ECTopologyVerifierResult(false, resultMessage);
    }
    // 满足所有要求，校验通过
    return new ECTopologyVerifierResult(true,
        String.format("The cluster setup can support EC policies: %s",
            readablePolicies));
  }

  /**
   * 统计当前集群中不同机架的数量
   */
  private static int getNumberOfRacks(DatanodeInfo[] report) {
    final Map<String, Integer> racks = new HashMap<>();
    for (DatanodeInfo dni : report) {
      // 按网络位置统计机架
      Integer count = racks.get(dni.getNetworkLocation());
      if (count == null) {
        count = 0;
      }
      racks.put(dni.getNetworkLocation(), count + 1);
    }
    return racks.size();
  }

  /**
   * 将策略集合转换为用逗号分隔的策略名称字符串，用于日志和结果输出
   */
  private static String getReadablePolicies(
      final Collection<ErasureCodingPolicy> policies) {
    return policies.stream().map(policyInfo -> policyInfo.getName())
        .collect(Collectors.joining(", "));
  }
}