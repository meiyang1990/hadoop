// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.diskbalancer.planner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel
    .DiskBalancerDataNode;

/**
 * 磁盘均衡计划工厂，根据用户指定的规划器类型创建对应的规划器实例
 * 用于为数据节点生成磁盘数据均衡的移动计划
 */
public final class PlannerFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(PlannerFactory.class);

  /** 贪心规划器标识 */
  public static final String GREEDY_PLANNER = "greedyPlanner";

  /**
   * 根据规划器名称创建对应规划器实例，用于为指定数据节点生成磁盘均衡计划
   * @param plannerName - 规划器名称，指定要创建的规划器类型
   * @param node - 需要进行磁盘均衡的数据节点
   * @param threshold - 磁盘使用率不均衡阈值，超过该阈值触发均衡
   * @return 规划器实例，用于生成具体的均衡移动计划
   */
  public static Planner getPlanner(String plannerName,
      DiskBalancerDataNode node, double threshold) {
    if (plannerName.equals(GREEDY_PLANNER)) {
      if (LOG.isDebugEnabled()) {
        String message = String
            .format("Creating a %s for Node : %s IP : %s ID : %s",
                GREEDY_PLANNER, node.getDataNodeName(), node.getDataNodeIP(),
                node.getDataNodeUUID());
        LOG.debug(message);
      }
      return new GreedyPlanner(threshold, node);
    }

    throw new IllegalArgumentException("Unrecognized planner name : " +
        plannerName);
  }

  private PlannerFactory() {
    // 工具类不允许实例化
  }
}