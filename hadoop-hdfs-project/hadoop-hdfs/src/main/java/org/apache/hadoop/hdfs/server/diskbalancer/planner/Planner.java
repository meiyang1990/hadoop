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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.diskbalancer.planner;

import org.apache.hadoop.hdfs.server.diskbalancer.datamodel
    .DiskBalancerDataNode;

/**
 * HDFS磁盘均衡器的规划器接口，定义规划磁盘均衡方案的统一契约
 * 允许接入不同的均衡规划算法实现，遵循开闭原则扩展均衡策略
 */
public interface Planner {
  /**
   * 为指定数据节点生成磁盘数据均衡规划方案
   * @param node 待均衡的数据节点，包含节点上所有磁盘的使用信息
   * @return 包含所有移动块任务计划的节点均衡方案
   * @throws Exception 规划过程中发生的任何异常
   */
  NodePlan plan(DiskBalancerDataNode node) throws Exception;
}