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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import java.util.Map;

/**
 * YARN资源调度应用放置候选节点集合接口
 * 
 * 表示一组可被调度器分配给应用的候选节点，核心包含三部分：
 * 1) 所有可调度节点的映射表
 * 2) 节点集合版本号，当节点增减时版本更新，用于{@link AppPlacementAllocator}判断是否需要失效本地缓存
 * 3) 候选节点集合所属的节点分区
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface CandidateNodeSet<N extends SchedulerNode> {
  /**
   * 获取该候选集合中的所有可调度节点
   * @return 所有可调度节点映射（节点ID -> 调度节点对象）
   */
  Map<NodeId, N> getAllNodes();

  /**
   * 获取候选节点集合的版本号，用于帮助{@link AppPlacementAllocator}判断是否需要更新缓存
   * @return 当前版本号
   */
  long getVersion();

  /**
   * 获取该节点集合所属的节点分区
   * @return 节点分区名称
   */
  String getPartition();
}