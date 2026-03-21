// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeFilter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import java.util.List;

/**
 * YARN容量调度器约束处理模块的节点候选选择器接口，基于ClusterNodeTracker的只读实现，
 * 核心职责是根据过滤条件返回符合要求的节点列表，用于调度时节点筛选。
 */
public interface NodeCandidateSelector {

  /**
   * 根据指定过滤条件筛选符合要求的候选节点列表
   * @param filter 节点过滤器，定义筛选规则
   * @return 筛选后符合条件的调度节点列表
   */
  List<SchedulerNode> selectNodes(NodeFilter filter);

}