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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

/**
 * 文件级说明：候选节点集合工具类，为YARN应用放置调度提供候选节点集合的通用处理能力
 * Utility methods for {@link CandidateNodeSet}.
 */
public final class CandidateNodeSetUtils {

  /**
   * 工具类不允许实例化
   */
  private CandidateNodeSetUtils() {
  }

  /**
   * 如果候选节点集合仅包含一个节点，则返回该节点；否则返回null
   * 常用于单节点候选场景的快速获取逻辑
   * @param candidates 候选节点集合
   * @param <N> 继承自SchedulerNode的节点类型
   * @return 单个候选节点或null
   */
  public static <N extends SchedulerNode> N getSingleNode(
      CandidateNodeSet<N> candidates) {
    N node = null;
    if (1 == candidates.getAllNodes().size()) {
      node = candidates.getAllNodes().values().iterator().next();
    }

    return node;
  }
}