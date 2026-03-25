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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.NodeId;

/**
 * 节点标签更新调度事件，用于通知调度器节点标签发生了变更。
 * 当集群节点标签配置修改后，触发该事件通知调度器更新节点标签映射。
 */
public class NodeLabelsUpdateSchedulerEvent extends SchedulerEvent {
  private Map<NodeId, Set<String>> nodeToLabels;

  /**
   * 构造节点标签更新调度事件。
   * @param nodeToLabels 更新后的节点到标签集合的映射
   */
  public NodeLabelsUpdateSchedulerEvent(Map<NodeId, Set<String>> nodeToLabels) {
    super(SchedulerEventType.NODE_LABELS_UPDATE);
    this.nodeToLabels = nodeToLabels;
  }
  
  /**
   * 获取更新后的节点标签映射关系。
   * @return 节点ID到对应标签集合的映射表
   */
  public Map<NodeId, Set<String>> getUpdatedNodeToLabels() {
    return nodeToLabels;
  }
}