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

import org.apache.hadoop.yarn.api.records.NodeAttribute;

/**
 * 节点属性更新调度事件，用于通知YARN资源调度器节点属性发生变更。
 * 封装了需要更新的所有节点及其对应的属性集合，由调度器处理完成属性更新。
 */
public class NodeAttributesUpdateSchedulerEvent extends SchedulerEvent {
  // 存储节点ID到对应属性集合的映射，表示需要更新的节点属性
  private Map<String, Set<NodeAttribute>> nodeToAttributes;

  /**
   * 构造节点属性更新调度事件
   * @param newNodeToAttributesMap 需要更新的节点与对应属性集合映射
   */
  public NodeAttributesUpdateSchedulerEvent(
      Map<String, Set<NodeAttribute>> newNodeToAttributesMap) {
    super(SchedulerEventType.NODE_ATTRIBUTES_UPDATE);
    this.nodeToAttributes = newNodeToAttributesMap;
  }

  /**
   * 获取需要更新的所有节点属性映射
   * @return 节点ID到属性集合的映射
   */
  public Map<String, Set<NodeAttribute>> getUpdatedNodeToAttributes() {
    return nodeToAttributes;
  }
}