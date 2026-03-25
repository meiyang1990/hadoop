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

package org.apache.hadoop.yarn.server.resourcemanager.nodelabels;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.nodelabels.AttributeValue;
import org.apache.hadoop.yarn.server.api.protocolrecords.AttributeMappingOperationType;

/**
 * 节点属性持久化存储事件，封装了需要写入后端存储的节点属性变更信息
 */
public class NodeAttributesStoreEvent
    extends AbstractEvent<NodeAttributesStoreEventType> {
  // 节点属性映射表：key为节点ID，value为该节点关联的所有属性与对应值
  private Map<String, Map<NodeAttribute, AttributeValue>> nodeAttributeMapping;
  // 当前操作类型（添加/删除/更新等）
  private AttributeMappingOperationType operation;

  /**
   * 构造节点属性存储事件
   * @param nodeAttributeMappingList 需要持久化的节点属性映射集合
   * @param operation 属性映射操作类型
   */
  public NodeAttributesStoreEvent(
      Map<String, Map<NodeAttribute, AttributeValue>> nodeAttributeMappingList,
      AttributeMappingOperationType operation) {
    super(NodeAttributesStoreEventType.STORE_ATTRIBUTES);
    this.nodeAttributeMapping = nodeAttributeMappingList;
    this.operation = operation;
  }

  /**
   * 获取需要持久化的节点属性映射集合
   * @return 按节点分组的节点属性映射表
   */
  public Map<String,
      Map<NodeAttribute, AttributeValue>> getNodeAttributeMappingList() {
    return nodeAttributeMapping;
  }

  /**
   * 获取当前操作类型
   * @return 属性映射操作类型
   */
  public AttributeMappingOperationType getOperation() {
    return operation;
  }
}