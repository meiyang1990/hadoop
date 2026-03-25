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
import java.util.Set;

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;

/**
 * YARN ResourceManager 节点标签映射提供者抽象基类，负责提供节点到对应标签的映射关系
 */
public abstract class RMNodeLabelsMappingProvider extends AbstractService {

  public RMNodeLabelsMappingProvider(String name) {
    super(name);
  }

  /**
   * 获取指定节点集合对应的标签映射
   * 在标签发生变更前，方法会持续返回相同的标签结果
   *
   * @param nodes 需要获取标签的节点ID集合
   * @return 节点ID到对应节点标签集合的映射
   */
  public abstract Map<NodeId, Set<NodeLabel>> getNodeLabels(Set<NodeId> nodes);
}