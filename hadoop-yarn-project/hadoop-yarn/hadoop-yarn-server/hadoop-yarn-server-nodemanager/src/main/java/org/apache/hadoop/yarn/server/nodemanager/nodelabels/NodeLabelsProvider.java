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

package org.apache.hadoop.yarn.server.nodemanager.nodelabels;

import org.apache.hadoop.yarn.api.records.NodeLabel;

/**
 * 节点标签提供者抽象基类，负责获取当前节点的标签信息，为YARN节点标签调度提供标签数据。
 *
 */
public abstract class NodeLabelsProvider
    extends AbstractNodeDescriptorsProvider<NodeLabel>{

  /**
   * 构造节点标签提供者实例
   * @param name 提供者名称
   */
  public NodeLabelsProvider(String name) {
    super(name);
  }
}