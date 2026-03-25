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

import org.apache.hadoop.yarn.api.records.NodeAttribute;

/**
 * 文件: NodeAttributesProvider.java
 * 所属模块: YARN NodeManager 节点标签模块
 * 核心职责: 定义节点属性提供者的抽象基类，负责获取节点属性，为不同来源的节点属性提供统一扩展接口
 * 用于YARN节点标签功能，支持从不同数据源获取节点属性并提供给调度器使用
 */
public abstract class NodeAttributesProvider
    extends AbstractNodeDescriptorsProvider<NodeAttribute> {

  /**
   * 构造函数，初始化节点属性提供者
   * @param name 提供者名称标识
   */
  public NodeAttributesProvider(String name) {
    super(name);
  }
}