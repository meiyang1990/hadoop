// 这个文件已经全部加上中文注释
/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * 网络标签映射管理器基础接口，负责管理容器到网络标签的映射管理，
 * 用于YARN节点管理器实现容器网络流量控制与隔离。
 */
public interface NetworkTagMappingManager {

  /**
   * 初始化网络标签映射管理器，加载配置参数。
   * @param conf 配置对象
   */
  void initialize(Configuration conf);

  /**
   * 根据容器获取对应的十六进制格式网络标签ID，用于网络规则匹配。
   * @param container 目标容器
   * @return 十六进制格式的网络标签ID
   */
  String getNetworkTagHexID(Container container);
}