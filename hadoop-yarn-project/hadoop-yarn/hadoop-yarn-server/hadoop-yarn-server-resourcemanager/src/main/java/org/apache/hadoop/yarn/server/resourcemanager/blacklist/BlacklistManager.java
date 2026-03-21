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

package org.apache.hadoop.yarn.server.resourcemanager.blacklist;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;

/**
 * 文件说明：YARN ResourceManager 节点黑名单管理接口，负责根据节点上容器失败情况维护黑名单
 * 核心职责：跟踪失败节点，为调度器提供黑名单更新信息，实现故障节点的自动规避
 */
@Private
public interface BlacklistManager {

  /**
   * 上报指定节点上的容器失败事件，将节点加入黑名单跟踪
   * @param node 发生容器失败的节点主机名
   */
  void addNode(String node);

  /**
   * 获取当前周期的黑名单更新请求，包含需要新增和移除出黑名单的节点
   * @return 包含增减节点信息的黑名单更新请求对象
   */
  ResourceBlacklistRequest getBlacklistUpdates();

  /**
   * 刷新集群中可用NodeManager的总数量，用于动态调整黑名单大小阈值
   * @param nodeHostCount 当前集群存活节点主机总数
   */
  void refreshNodeHostCount(int nodeHostCount);
}