// 这个文件已经全部加上中文注释
/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this use this file except in compliance
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

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeAttributesProvider;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeLabelsProvider;

/**
 * NodeManager节点状态更新器接口，定义了NodeManager向ResourceManager汇报节点状态
 * 和处理本地容器状态的核心方法，是节点状态同步流程的核心抽象接口。
 */
public interface NodeStatusUpdater extends Service {

  /**
   * 发起一次带外心跳，在常规周期性心跳之外向ResourceManager发送状态更新。
   * 通常在节点上容器状态发生变化时调用，用于更快通知RM状态变更。
   */
  void sendOutofBandHeartBeat();

  /**
   * 获取节点注册时从ResourceManager得到的RM标识符。
   * @return ResourceManager ID
   */
  long getRMIdentifier();
  
  /**
   * 查询指定容器是否最近刚停止。
   * @param containerId 容器ID
   * @return true 如果容器最近已完成停止
   */
  public boolean isContainerRecentlyStopped(ContainerId containerId);
  
  /**
   * 将已完成的容器添加到最近完成容器列表中。
   * @param containerId 已完成容器的ID
   */
  public void addCompletedContainer(ContainerId containerId);

  /**
   * 清空缓存中最近完成容器的列表。
   */
  public void clearFinishedContainersFromCache();

  /**
   * 上报不可恢复的异常，标记节点为不健康状态。
   * @param ex 导致节点不健康的异常
   */
  void reportException(Exception ex);

  /**
   * 设置节点属性提供者，用于汇报节点属性给ResourceManager。
   * @param provider 节点属性提供者实例
   */
  void setNodeAttributesProvider(NodeAttributesProvider provider);

  /**
   * 设置节点标签提供者，用于汇报节点标签给ResourceManager。
   * @param provider 节点标签提供者实例
   */
  void setNodeLabelsProvider(NodeLabelsProvider provider);
}