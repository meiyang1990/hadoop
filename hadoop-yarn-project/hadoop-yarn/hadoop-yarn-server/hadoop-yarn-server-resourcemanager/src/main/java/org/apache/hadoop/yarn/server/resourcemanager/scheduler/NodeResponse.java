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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;


/**
 * YARN资源调度器节点响应封装类，封装了节点管理器上报更新后集群信息返回的结果。
 * 包含已完成容器、待清理容器和已结束应用三类结果数据。
 */
public class NodeResponse {
  private final List<Container> completed;
  private final List<Container> toCleanUp;
  private final List<ApplicationId> finishedApplications;
  
  /**
   * 构造节点响应对象，封装三类处理结果。
   * @param finishedApplications 已结束的应用ID列表
   * @param completed 已完成的容器列表
   * @param toKill 需要清理的容器列表
   */
  public NodeResponse(List<ApplicationId> finishedApplications,
      List<Container> completed, List<Container> toKill) {
    this.finishedApplications = finishedApplications;
    this.completed = completed;
    this.toCleanUp = toKill;
  }
  
  /**
   * 获取已结束的应用ID列表。
   * @return 已结束应用ID列表
   */
  public List<ApplicationId> getFinishedApplications() {
    return this.finishedApplications;
  }
  
  /**
   * 获取已完成的容器列表。
   * @return 已完成容器列表
   */
  public List<Container> getCompletedContainers() {
    return this.completed;
  }
  
  /**
   * 获取需要清理的容器列表。
   * @return 待清理容器列表
   */
  public List<Container> getContainersToCleanUp() {
    return this.toCleanUp;
  }
}