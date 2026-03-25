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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

/**
 * 封装NodeManager上报给ResourceManager的容器更新信息，
 * 包含新启动容器、已完成容器和状态更新容器三类更新。
 */
public class UpdatedContainerInfo {
  private List<ContainerStatus> newlyLaunchedContainers;
  private List<ContainerStatus> completedContainers;
  private List<Map.Entry<ApplicationId, ContainerStatus>> updateContainers;
  
  /**
   * 构造空的容器更新信息对象。
   */
  public UpdatedContainerInfo() {
  }

  /**
   * 构造完整的容器更新信息对象，包含三类更新。
   * @param newlyLaunchedContainers 新启动完成的容器状态列表
   * @param completedContainers 已完成退出的容器状态列表
   * @param updateContainers 需要更新状态的容器列表，每项包含应用ID和容器状态
   */
  public UpdatedContainerInfo(List<ContainerStatus> newlyLaunchedContainers,
                              List<ContainerStatus> completedContainers,
                              List<Map.Entry<ApplicationId, ContainerStatus>>
                                  updateContainers) {
    this.newlyLaunchedContainers = newlyLaunchedContainers;
    this.completedContainers = completedContainers;
    this.updateContainers = updateContainers;
  } 

  /**
   * 获取新启动容器的状态列表。
   * @return 新启动容器状态列表
   */
  public List<ContainerStatus> getNewlyLaunchedContainers() {
    return this.newlyLaunchedContainers;
  }

  /**
   * 获取已完成容器的状态列表。
   * @return 已完成容器状态列表
   */
  public List<ContainerStatus> getCompletedContainers() {
    return this.completedContainers;
  }

  /**
   * 获取需要更新状态的容器列表。
   * @return 状态更新容器列表，每项为应用ID和对应容器状态
   */
  public List<Map.Entry<ApplicationId, ContainerStatus>> getUpdateContainers() {
    return this.updateContainers;
  }
}