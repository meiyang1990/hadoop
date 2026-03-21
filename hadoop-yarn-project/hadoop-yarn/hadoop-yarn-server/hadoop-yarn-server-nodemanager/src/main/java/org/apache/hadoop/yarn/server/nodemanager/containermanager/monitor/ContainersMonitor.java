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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;

/**
 * 容器监控接口，定义NodeManager上所有容器资源监控的核心能力
 * 负责跟踪容器资源使用情况，提供资源利用率统计信息
 */
public interface ContainersMonitor extends Service,
    EventHandler<ContainersMonitorEvent>, ResourceView {
  /**
   * 获取当前所有容器的资源利用率信息
   * @return 容器资源利用率统计结果
   */
  ResourceUtilization getContainersUtilization();

  /**
   * 获取虚拟内存与物理内存的比值
   * @return 虚拟内存比例系数
   */
  float getVmemRatio();

  /**
   * 从当前资源利用率中减去节点预留资源
   * @param resourceUtil 待调整的资源利用率对象
   */
  void subtractNodeResourcesFromResourceUtilization(
      ResourceUtilization resourceUtil);

  /**
   * Utility method to add a {@link Resource} to the
   * {@link ResourceUtilization}.
   * @param containersMonitor Containers Monitor.
   * @param resourceUtil Resource Utilization.
   * @param resource Resource.
   */
  static void increaseResourceUtilization(
      ContainersMonitor containersMonitor, ResourceUtilization resourceUtil,
      Resource resource) {
    float vCores = (float) resource.getVirtualCores();
    int vmem = (int) (resource.getMemorySize()
        * containersMonitor.getVmemRatio());
    resourceUtil.addTo((int)resource.getMemorySize(), vmem, vCores);
  }

  /**
   * Utility method to subtract a {@link Resource} from the
   * {@link ResourceUtilization}.
   * @param containersMonitor Containers Monitor.
   * @param resourceUtil Resource Utilization.
   * @param resource Resource.
   */
  static void decreaseResourceUtilization(
      ContainersMonitor containersMonitor, ResourceUtilization resourceUtil,
      Resource resource) {
    float vCores = (float) resource.getVirtualCores();
    int vmem = (int) (resource.getMemorySize()
        * containersMonitor.getVmemRatio());
    resourceUtil.subtractFrom((int)resource.getMemorySize(), vmem, vCores);
  }

  /**
   * Set the allocated resources for containers.
   * @param resource Resources allocated for the containers.
   */
  void setAllocatedResourcesForContainers(Resource resource);
}