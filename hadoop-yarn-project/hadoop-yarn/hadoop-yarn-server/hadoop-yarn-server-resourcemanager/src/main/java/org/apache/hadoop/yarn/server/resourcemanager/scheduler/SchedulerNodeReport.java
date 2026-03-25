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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;

/**
 * YARN调度器节点资源使用报告，封装了单个节点的资源分配与使用状态信息
 */
@Private
@Stable
public class SchedulerNodeReport {
  private final Resource used;
  private final Resource avail;
  private final ResourceUtilization utilization;
  private final int num;
  
  /**
   * 根据调度节点信息构造节点资源使用报告
   * @param node 调度器节点对象
   */
  public SchedulerNodeReport(SchedulerNode node) {
    this.used = node.getAllocatedResource();
    this.avail = node.getUnallocatedResource();
    this.num = node.getNumContainers();
    this.utilization = node.getNodeUtilization();
  }
  
  /**
   * @return 节点当前已使用的资源总量
   */
  public Resource getUsedResource() {
    return used;
  }

  /**
   * @return 节点当前可用的资源总量
   */
  public Resource getAvailableResource() {
    return avail;
  }

  /**
   * @return 节点当前正在运行的容器数量
   */
  public int getNumContainers() {
    return num;
  }

  /**
   *
   * @return 节点当前资源利用率信息
   */
  public ResourceUtilization getUtilization() {
    return utilization;
  }
}