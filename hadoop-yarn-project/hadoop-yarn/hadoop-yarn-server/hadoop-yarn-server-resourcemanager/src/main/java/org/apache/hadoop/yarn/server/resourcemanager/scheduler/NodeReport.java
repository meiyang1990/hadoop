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

/**
 * YARN资源调度器节点资源使用报告，存储节点当前已使用资源和容器数量信息
 */
@Private
@Stable
public class NodeReport {
  private final Resource usedResources;
  private final int numContainers;
  
  /**
   * 构造节点资源使用报告对象
   * @param used 节点已使用的资源信息
   * @param numContainers 节点上运行的容器数量
   */
  public NodeReport(Resource used, int numContainers) {
    this.usedResources = used;
    this.numContainers = numContainers;
  }

  /**
   * 获取节点已使用资源信息
   * @return 节点已使用资源
   */
  public Resource getUsedResources() {
    return usedResources;
  }

  /**
   * 获取节点上运行的容器总数
   * @return 容器数量
   */
  public int getNumContainers() {
    return numContainers;
  }
}