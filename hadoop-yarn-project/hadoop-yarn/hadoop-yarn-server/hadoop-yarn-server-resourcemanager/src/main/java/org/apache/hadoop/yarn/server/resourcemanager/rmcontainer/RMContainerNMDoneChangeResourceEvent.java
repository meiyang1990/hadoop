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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * NodeManager已完成容器资源变更事件，用于通知ResourceManager节点侧已经完成资源调整。
 */
public class RMContainerNMDoneChangeResourceEvent extends RMContainerEvent {

  // NodeManager调整完成后的容器最新资源配置
  private final Resource nmContainerResource;

  /**
   * 构造NodeManager容器资源变更完成事件。
   * @param containerId 目标容器ID
   * @param nmContainerResource NodeManager侧调整后的容器最新资源
   */
  public RMContainerNMDoneChangeResourceEvent(
      ContainerId containerId, Resource nmContainerResource) {
    super(containerId, RMContainerEventType.NM_DONE_CHANGE_RESOURCE);
    this.nmContainerResource = nmContainerResource;
  }

  /**
   * 获取NodeManager侧调整完成后的容器最新资源配置。
   * @return 调整后的容器资源
   */
  public Resource getNMContainerResource() {
    return nmContainerResource;
  }
}