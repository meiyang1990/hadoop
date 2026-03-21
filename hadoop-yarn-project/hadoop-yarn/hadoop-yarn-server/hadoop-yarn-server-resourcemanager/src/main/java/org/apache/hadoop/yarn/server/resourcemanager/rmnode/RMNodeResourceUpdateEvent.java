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

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceOption;

/**
 * YARN ResourceManager 节点资源更新事件，用于通知NodeManager资源容量发生变更。
 */
public class RMNodeResourceUpdateEvent extends RMNodeEvent {

  // 包含更新后的资源信息与更新选项
  private final ResourceOption resourceOption;
  
  /**
   * 构造节点资源更新事件。
   * @param nodeId 目标节点ID
   * @param resourceOption 新的资源配置选项
   */
  public RMNodeResourceUpdateEvent(NodeId nodeId, ResourceOption resourceOption) {
    super(nodeId, RMNodeEventType.RESOURCE_UPDATE);
    this.resourceOption = resourceOption;
  }

  /**
   * 获取更新后的资源配置选项。
   * @return 资源更新选项，包含新资源总量和更新标识
   */
  public ResourceOption getResourceOption() {
    return resourceOption;
  }

}