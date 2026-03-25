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

package org.apache.hadoop.yarn.server.nodemanager;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * 容器管理器处理已完成容器的事件，封装需要清理的已完成容器列表和完成原因。
 * 是NodeManager容器状态机处理容器终止流程的事件类型。
 */
public class CMgrCompletedContainersEvent extends ContainerManagerEvent {

  // 需要清理的已完成容器ID列表
  private final List<ContainerId> containerToCleanup;
  // 容器完成的原因
  private final Reason reason;

  /**
   * 构造已完成容器处理事件。
   * @param containersToCleanup 需要清理的容器ID列表
   * @param reason 容器完成原因
   */
  public CMgrCompletedContainersEvent(List<ContainerId> containersToCleanup,
                                      Reason reason) {
    super(ContainerManagerEventType.FINISH_CONTAINERS);
    this.containerToCleanup = containersToCleanup;
    this.reason = reason;
  }

  /**
   * 获取需要清理的容器ID列表。
   * @return 需要清理的容器ID列表
   */
  public List<ContainerId> getContainersToCleanup() {
    return this.containerToCleanup;
  }

  /**
   * 获取容器完成的原因。
   * @return 容器完成原因枚举
   */
  public Reason getReason() {
    return reason;
  }

  /**
   * 容器完成终止的原因枚举，定义不同场景下的容器清理触发条件。
   */
  public enum Reason {
    /**
     * Container is killed as NodeManager is shutting down
     */
    ON_SHUTDOWN,

    /**
     * Container is killed as the Nodemanager is re-syncing with the
     * ResourceManager
     */
    ON_NODEMANAGER_RESYNC,

    /**
     * Container is killed on request by the ResourceManager
     */
    BY_RESOURCEMANAGER
  }

}