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

/**
 * 容器更新已获取事件，用于标记RM已获取NM返回的容器更新响应，通知RM容器状态机处理更新结果。
 */
public class RMContainerUpdatesAcquiredEvent extends RMContainerEvent  {
  // 标记本次更新是否为容器资源增加
  private final boolean increasedContainer;
  
  /**
   * 构造容器更新已获取事件。
   * @param containerId 目标容器ID
   * @param increasedContainer 是否为容器资源增加更新
   */
  public RMContainerUpdatesAcquiredEvent(ContainerId containerId,
      boolean increasedContainer) {
    super(containerId, RMContainerEventType.ACQUIRE_UPDATED_CONTAINER);
    this.increasedContainer = increasedContainer; 
  }
  
  /**
   * 获取本次更新是否为资源增加标记。
   * @return true表示本次更新是容器资源增加，false表示资源减少
   */
  public boolean isIncreasedContainer() {
    return increasedContainer;
  }
}