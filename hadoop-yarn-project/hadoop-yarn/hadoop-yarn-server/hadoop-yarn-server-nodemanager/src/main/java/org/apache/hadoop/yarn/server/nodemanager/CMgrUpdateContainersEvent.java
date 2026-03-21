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

import org.apache.hadoop.yarn.api.records.Container;
import java.util.List;

/**
 * YARN NodeManager中，用于NodeStatusUpdater向ContainerManager
 * 通知从ResourceManager收到的容器更新命令的事件
 */
public class CMgrUpdateContainersEvent extends ContainerManagerEvent {

  // 待更新的容器列表
  private final List<Container> containersToUpdate;

  /**
   * 构造容器更新事件
   * @param containersToUpdate 待更新的容器列表
   */
  public CMgrUpdateContainersEvent(List<Container> containersToUpdate) {
    super(ContainerManagerEventType.UPDATE_CONTAINERS);
    this.containersToUpdate = containersToUpdate;
  }

  /**
   * 获取待更新的容器列表
   * @return 待更新的容器列表
   */
  public List<Container> getContainersToUpdate() {
    return this.containersToUpdate;
  }
}