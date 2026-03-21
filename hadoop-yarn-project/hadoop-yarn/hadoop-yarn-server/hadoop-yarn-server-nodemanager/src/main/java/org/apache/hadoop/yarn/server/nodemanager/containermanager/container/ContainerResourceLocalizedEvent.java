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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;

/**
 * 容器资源本地化完成事件，当容器所需资源在NodeManager本地化完成后触发
 */
public class ContainerResourceLocalizedEvent extends ContainerResourceEvent {

  // 本地化后资源在本地文件系统的存储路径
  private final Path loc;

  // 资源大小，符号标识资源来源：大于0表示本次下载的资源，小于0表示复用缓存的资源
  private long size;

  /**
   * 构造资源本地化完成事件
   * @param container 目标容器ID
   * @param rsrc 已本地化的资源请求
   * @param loc 本地化后资源的本地路径
   */
  public ContainerResourceLocalizedEvent(ContainerId container, LocalResourceRequest rsrc,
      Path loc) {
    super(container, ContainerEventType.RESOURCE_LOCALIZED, rsrc);
    this.loc = loc;
  }

  /**
   * 获取本地化后资源的本地路径
   * @return 资源本地文件路径
   */
  public Path getLocation() {
    return loc;
  }

  /**
   * 获取本地化资源大小
   * @return 资源大小，正数为本次下载，负数为缓存复用
   */
  public long getSize() {
    return size;
  }

  /**
   * 设置本地化资源大小
   * @param size 资源大小
   */
  public void setSize(long size) {
    this.size = size;
  }

}