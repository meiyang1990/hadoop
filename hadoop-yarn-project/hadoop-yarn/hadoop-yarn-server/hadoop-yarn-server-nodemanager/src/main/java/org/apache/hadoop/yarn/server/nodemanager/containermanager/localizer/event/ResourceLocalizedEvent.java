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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;

/**
 * 资源本地化完成事件，通知资源本地化已完成并携带本地化后的资源信息
 */
public class ResourceLocalizedEvent extends ResourceEvent {

  // 本地化后资源的大小
  private final long size;
  // 本地化后资源在本地文件系统的路径
  private final Path location;

  /**
   * 构造资源本地化完成事件
   * @param rsrc 对应的资源请求
   * @param location 本地化后资源的本地路径
   * @param size 本地化资源大小
   */
  public ResourceLocalizedEvent(LocalResourceRequest rsrc, Path location,
      long size) {
    super(rsrc, ResourceEventType.LOCALIZED);
    this.size = size;
    this.location = location;
  }

  /**
   * 获取本地化资源的本地路径
   * @return 本地化资源路径
   */
  public Path getLocation() {
    return location;
  }

  /**
   * 获取本地化资源的大小
   * @return 本地化资源大小，单位字节
   */
  public long getSize() {
    return size;
  }

}