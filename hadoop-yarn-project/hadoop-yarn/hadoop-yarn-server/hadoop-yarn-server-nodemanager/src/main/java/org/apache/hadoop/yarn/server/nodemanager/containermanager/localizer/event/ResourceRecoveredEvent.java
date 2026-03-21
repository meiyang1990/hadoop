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
 * NM节点资源恢复完成事件，NodeManager重启后恢复本地资源时触发，
 * 通知资源本地化模块该资源已经在本地恢复完成。
 */
public class ResourceRecoveredEvent extends ResourceEvent {

  // 恢复完成的资源本地路径
  private final Path localPath;
  // 恢复完成的资源文件大小
  private final long size;

  /**
   * 构造资源恢复完成事件。
   * @param rsrc 资源请求信息
   * @param localPath 资源本地存储路径
   * @param size 资源文件大小
   */
  public ResourceRecoveredEvent(LocalResourceRequest rsrc, Path localPath,
      long size) {
    super(rsrc, ResourceEventType.RECOVERED);
    this.localPath = localPath;
    this.size = size;
  }

  /**
   * 获取恢复完成资源的本地路径。
   * @return 本地文件路径
   */
  public Path getLocalPath() {
    return localPath;
  }

  /**
   * 获取恢复完成资源的文件大小。
   * @return 文件大小（字节）
   */
  public long getSize() {
    return size;
  }
}