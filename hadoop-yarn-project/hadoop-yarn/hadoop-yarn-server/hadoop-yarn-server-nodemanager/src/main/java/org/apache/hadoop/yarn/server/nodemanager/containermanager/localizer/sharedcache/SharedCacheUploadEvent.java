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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;

/**
 * 节点管理器共享缓存上传事件，封装共享缓存资源上传所需的上下文信息
 */
@Private
@Unstable
public class SharedCacheUploadEvent extends
    AbstractEvent<SharedCacheUploadEventType> {
  // 待上传资源集合，key为资源请求，value为资源本地路径
  private final Map<LocalResourceRequest,Path> resources;
  // 容器启动上下文，关联本次上传所属的容器
  private final ContainerLaunchContext context;
  // 提交本次请求的用户名
  private final String user;

  /**
   * 构造共享缓存上传事件对象
   * @param resources 待上传资源集合
   * @param context 容器启动上下文
   * @param user 提交请求用户名
   * @param eventType 事件类型
   */
  public SharedCacheUploadEvent(Map<LocalResourceRequest,Path> resources,
      ContainerLaunchContext context, String user,
      SharedCacheUploadEventType eventType) {
    super(eventType);
    this.resources = resources;
    this.context = context;
    this.user = user;
  }

  /**
   * 获取待上传资源集合
   * @return 待上传资源Map
   */
  public Map<LocalResourceRequest,Path> getResources() {
    return resources;
  }

  /**
   * 获取关联的容器启动上下文
   * @return 容器启动上下文对象
   */
  public ContainerLaunchContext getContainerLaunchContext() {
    return context;
  }

  /**
   * 获取提交请求的用户名
   * @return 用户名字符串
   */
  public String getUser() {
    return user;
  }
}