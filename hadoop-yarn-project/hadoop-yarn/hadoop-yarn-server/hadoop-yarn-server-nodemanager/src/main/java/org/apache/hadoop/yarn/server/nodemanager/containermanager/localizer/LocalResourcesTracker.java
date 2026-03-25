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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceEvent;

/**
 * 本文件定义了同一可见性级别本地化资源的追踪接口，是NM本地资源追踪的核心抽象
 * 负责统一管理相同可见性（公有/私有/应用私有）的资源本地化状态，处理资源事件
 */
/**
 * Component tracking resources all of the same {@link LocalResourceVisibility}
 * 
 */
interface LocalResourcesTracker
    extends EventHandler<ResourceEvent>, Iterable<LocalizedResource> {

  /**
   * 移除指定本地化资源，通过删除服务调度资源删除操作
   * @param req 要移除的本地化资源
   * @param delService 删除服务
   * @return 是否成功移除资源
   */
  boolean remove(LocalizedResource req, DeletionService delService);

  /**
   * 获取资源本地化存储的目标路径
   * @param req 本地化资源请求
   * @param localDirPath NM本地目录路径
   * @param delService 删除服务
   * @return 资源本地化后的目标路径
   */
  Path getPathForLocalization(LocalResourceRequest req, Path localDirPath,
      DeletionService delService);

  /**
   * 获取当前资源追踪器所属用户，私有资源追踪器对应用户，公有/应用级资源返回null
   * @return 所属用户名
   */
  String getUser();

  /**
   * 根据资源请求获取对应的已本地化资源对象
   * @param request 本地化资源请求
   * @return 已本地化资源对象，不存在则返回null
   */
  LocalizedResource getLocalizedResource(LocalResourceRequest request);
}