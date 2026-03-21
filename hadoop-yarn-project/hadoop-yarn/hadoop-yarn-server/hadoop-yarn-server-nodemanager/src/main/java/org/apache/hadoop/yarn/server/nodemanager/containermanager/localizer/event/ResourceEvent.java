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

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;

/**
 * 资源本地化事件，封装本地资源请求相关事件信息
 * 用于节点管理器本地化流程中传递资源请求事件
 */
public class ResourceEvent extends AbstractEvent<ResourceEventType> {

  // 关联的本地资源请求
  private final LocalResourceRequest rsrc;

  /**
   * 构造资源事件
   * @param rsrc 本地资源请求
   * @param type 事件类型
   */
  public ResourceEvent(LocalResourceRequest rsrc, ResourceEventType type) {
    super(type);
    this.rsrc = rsrc;
  }

  /**
   * 获取关联的本地资源请求
   * @return 本地资源请求对象
   */
  public LocalResourceRequest getLocalResourceRequest() {
    return rsrc;
  }

}