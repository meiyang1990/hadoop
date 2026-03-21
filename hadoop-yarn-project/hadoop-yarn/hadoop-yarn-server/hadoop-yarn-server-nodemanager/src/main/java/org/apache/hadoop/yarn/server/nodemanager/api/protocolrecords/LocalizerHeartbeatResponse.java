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
package org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords;

import java.util.List;
import org.apache.hadoop.yarn.server.nodemanager.api.*;

/**
 * 本地化服务心跳响应接口，定义NodeManager返回给本地化容器的响应结构
 * 用于NodeManager向本地化器下发指令和待本地化资源列表
 */
public interface LocalizerHeartbeatResponse {

  /**
   * 获取NodeManager要求本地化器执行的动作
   * @return 本地化动作指令
   */
  public LocalizerAction getLocalizerAction();
  /**
   * 设置本地化器需要执行的动作指令
   * @param action 本地化动作指令
   */
  public void setLocalizerAction(LocalizerAction action);

  /**
   * 获取需要本地化的资源规范列表
   * @return 待本地化资源规范列表
   */
  public List<ResourceLocalizationSpec> getResourceSpecs();
  /**
   * 设置需要本地化的资源规范列表
   * @param rsrcs 待本地化资源规范列表
   */
  public void setResourceSpecs(List<ResourceLocalizationSpec> rsrcs);
}