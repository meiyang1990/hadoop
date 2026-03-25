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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container
    .Container;

/**
 * NodeManager辅助服务事件，用于封装辅助服务相关的各类事件信息，
 * 在容器管理器的状态机流转中传递辅助服务操作请求和数据。
 */
public class AuxServicesEvent extends AbstractEvent<AuxServicesEventType> {

  private final String user;
  private final String serviceId;
  private final ByteBuffer serviceData;
  private final ApplicationId appId;
  private final Container container;

  /**
   * 构造仅包含事件类型和应用ID的辅助服务事件。
   * @param eventType 事件类型
   * @param appId 应用ID
   */
  public AuxServicesEvent(AuxServicesEventType eventType, ApplicationId appId) {
    this(eventType, null, appId, null, null);
  }

  /**
   * 构造关联容器的辅助服务事件，自动从容器提取应用ID。
   * @param eventType 事件类型
   * @param container 关联的容器对象
   */
  public AuxServicesEvent(AuxServicesEventType eventType, Container container) {
    this(eventType, null, container.getContainerId().getApplicationAttemptId()
        .getApplicationId(), null, null, container);
  }

  /**
   * 构造包含完整用户、应用、服务信息的辅助服务事件。
   * @param eventType 事件类型
   * @param user 提交应用的用户名
   * @param appId 应用ID
   * @param serviceId 目标辅助服务ID
   * @param serviceData 辅助服务处理所需数据
   */
  public AuxServicesEvent(AuxServicesEventType eventType, String user,
      ApplicationId appId, String serviceId, ByteBuffer serviceData) {
    this(eventType, user, appId, serviceId, serviceData, null);
  }

  /**
   * 全参数构造辅助服务事件。
   * @param eventType 事件类型
   * @param user 提交应用的用户名
   * @param appId 应用ID
   * @param serviceId 目标辅助服务ID
   * @param serviceData 辅助服务处理所需数据
   * @param container 关联的容器对象，可以为null
   */
  public AuxServicesEvent(AuxServicesEventType eventType, String user,
      ApplicationId appId, String serviceId, ByteBuffer serviceData,
        Container container) {
    super(eventType);
    this.user = user;
    this.appId = appId;
    this.serviceId = serviceId;
    this.serviceData = serviceData;
    this.container = container;
  }

  /**
   * 获取目标辅助服务ID。
   * @return 辅助服务ID
   */
  public String getServiceID() {
    return serviceId;
  }

  /**
   * 获取辅助服务处理所需数据。
   * @return 服务数据字节缓冲区
   */
  public ByteBuffer getServiceData() {
    return serviceData;
  }

  /**
   * 获取提交应用的用户名。
   * @return 用户名
   */
  public String getUser() {
    return user;
  }

  /**
   * 获取关联的应用ID。
   * @return 应用ID
   */
  public ApplicationId getApplicationID() {
    return appId;
  }

  /**
   * 获取关联的容器对象。
   * @return 容器对象，可能为null
   */
  public Container getContainer() {
    return container;
  }

}