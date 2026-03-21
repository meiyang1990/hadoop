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

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.nodemanager.ContainerManagerEvent;
import org.apache.hadoop.yarn.server.nodemanager.ContainerManagerEventType;

/**
 * 容器管理器处理已完成应用的清理事件，用于通知NodeManager清理已完成应用的相关资源
 */
public class CMgrCompletedAppsEvent extends ContainerManagerEvent {

  // 需要清理的应用ID列表
  private final List<ApplicationId> appsToCleanup;
  // 触发清理的原因
  private final Reason reason;

  /**
   * 构造已完成应用清理事件
   * @param appsToCleanup 需要清理的应用ID列表
   * @param reason 触发清理的原因
   */
  public CMgrCompletedAppsEvent(List<ApplicationId> appsToCleanup, Reason reason) {
    super(ContainerManagerEventType.FINISH_APPS);
    this.appsToCleanup = appsToCleanup;
    this.reason = reason;
  }

  /**
   * 获取需要清理的应用ID列表
   * @return 需要清理的应用ID列表
   */
  public List<ApplicationId> getAppsToCleanup() {
    return this.appsToCleanup;
  }

  /**
   * 获取触发清理的原因
   * @return 触发清理的原因枚举
   */
  public Reason getReason() {
    return reason;
  }

  /**
   * 触发应用清理的原因枚举
   */
  public enum Reason {
    /**
     * 节点管理器关闭导致应用被终止清理
     */
    ON_SHUTDOWN, 

    /**
     * 资源管理器主动请求终止清理应用
     */
    BY_RESOURCEMANAGER
  }
}