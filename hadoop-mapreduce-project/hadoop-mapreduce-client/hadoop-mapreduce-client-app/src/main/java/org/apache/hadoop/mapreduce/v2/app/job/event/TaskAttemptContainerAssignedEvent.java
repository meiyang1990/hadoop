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

package org.apache.hadoop.mapreduce.v2.app.job.event;

import java.util.Map;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessControl;
import org.apache.hadoop.yarn.api.records.Container;

/**
 * 任务尝试容器分配事件，当YARN为某个任务尝试分配容器后触发该事件
 * 携带分配到的容器信息和应用程序访问控制列表，供MR App处理后续任务启动流程
 */
public class TaskAttemptContainerAssignedEvent extends TaskAttemptEvent {

  // 分配给本次任务尝试的YARN容器实例
  private final Container container;
  // 应用程序访问控制列表，定义不同用户对应用的访问权限
  private final Map<ApplicationAccessType, String> applicationACLs;

  /**
   * 构造任务尝试容器分配事件
   * @param id 任务尝试唯一标识
   * @param container YARN分配的容器实例
   * @param applicationACLs 应用程序访问控制列表
   */
  public TaskAttemptContainerAssignedEvent(TaskAttemptId id,
      Container container, Map<ApplicationAccessType, String> applicationACLs) {
    super(id, TaskAttemptEventType.TA_ASSIGNED);
    this.container = container;
    this.applicationACLs = applicationACLs;
  }

  /**
   * 获取YARN为本次任务尝试分配的容器
   * @return 分配的容器实例
   */
  public Container getContainer() {
    return this.container;
  }

  /**
   * 获取应用程序访问控制列表
   * @return 访问权限映射表，键为访问类型，值为授权用户列表
   */
  public Map<ApplicationAccessType, String> getApplicationACLs() {
    return this.applicationACLs;
  }
}