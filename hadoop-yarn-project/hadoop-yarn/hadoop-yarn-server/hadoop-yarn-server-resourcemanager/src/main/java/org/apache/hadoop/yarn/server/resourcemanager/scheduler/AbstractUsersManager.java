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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * YARN ResourceManager 用户管理器抽象接口，负责跟踪系统中活跃用户状态，用于用户级资源调度限额管理
 */
@Private
public interface AbstractUsersManager {
  /**
   * 激活应用，标记对应用户存在待处理资源请求
   *
   * @param user
   *          提交应用的用户名
   * @param applicationId
   *          待激活的应用ID
   */
  void activateApplication(String user, ApplicationId applicationId);

  /**
   * 停用应用，移除对应用户的待处理资源请求标记
   *
   * @param user
   *          提交应用的用户名
   * @param applicationId
   *          待停用的应用ID
   */
  void deactivateApplication(String user, ApplicationId applicationId);

  /**
   * 获取当前活跃用户数量，活跃用户定义为存在带待处理资源请求应用的用户
   *
   * @return 活跃用户总数
   */
  int getNumActiveUsers();
}