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
package org.apache.hadoop.yarn.server.webapp;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;

/**
 * 文件级：YARN Web服务端基础应用信息封装类，为日志查询Servlet提供精简应用信息
 * 工具类，封装{@link LogServlet}所需的应用基本信息，仅保留状态和用户字段
 */
@InterfaceAudience.LimitedPrivate({"YARN"})
@InterfaceStability.Unstable
class BasicAppInfo {
  private final YarnApplicationState appState;
  private final String user;

  /**
   * 构造函数，从状态和用户名创建基本应用信息对象
   */
  BasicAppInfo(YarnApplicationState appState, String user) {
    this.appState = appState;
    this.user = user;
  }

  /**
   * 从完整AppInfo对象创建精简的BasicAppInfo对象
   * @param report 完整应用信息对象
   * @return 精简后的基本应用信息对象
   */
  static BasicAppInfo fromAppInfo(AppInfo report) {
    return new BasicAppInfo(report.getAppState(), report.getUser());
  }

  /**
   * 获取应用运行状态
   * @return 应用运行状态枚举
   */
  YarnApplicationState getAppState() {
    return this.appState;
  }

  /**
   * 获取应用提交用户名
   * @return 提交应用的用户名
   */
  String getUser() {
    return this.user;
  }
}