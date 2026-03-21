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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.api.ContainerLogContext;

/**
 * 应用日志聚合器接口，定义YARN NodeManager上单个应用的日志聚合核心行为
 * 负责将应用所有容器的本地日志聚合上传到远程存储供后续查询
 */
public interface AppLogAggregator extends Runnable {

  /**
   * 启动指定容器的日志聚合流程
   * @param logContext 容器日志上下文，包含容器日志路径、标识等信息
   */
  void startContainerLogAggregation(ContainerLogContext logContext);

  /**
   * 中止当前正在进行的日志聚合
   */
  void abortLogAggregation();

  /**
   * 完成整个应用的日志聚合，执行收尾清理工作
   */
  void finishLogAggregation();

  /**
   * 禁用当前应用的日志聚合功能
   */
  void disableLogAggregation();

  /**
   * 启用当前应用的日志聚合功能
   */
  void enableLogAggregation();

  /**
   * 查询日志聚合功能是否已启用
   * @return true表示已启用，false表示已禁用
   */
  boolean isAggregationEnabled();

  /**
   * 更新日志聚合操作所需的认证凭证
   * @param cred 凭证存储对象
   * @return 认证通过后的用户信息对象
   */
  UserGroupInformation updateCredentials(Credentials cred);
}