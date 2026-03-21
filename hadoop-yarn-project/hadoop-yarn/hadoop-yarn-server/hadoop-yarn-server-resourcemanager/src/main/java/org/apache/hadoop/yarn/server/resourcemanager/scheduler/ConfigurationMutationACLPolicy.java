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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

/**
 * YARN调度器配置修改访问控制策略接口，定义判断用户是否有权限修改调度配置的契约。
 */
public interface ConfigurationMutationACLPolicy {

  /**
   * 使用配置和RM上下文初始化ACL策略。
   * @param conf 初始化所用的配置对象
   * @param rmContext ResourceManager上下文对象
   */
  void init(Configuration conf, RMContext rmContext);

  /**
   * 检查当前用户是否允许执行指定的配置修改操作。
   * @param user 发起配置修改请求的用户信息
   * @param confUpdate 待修改的调度配置信息
   * @return true表示允许修改，false表示拒绝修改
   */
  boolean isMutationAllowed(UserGroupInformation user, SchedConfUpdateInfo
      confUpdate);

}