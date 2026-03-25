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
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

/**
 * 文件：调度配置修改访问控制默认策略实现
 * 功能：为YARN资源管理器调度器配置在线修改提供默认的ACL权限检查，仅允许YARN管理员修改配置
 */
public class DefaultConfigurationMutationACLPolicy implements
    ConfigurationMutationACLPolicy {

  // YARN权限验证器实例
  private YarnAuthorizationProvider authorizer;

  @Override
  public void init(Configuration conf, RMContext rmContext) {
    // 从配置初始化权限验证器实例
    authorizer = YarnAuthorizationProvider.getInstance(conf);
  }

  @Override
  public boolean isMutationAllowed(UserGroupInformation user,
      SchedConfUpdateInfo confUpdate) {
    // 仅当用户是YARN管理员时才允许修改配置
    return authorizer.isAdmin(user);
  }
}