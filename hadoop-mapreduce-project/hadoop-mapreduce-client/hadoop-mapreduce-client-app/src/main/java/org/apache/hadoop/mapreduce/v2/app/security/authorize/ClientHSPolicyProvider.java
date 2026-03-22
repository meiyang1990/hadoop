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
package org.apache.hadoop.mapreduce.v2.app.security.authorize;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocol;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocolPB;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.tools.GetUserMappingsProtocol;

/**
 * MapReduce历史服务器协议的安全授权策略提供者，为历史服务器暴露的各个服务协议提供访问控制策略定义。
 * 继承自Hadoop通用的PolicyProvider，为服务授权模块提供需要保护的服务列表。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ClientHSPolicyProvider extends PolicyProvider {
  
  /**
   * 历史服务器所有需要进行安全授权检查的服务列表，
   * 每个服务关联配置中的授权配置项和对应的协议接口。
   */
  private static final Service[] mrHSServices = 
      new Service[] {
    // 历史服务器客户端协议服务授权配置
    new Service(
        JHAdminConfig.MR_HS_SECURITY_SERVICE_AUTHORIZATION,
        HSClientProtocolPB.class),
    // 获取用户映射关系协议服务授权配置
    new Service(
        CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_GET_USER_MAPPINGS,
        GetUserMappingsProtocol.class),
    // 刷新用户映射关系协议服务授权配置
    new Service(
        CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_REFRESH_USER_MAPPINGS,
        RefreshUserMappingsProtocol.class),
    // 历史服务器管理刷新协议服务授权配置
    new Service(
        JHAdminConfig.MR_HS_SECURITY_SERVICE_AUTHORIZATION_ADMIN_REFRESH,
        HSAdminRefreshProtocol.class)
  };

  /**
   * 获取当前提供者定义的所有需要授权检查的服务列表
   * @return 历史服务器所有需要授权检查的服务数组
   */
  @Override
  public Service[] getServices() {
    return mrHSServices;
  }
}