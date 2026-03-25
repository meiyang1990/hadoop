// 这个文件已经全部加上中文注释
/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.hadoop.yarn.server.router.security.authorize;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.yarn.api.ApplicationClientProtocolPB;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocolPB;

/**
 * YARN Router服务端协议的权限策略提供者，为Router各个RPC协议提供访问控制规则定义。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RouterPolicyProvider extends PolicyProvider {

  // 单例实例，使用volatile保证双重检查锁定的可见性
  private static volatile RouterPolicyProvider routerPolicyProvider = null;

  private RouterPolicyProvider() {
  }

  /**
   * 获取RouterPolicyProvider单例实例，使用双重检查锁定实现线程安全的延迟初始化。
   * @return RouterPolicyProvider单例对象
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static RouterPolicyProvider getInstance() {
    if (routerPolicyProvider == null) {
      synchronized (RouterPolicyProvider.class) {
        if (routerPolicyProvider == null) {
          routerPolicyProvider = new RouterPolicyProvider();
        }
      }
    }
    return routerPolicyProvider;
  }

  // 定义Router需要授权的RPC服务列表，每个服务关联配置项和协议接口
  private static final Service[] ROUTER_SERVICES = new Service[] {
      new Service(
          YarnConfiguration.YARN_SECURITY_SERVICE_AUTHORIZATION_APPLICATIONCLIENT_PROTOCOL,
          ApplicationClientProtocolPB.class),
      new Service(
          YarnConfiguration.YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCEMANAGER_ADMINISTRATION_PROTOCOL,
          ResourceManagerAdministrationProtocolPB.class), };

  @Override
  public Service[] getServices() {
    return ROUTER_SERVICES;
  }

}