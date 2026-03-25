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

/**
 * 文件说明：ResourceManager HTTP认证过滤器初始化器
 * 核心职责：为YARN ResourceManager的Web服务初始化定制化HTTP认证过滤器，
 * 专门处理RM委托令牌认证和代理用户配置注入
 */
package org.apache.hadoop.yarn.server.security.http;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;

@Unstable
public class RMAuthenticationFilterInitializer extends FilterInitializer {

  String configPrefix;

  /**
   * 构造函数，初始化HTTP认证配置前缀
   */
  public RMAuthenticationFilterInitializer() {
    this.configPrefix = "hadoop.http.authentication.";
  }

  /**
   * 创建认证过滤器配置，整合通用认证配置、代理用户配置和RM委托令牌配置
   * @param conf YARN配置对象
   * @return 组装完成的过滤器配置键值对
   */
  protected Map<String, String> createFilterConfig(Configuration conf) {
    // 从通用认证初始化器获取基础配置
    Map<String, String> filterConfig = AuthenticationFilterInitializer
        .getFilterConfigMap(conf, configPrefix);

    // Before conf object is passed in, RM has already processed it and used RM
    // specific configs to overwrite hadoop common ones. Hence we just need to
    // source hadoop.proxyuser configs here.

    // 将代理用户配置注入过滤器配置
    for (Map.Entry<String, String> entry : conf.
        getPropsWithPrefix(ProxyUsers.CONF_HADOOP_PROXYUSER).entrySet()) {
      filterConfig.put("proxyuser" + entry.getKey(), entry.getValue());
    }

    // 设置委托令牌类型为ResourceManager专属令牌类型
    filterConfig.put(DelegationTokenAuthenticationHandler.TOKEN_KIND,
        RMDelegationTokenIdentifier.KIND_NAME.toString());

    return filterConfig;
  }

  @Override
  /**
   * 初始化并注册RM认证过滤器到Web容器
   * @param container 过滤器容器，用于注册过滤器
   * @param conf YARN配置对象
   */
  public void initFilter(FilterContainer container, Configuration conf) {
    Map<String, String> filterConfig = createFilterConfig(conf);
    container.addFilter("RMAuthenticationFilter",
      RMAuthenticationFilter.class.getName(), filterConfig);
  }
}