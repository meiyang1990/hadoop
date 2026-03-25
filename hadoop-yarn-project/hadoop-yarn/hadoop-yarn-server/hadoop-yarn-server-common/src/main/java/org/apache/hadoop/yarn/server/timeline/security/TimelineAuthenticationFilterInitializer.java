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

package org.apache.hadoop.yarn.server.timeline.security;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.PseudoDelegationTokenAuthenticationHandler;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_HTTP_AUTH_PREFIX;

import java.util.HashMap;
import java.util.Map;

/**
 * 时间线服务HTTP认证过滤器初始化器，为TimelineServer初始化支持Kerberos SPNEGO和代理用户认证
 * 为时间线服务启用Kerberos HTTP SPNEGO + 委托令牌认证机制
 * 相关配置前缀为 hadoop.http.authentication.，具体配置选项可参考core-default.xml中HTTP Authentication注释部分
 */
public class TimelineAuthenticationFilterInitializer extends FilterInitializer {

  @VisibleForTesting
  Map<String, String> filterConfig;

  /**
   * 从配置中生成并合并认证过滤器配置
   * @param conf YARN配置对象
   */
  protected void setAuthFilterConfig(Configuration conf) {
    filterConfig = new HashMap<String, String>();

    // 加载全局代理用户配置，将配置前缀转换为过滤器要求的格式
    for (Map.Entry<String, String> entry : conf
        .getPropsWithPrefix(ProxyUsers.CONF_HADOOP_PROXYUSER).entrySet()) {
      filterConfig.put("proxyuser" + entry.getKey(), entry.getValue());
    }

    // 时间线服务专属的代理用户配置会覆盖全局 hadoop.proxyuser 配置
    Map<String, String> timelineAuthProps =
        AuthenticationFilterInitializer.getFilterConfigMap(conf,
                TIMELINE_HTTP_AUTH_PREFIX);

    // 合并时间线专属认证配置，覆盖同名配置会覆盖之前的全局配置
    filterConfig.putAll(timelineAuthProps);
  }

  /**
   * 获取生成好的过滤器配置，仅用于测试
   * @return 过滤器配置Map
   */
  protected Map<String, String> getFilterConfig() {
    return filterConfig;
  }

  /**
   * 初始化TimelineAuthenticationFilter，将时间线服务相关认证配置注入过滤器
   * 加载所有以 TIMELINE_HTTP_AUTH_PREFIX 为前缀的YARN配置到过滤器
   *
   * @param container 过滤器容器，用于注册过滤器
   * @param conf 运行时配置
   */
  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    // 生成并加载认证配置
    setAuthFilterConfig(conf);

    // 获取配置中的认证类型
    String authType = filterConfig.get(AuthenticationFilter.AUTH_TYPE);
    // 如果使用伪认证，替换为支持委托令牌的伪认证处理器
    if (authType.equals(PseudoAuthenticationHandler.TYPE)) {
      filterConfig.put(AuthenticationFilter.AUTH_TYPE,
          PseudoDelegationTokenAuthenticationHandler.class.getName());
    } 
    // 如果使用Kerberos认证，替换为支持委托令牌的Kerberos认证处理器
    else if (authType.equals(KerberosAuthenticationHandler.TYPE)) {
      filterConfig.put(AuthenticationFilter.AUTH_TYPE,
          KerberosDelegationTokenAuthenticationHandler.class.getName());
    }
    // 设置委托令牌类型为时间线服务专属令牌类型
    filterConfig.put(DelegationTokenAuthenticationHandler.TOKEN_KIND,
        TimelineDelegationTokenIdentifier.KIND_NAME.toString());

    // 向过滤器容器注册全局时间线认证过滤器
    container.addGlobalFilter("Timeline Authentication Filter",
        TimelineAuthenticationFilter.class.getName(),
        filterConfig);
  }
}