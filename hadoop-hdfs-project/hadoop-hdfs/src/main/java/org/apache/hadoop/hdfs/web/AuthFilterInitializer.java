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

package org.apache.hadoop.hdfs.web;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;

/**
 * HDFS Web UI认证过滤器初始化器，负责初始化并注册AuthFilter到HDFS HTTP服务
 * 继承FilterInitializer，适配Hadoop HTTP服务的过滤器初始化机制
 */
public class AuthFilterInitializer extends FilterInitializer {

  private String configPrefix;

  /**
   * 构造函数，初始化认证配置前缀
   */
  public AuthFilterInitializer() {
    this.configPrefix = "hadoop.http.authentication.";
  }

  /**
   * 根据Hadoop配置生成AuthFilter的过滤器配置参数
   * @param conf Hadoop全局配置对象
   * @return 生成的过滤器配置键值对
   */
  protected Map<String, String> createFilterConfig(Configuration conf) {
    // 从公共工具方法加载基础认证配置
    Map<String, String> filterConfig = AuthenticationFilterInitializer
        .getFilterConfigMap(conf, configPrefix);

    // 将所有代理用户配置添加到过滤器配置中，供代理身份认证使用
    for (Map.Entry<String, String> entry : conf.getPropsWithPrefix(
        ProxyUsers.CONF_HADOOP_PROXYUSER).entrySet()) {
      filterConfig.put("proxyuser" + entry.getKey(), entry.getValue());
    }

    // 如果未配置认证类型，根据全局安全开关自动选择认证类型
    if (filterConfig.get("type") == null) {
      filterConfig.put("type", UserGroupInformation.isSecurityEnabled() ?
          KerberosAuthenticationHandler.TYPE :
          PseudoAuthenticationHandler.TYPE);
    }

    // 设置认证Cookie路径为根路径，覆盖整个Web服务
    filterConfig.put("cookie.path", "/");
    return filterConfig;
  }

  /**
   * 初始化过滤器，将AuthFilter注册到HTTP服务容器
   * @param container HTTP过滤器容器
   * @param conf Hadoop全局配置对象
   */
  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    Map<String, String> filterConfig = createFilterConfig(conf);
    container.addFilter("AuthFilter", AuthFilter.class.getName(),
        filterConfig);
  }

}