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

package org.apache.hadoop.yarn.server.timelineservice.reader.security;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * 文件说明：ATSv2时间线读取服务白名单授权过滤器初始化器
 * 核心职责：负责加载时间线服务配置，初始化{@link TimelineReaderWhitelistAuthorizationFilter}，并注册到HTTP过滤器容器
 */
public class TimelineReaderWhitelistAuthorizationFilterInitializer
    extends FilterInitializer {

  /**
   * 初始化白名单授权过滤器，读取配置参数并注册到过滤器容器
   *
   * @param container HTTP过滤器容器，用于注册过滤器
   * @param conf YARN配置对象，用于读取服务配置
   */
  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    // 存储过滤器初始化参数
    Map<String, String> params = new HashMap<String, String>();
    // 读取并设置读取权限认证开关配置
    String isWhitelistReadAuthEnabled = Boolean.toString(
        conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_READ_AUTH_ENABLED,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_READ_AUTH_ENABLED));
    params.put(YarnConfiguration.TIMELINE_SERVICE_READ_AUTH_ENABLED,
        isWhitelistReadAuthEnabled);
    // 读取并设置允许访问的用户白名单配置
    params.put(YarnConfiguration.TIMELINE_SERVICE_READ_ALLOWED_USERS,
        conf.get(YarnConfiguration.TIMELINE_SERVICE_READ_ALLOWED_USERS,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_READ_ALLOWED_USERS));

    // 读取并设置YARN管理员ACL配置，未配置时默认不开放所有人访问，使用空字符串
    params.put(YarnConfiguration.YARN_ADMIN_ACL,
        conf.get(YarnConfiguration.YARN_ADMIN_ACL,
            // using a default of ""
            // instead of DEFAULT_YARN_ADMIN_ACL
            // The reason being, DEFAULT_YARN_ADMIN_ACL is set to all users
            // and we do not wish to allow everyone by default if
            // read auth is enabled and YARN_ADMIN_ACL is unset
            TimelineReaderWhitelistAuthorizationFilter.EMPTY_STRING));
    // 将白名单授权过滤器注册为全局过滤器，生效于所有请求
    container.addGlobalFilter("Timeline Reader Whitelist Authorization Filter",
        TimelineReaderWhitelistAuthorizationFilter.class.getName(), params);
  }
}