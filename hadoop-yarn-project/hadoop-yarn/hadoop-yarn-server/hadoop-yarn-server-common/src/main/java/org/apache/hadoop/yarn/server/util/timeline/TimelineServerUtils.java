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
 * Unless required by applicable law or agreed to writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.util.timeline;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.security.authentication.server.ProxyUserAuthenticationFilterInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.yarn.server.timeline.security.TimelineAuthenticationFilter;
import org.apache.hadoop.yarn.server.timeline.security.TimelineAuthenticationFilterInitializer;
import org.apache.hadoop.yarn.server.timeline.security.TimelineDelgationTokenSecretManagerService;

/**
 * 时间线服务工具类，提供时间线读取器和收集器通用工具方法
 */
public final class TimelineServerUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineServerUtils.class);

  private TimelineServerUtils() {
  }

  /**
   * 基于已有配置和时间线服务默认过滤器，设置过滤器初始化器配置
   * @param conf 配置对象
   * @param configuredInitializers 用户配置的逗号分隔过滤器初始化器列表
   * @param defaultInitializers 时间线服务默认添加的过滤器初始化器集合
   */
  public static void setTimelineFilters(Configuration conf,
      String configuredInitializers, Set<String> defaultInitializers) {

    // 需要忽略的通用过滤器初始化器集合
    Set<String> ignoreInitializers = new LinkedHashSet<>();
    ignoreInitializers.add(AuthenticationFilterInitializer.class.getName());
    ignoreInitializers.add(
        ProxyUserAuthenticationFilterInitializer.class.getName());

    // 分割用户配置的初始化器列表
    String[] parts = configuredInitializers.split(",");
    Set<String> target = new LinkedHashSet<String>();
    for (String filterInitializer : parts) {
      filterInitializer = filterInitializer.trim();
      // 跳过需要忽略的和空的初始化器
      if (ignoreInitializers.contains(filterInitializer) ||
          filterInitializer.isEmpty()) {
        continue;
      }
      target.add(filterInitializer);
    }
    // 合并添加默认初始化器
    target.addAll(defaultInitializers);
    // 拼接为逗号分隔的字符串
    String actualInitializers =
        org.apache.commons.lang3.StringUtils.join(target, ",");
    LOG.info("Filter initializers set for timeline service: " +
        actualInitializers);
    // 将最终配置写入配置对象
    conf.set("hadoop.http.filter.initializers", actualInitializers);
  }

  /**
   * 添加时间线认证过滤器到默认过滤器初始化器集合，并为过滤器注入令牌管理器
   * @param initializers 用户配置的逗号分隔过滤器初始化器列表
   * @param defaultInitializers 时间线服务默认的过滤器初始化器集合
   * @param delegationTokenMgrService 委托令牌管理器服务，供认证过滤器颁发令牌使用
   */
  public static void addTimelineAuthFilter(String initializers,
      Set<String> defaultInitializers,
      TimelineDelgationTokenSecretManagerService delegationTokenMgrService) {
    // 为认证过滤器设置委托令牌管理器实例
    TimelineAuthenticationFilter.setTimelineDelegationTokenSecretManager(
        delegationTokenMgrService.getTimelineDelegationTokenSecretManager());
    // 如果用户配置中未添加该初始化器，则添加到默认集合中
    if (!initializers.contains(
        TimelineAuthenticationFilterInitializer.class.getName())) {
      defaultInitializers.add(
          TimelineAuthenticationFilterInitializer.class.getName());
    }
  }
}