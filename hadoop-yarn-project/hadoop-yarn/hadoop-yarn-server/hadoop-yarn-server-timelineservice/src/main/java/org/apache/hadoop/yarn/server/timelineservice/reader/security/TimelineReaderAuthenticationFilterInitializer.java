// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain copy of the License at
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.yarn.server.timeline.security.TimelineAuthenticationFilterInitializer;

/**
 * 文件说明：时间线服务v2读取器Web服务的认证过滤器初始化器
 * 核心功能：为时间线读取器HTTP服务初始化Hadoop认证过滤器，加载时间线服务专属配置
 */
public class TimelineReaderAuthenticationFilterInitializer extends
    TimelineAuthenticationFilterInitializer{

  /**
   * 初始化时间线读取器HTTP认证过滤器
   * 把YARN配置中前缀为TIMELINE_HTTP_AUTH_PREFIX的配置传递给认证过滤器，并注册到全局过滤器容器
   * 
   * @param container 过滤器容器，用于注册全局过滤器
   * @param conf YARN运行时配置
   */
  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    // 加载并构建认证过滤器配置
    setAuthFilterConfig(conf);
    // 将认证过滤器注册到容器，用于时间线读取器HTTP请求认证
    container.addGlobalFilter("Timeline Reader Authentication Filter",
        AuthenticationFilter.class.getName(),
        getFilterConfig());
  }
}