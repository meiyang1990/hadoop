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
 * 时间线服务跨域资源共享过滤器初始化器
 * 继承Hadoop通用跨域初始化器，提供YARN时间线服务专属的跨域访问过滤器配置
 * 允许前端跨域访问时间线服务REST API，支持时间线服务独立配置覆盖全局配置
 */
package org.apache.hadoop.yarn.server.timeline.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.security.http.CrossOriginFilter;

import java.util.Map;

public class CrossOriginFilterInitializer extends HttpCrossOriginFilterInitializer {

  /** 时间线服务跨域配置项前缀，优先级高于全局Hadoop跨域配置 */
  public static final String PREFIX =
      "yarn.timeline-service.http-cross-origin.";

  @Override
  protected String getPrefix() {
    return PREFIX;
  }

  @Override
  /**
   * 初始化跨域过滤器，合并全局和时间线服务专属配置
   * @param container 过滤器容器，用于注册过滤器
   * @param conf Hadoop配置对象
   */
  public void initFilter(FilterContainer container, Configuration conf) {

    // setup the filter
    // use the keys with "yarn.timeline-service.http-cross-origin" prefix to
    // override the ones with the "hadoop.http.cross-origin" prefix.

    // 加载全局Hadoop跨域配置作为默认值
    Map<String, String> filterParameters =
        getFilterParameters(conf, HttpCrossOriginFilterInitializer.PREFIX);
    // 加载时间线服务专属配置，覆盖全局默认配置
    filterParameters.putAll(getFilterParameters(conf, getPrefix()));

    // 向容器注册全局跨域过滤器
    container.addGlobalFilter("Cross Origin Filter",
          CrossOriginFilter.class.getName(), filterParameters);
  }
}