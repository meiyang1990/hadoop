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

package org.apache.hadoop.mapreduce.v2.app.webapp.jsonprovider;

import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;

import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.internal.InternalProperties;

/**
 * 自定义JSON序列化功能类，用于MapReduce应用Web服务的JSON响应处理。
 * 禁用Jersey默认的JSON序列化功能，注册自定义的JSON提供器实现定制化序列化。
 */
public class JsonProviderFeature implements Feature {
  @Override
  public boolean configure(FeatureContext context) {
    // 禁用Jersey自动发现的MOXy JSON功能，确保使用自定义提供器
    context.property(CommonProperties.MOXY_JSON_FEATURE_DISABLE, true);
    // 设置当前自定义JSON功能标识
    context.property(InternalProperties.JSON_FEATURE, "JsonProviderFeature");
    // 注册带根元素包裹的自定义JSON提供器，设置优先级
    context.register(IncludeRootJSONProvider.class, 2001);
    // 注册不带根元素包裹的自定义JSON提供器，设置优先级
    context.register(ExcludeRootJSONProvider.class, 2002);
    return true;
  }
}