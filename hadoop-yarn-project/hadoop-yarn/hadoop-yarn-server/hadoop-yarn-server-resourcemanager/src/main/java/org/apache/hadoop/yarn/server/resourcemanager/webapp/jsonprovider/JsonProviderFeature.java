// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider;

import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;

/**
 * JAX-RS功能实现类，为YARN RM Web服务注册自定义MOXy JSON提供者
 * 支持带/不带根元素的JSON序列化与反序列化，禁用MOXy自动发现，确保自定义提供者按优先级生效
 * 
 * <p>
 * 本功能禁用MOXy自动提供者发现，确保自定义提供者{@link IncludeRootJSONProvider} 和
 * {@link ExcludeRootJSONProvider} 按指定优先级显式使用。
 * </p>
 *
 * <p>配置详情:</p>
 * <ul>
 *   <li>注册{@link IncludeRootJSONProvider}，优先级为{@code 2001}。</li>
 *   <li>注册{@link ExcludeRootJSONProvider}，优先级为{@code 2002}。</li>
 * </ul>
 *
 * @see IncludeRootJSONProvider
 * @see ExcludeRootJSONProvider
 * @see org.glassfish.jersey.CommonProperties#MOXY_JSON_FEATURE_DISABLE
 */
public class JsonProviderFeature implements Feature {

  /**
   * 默认构造函数。
   */
  public JsonProviderFeature() {
  }

  /**
   * 配置JAX-RS功能，注册自定义JSON提供者
   *
   * @param context JAX-RS运行时提供的功能上下文
   * @return {@code true} 表示功能配置成功
   */
  @Override
  public boolean configure(FeatureContext context) {
    // 优先级用于维护JSON提供者之间的顺序，保证应用行为确定性
    context.register(IncludeRootJSONProvider.class, 2001);
    context.register(ExcludeRootJSONProvider.class, 2002);
    return true;
  }
}