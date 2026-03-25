// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.mapreduce.util;

import java.util.Locale;
import java.util.ResourceBundle;
import java.util.MissingResourceException;

/**
 * 文件: ResourceBundles.java
 * 位于: hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core
 * 功能: MapReduce模块国际化资源绑定工具类，提供更可靠的资源包加载与查找能力，
 * 主要用于支持计数器名称等文本的国际化多语言显示
 */

/**
 * 资源包处理工具类，提供更稳定的资源绑定加载与查找能力，解决Java原生ResourceBundle使用中的常见问题
 */
public class ResourceBundles {

  /**
   * 加载指定名称的资源包，使用当前线程上下文类加载器和默认区域设置
   * @param bundleName 要加载的资源包名称
   * @return 加载完成的资源包实例
   * @throws MissingResourceException 当资源包不存在时抛出异常
   */
  public static ResourceBundle getBundle(String bundleName) {
    return ResourceBundle.getBundle(bundleName.replace('$', '_'),
        Locale.getDefault(), Thread.currentThread().getContextClassLoader());
  }

  /**
   * 根据资源包名、键和后缀查找资源，找不到则返回默认值
   * @param <T> 资源的类型
   * @param bundleName 资源包名称
   * @param key 资源查找键
   * @param suffix 查找键后缀
   * @param defaultValue 查找失败时返回的默认值
   * @return 查找到的资源，或默认值
   * @throws ClassCastException 当找到的资源类型与T不匹配时抛出异常
   */
  @SuppressWarnings("unchecked")
  public static synchronized <T> T getValue(String bundleName, String key,
                                            String suffix, T defaultValue) {
    T value;
    try {
      // 加载指定资源包
      ResourceBundle bundle = getBundle(bundleName);
      // 拼接完整查找键并获取资源
      value = (T) bundle.getObject(getLookupKey(key, suffix));
    }
    catch (Exception e) {
      // 任何异常都返回默认值
      return defaultValue;
    }
    return value;
  }

  /**
   * 拼接生成完整的资源查找键，若后缀为空则返回原键
   * @param key 基础键
   * @param suffix 后缀
   * @return 拼接后的完整查找键
   */
  private static String getLookupKey(String key, String suffix) {
    if (suffix == null || suffix.isEmpty()) return key;
    return key + suffix;
  }

  /**
   * 获取计数器分组的国际化显示名称，找不到则返回默认值
   * @param group 计数器分组名称
   * @param defaultValue 查找失败时返回的默认名称
   * @return 国际化后的计数器分组显示名称
   */
  public static String getCounterGroupName(String group, String defaultValue) {
    return getValue(group, "CounterGroupName", "", defaultValue);
  }

  /**
   * 获取计数器的国际化显示名称，找不到则返回默认值
   * @param group 计数器所属分组名称
   * @param counter 计数器名称
   * @param defaultValue 查找失败时返回的默认名称
   * @return 国际化后的计数器显示名称
   */
  public static String getCounterName(String group, String counter,
                                      String defaultValue) {
    return getValue(group, counter, ".name", defaultValue);
  }
}