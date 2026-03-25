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

package org.apache.hadoop.yarn.server.resourcemanager.resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YARNFeatureNotEnabledException;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.Map;

/**
 * 资源配置文件管理器接口，为YARN资源调度提供资源模板管理能力，
 * 定义了获取、初始化、重载资源配置文件的统一接口规范。
 * 资源配置文件用于预定义不同规格的资源模板，方便应用快速申请指定规格的资源。
 */
public interface ResourceProfilesManager {

  /**
   * 初始化资源配置文件管理器，从配置加载所有资源配置信息。
   * @param config YARN配置对象
   * @throws IOException 加载到无效资源配置名称时抛出
   */
  void init(Configuration config) throws IOException;

  /**
   * 根据配置名称获取对应的资源能力信息。
   * @param profile 资源配置名称
   * @return 对应配置的资源能力信息
   *
   * @throws YarnException 配置名称无效或资源配置功能未开启时抛出
   */
  Resource getProfile(String profile) throws YarnException;

  /**
   * 获取所有支持的资源配置及其对应的资源信息。
   * @return 资源配置名称到资源信息的映射表
   *
   * @throws YARNFeatureNotEnabledException 资源配置功能未开启时抛出
   */
  Map<String, Resource> getResourceProfiles() throws
      YARNFeatureNotEnabledException;

  /**
   * 根据更新后的配置重新加载资源配置信息。
   * @throws IOException 加载到无效资源配置名称时抛出
   */
  void reloadProfiles() throws IOException;

  /**
   * 获取默认资源配置对应的资源信息。
   * @return 默认资源配置的资源对象
   * @throws YarnException 配置名称无效或资源配置功能未开启时抛出
   */
  Resource getDefaultProfile() throws YarnException;

  /**
   * 获取最小资源配置对应的资源信息。
   * @return 最小资源配置的资源对象
   * @throws YarnException 配置名称无效或资源配置功能未开启时抛出
   */
  Resource getMinimumProfile() throws YarnException;

  /**
   * 获取最大资源配置对应的资源信息。
   * @return 最大资源配置的资源对象
   * @throws YarnException 配置名称无效或资源配置功能未开启时抛出
   */
  Resource getMaximumProfile() throws YarnException;
}