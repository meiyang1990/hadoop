// 这个文件已经全部加上中文注释
/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * 网络标签映射管理器工厂，根据配置创建并提供正确的NetworkTagMappingManager实现实例
 * 用于YARN NodeManager网络流量控制场景，支持自定义网络标签映射实现
 *
 */
public final class NetworkTagMappingManagerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(
      NetworkTagMappingManagerFactory.class);

  private NetworkTagMappingManagerFactory() {}

  /**
   * 根据Yarn配置创建并返回NetworkTagMappingManager实例
   * @param conf YARN配置对象
   * @return 配置指定的NetworkTagMappingManager实现实例，默认返回JSON格式实现
   */
  public static NetworkTagMappingManager getManager(Configuration conf) {
    // 从配置中读取管理器实现类，默认使用JSON文件实现
    Class<? extends NetworkTagMappingManager> managerClass =
        conf.getClass(YarnConfiguration.NM_NETWORK_TAG_MAPPING_MANAGER,
            NetworkTagMappingJsonManager.class,
            NetworkTagMappingManager.class);
    LOG.info("Using NetworkTagMappingManager implementation - "
        + managerClass);
    // 通过反射实例化管理器对象并返回
    return ReflectionUtils.newInstance(managerClass, conf);
  }
}