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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * YARN容量调度器配置存储工厂类，用于创建不同实现的YarnConfigurationStore实例。
 * 支持多种配置存储后端，通过配置选择具体实现。
 */
public final class YarnConfigurationStoreFactory {

  private static final Logger LOG = LoggerFactory.getLogger(
      YarnConfigurationStoreFactory.class);

  private YarnConfigurationStoreFactory() {
    // 工具类不允许实例化
  }

  /**
   * 根据配置创建并返回对应的配置存储实例。
   * @param conf YARN配置对象
   * @return 配置存储实例
   */
  public static YarnConfigurationStore getStore(Configuration conf) {
    // 从配置中读取存储实现类名称，默认使用内存存储
    String store = conf.get(
        YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);
    // 根据存储类型创建对应实例
    switch (store) {
      case YarnConfiguration.MEMORY_CONFIGURATION_STORE:
        return new InMemoryConfigurationStore();
      case YarnConfiguration.LEVELDB_CONFIGURATION_STORE:
        return new LeveldbConfigurationStore();
      case YarnConfiguration.ZK_CONFIGURATION_STORE:
        return new ZKConfigurationStore();
      case YarnConfiguration.FS_CONFIGURATION_STORE:
        return new FSSchedulerConfigurationStore();
      default:
        // 自定义实现：通过反射加载用户指定的配置存储类
        Class<? extends YarnConfigurationStore> storeClass =
            conf.getClass(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
            InMemoryConfigurationStore.class, YarnConfigurationStore.class);
        LOG.info("Using YarnConfigurationStore implementation - " + storeClass);
        return ReflectionUtils.newInstance(storeClass, conf);
    }
  }
}