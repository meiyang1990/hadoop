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
package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * RM状态存储工厂，负责根据配置创建对应类型的RM状态存储实例，
 * 支持不同的存储实现（内存、ZK、文件等），为ResourceManager恢复提供状态存储。
 */
public class RMStateStoreFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(RMStateStoreFactory.class);
  
  /**
   * 根据YARN配置创建对应的RM状态存储实例
   * @param conf YARN配置对象
   * @return 配置指定类型的RM状态存储实例，默认返回内存实现
   */
  public static RMStateStore getStore(Configuration conf) {
    Class<? extends RMStateStore> storeClass =
        conf.getClass(YarnConfiguration.RM_STORE,
            MemoryRMStateStore.class, RMStateStore.class);
    LOG.info("Using RMStateStore implementation - " + storeClass);
    return ReflectionUtils.newInstance(storeClass, conf);
  }
}