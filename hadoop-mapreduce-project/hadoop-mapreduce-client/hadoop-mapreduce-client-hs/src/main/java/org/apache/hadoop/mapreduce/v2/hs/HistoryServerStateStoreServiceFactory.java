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
package org.apache.hadoop.mapreduce.v2.hs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 历史服务器状态存储服务工厂类
 * 负责根据配置创建不同类型的历史服务器状态存储实例，支持恢复功能开启/关闭场景
 */
public class HistoryServerStateStoreServiceFactory {

  /**
   * 根据配置创建对应的历史服务器状态存储服务实例
   * 若未开启恢复功能，默认返回空存储实现；若开启则从配置中加载指定存储类
   * 
   * @param conf Hadoop配置对象，包含历史服务器相关配置项
   * @return 初始化完成的历史服务器状态存储服务实例
   */
  public static HistoryServerStateStoreService getStore(Configuration conf) {
    Class<? extends HistoryServerStateStoreService> storeClass =
        HistoryServerNullStateStoreService.class;
    // 从配置读取是否开启历史服务器恢复功能
    boolean recoveryEnabled = conf.getBoolean(
        JHAdminConfig.MR_HS_RECOVERY_ENABLE,
        JHAdminConfig.DEFAULT_MR_HS_RECOVERY_ENABLE);
    // 若开启恢复功能，加载配置指定的存储类
    if (recoveryEnabled) {
      storeClass = conf.getClass(JHAdminConfig.MR_HS_STATE_STORE, null,
          HistoryServerStateStoreService.class);
      // 配置中未指定存储类时抛出异常提示用户检查配置
      if (storeClass == null) {
        throw new RuntimeException("Unable to locate storage class, check "
            + JHAdminConfig.MR_HS_STATE_STORE);
      }
    }
    // 通过反射创建存储实例并完成初始化
    return ReflectionUtils.newInstance(storeClass, conf);
  }
}