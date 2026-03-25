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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 文件级注释：容量调度器配置存储的内存实现，YarnConfigurationStore接口的默认实现
 * 仅在内存中存储调度配置，不提供持久化存储能力，用于不需要持久化配置变更的场景
 *
 * A default implementation of {@link YarnConfigurationStore}. Doesn't offer
 * persistent configuration storage, just stores the configuration in memory.
 */
public class InMemoryConfigurationStore extends YarnConfigurationStore {

  // 内存存储的调度配置对象
  private Configuration schedConf;
  // 当前配置版本号
  private long configVersion;

  @Override
  /**
   * 初始化内存配置存储，加载初始调度配置并设置起始版本
   */
  public void initialize(Configuration conf, Configuration schedConf,
      RMContext rmContext) {
    this.schedConf = schedConf;
    this.configVersion = 1L;
  }

  /**
   * This method does not log as it does not support backing store.
   * The mutation to be applied on top of schedConf will be directly passed
   * in confirmMutation.
   */
  @Override
  /**
   * 记录配置变更日志，内存存储不支持持久化，此方法为空实现
   */
  public void logMutation(LogMutation logMutation) {
  }

  @Override
  /**
   * 确认并应用配置变更，更新内存中的配置
   */
  public void confirmMutation(LogMutation pendingMutation, boolean isValid) {
    // 如果变更有效，应用变更
    if (isValid) {
      // 遍历所有配置变更项
      for (Map.Entry<String, String> kv : pendingMutation.getUpdates()
          .entrySet()) {
        // 值为null则删除对应配置项
        if (kv.getValue() == null) {
          schedConf.unset(kv.getKey());
        } else {
          // 否则设置新的配置值
          schedConf.set(kv.getKey(), kv.getValue());
        }
      }
      // 配置版本号自增
      this.configVersion = this.configVersion + 1L;
    }
  }

  @Override
  /**
   * 格式化清空内存配置存储
   */
  public void format() {
    this.schedConf = null;
  }

  @Override
  /**
   * 从内存中获取当前调度配置
   */
  public synchronized Configuration retrieve() {
    return schedConf;
  }

  @Override
  /**
   * 获取当前配置版本号
   */
  public long getConfigVersion() {
    return configVersion;
  }

  /**
   * Configuration mutations not logged (i.e. not persisted) but directly
   * confirmed. As such, a list of persisted configuration mutations does not
   * exist.
   * @return null Configuration mutation list not applicable for this store.
   */
  @Override
  /**
   * 获取指定版本之后的已确认配置变更历史，内存存储不支持，返回null
   */
  public List<LogMutation> getConfirmedConfHistory(long fromId) {
    // Unimplemented.
    return null;
  }

  /**
   * Configuration mutations not logged (i.e. not persisted) but directly
   * confirmed. As such, a list of persisted configuration mutations does not
   * exist.
   * @return null Configuration mutation list not applicable for this store.
   */
  @Override
  /**
   * 获取所有变更日志，内存存储不支持持久化日志，返回null
   */
  protected LinkedList<LogMutation> getLogs() {
    // Unimplemented.
    return null;
  }

  /**
   * Configuration mutations applied directly in-memory. As such, there is no
   * persistent configuration store.
   * As there is no configuration store for versioning purposes,
   * a conf store version is not applicable.
   * @return null Conf store version not applicable for this store.
   * @throws Exception if any exception occurs during getConfStoreVersion.
   */
  @Override
  /**
   * 获取配置存储的版本，内存存储不支持版本持久化，返回null
   * @throws Exception 不会抛出异常
   */
  public Version getConfStoreVersion() throws Exception {
    // Does nothing.
    return null;
  }

  /**
   * Configuration mutations not logged (i.e. not persisted). As such, they are
   * not persisted and not versioned. Hence, no version information to store.
   * @throws Exception if any exception occurs during store Version.
   */
  @Override
  /**
   * 存储配置存储版本，内存存储不需要持久化版本，空实现
   * @throws Exception 不会抛出异常
   */
  public void storeVersion() throws Exception {
    // Does nothing.
  }

  /**
   * Configuration mutations not logged (i.e. not persisted). As such, they are
   * not persisted and not versioned. Hence, a current version is not
   * applicable.
   * @return null A current version not applicable for this store.
   */
  @Override
  /**
   * 获取配置存储当前版本，内存存储不支持，返回null
   */
  public Version getCurrentVersion() {
    // Does nothing.
    return null;
  }

  /**
   * Configuration mutations not logged (i.e. not persisted). As such, they are
   * not persisted and not versioned. Hence, version is always compatible,
   * since it is in-memory.
   */
  @Override
  /**
   * 检查配置存储版本兼容性，内存存储总是兼容，空实现
   */
  public void checkVersion() {
    // Does nothing. (Version is always compatible since it's in memory)
  }

  @Override
  /**
   * 关闭内存配置存储，释放资源，空实现
   * @throws IOException 不会抛出IO异常
   */
  public void close() throws IOException {
    // Does nothing.
  }
}