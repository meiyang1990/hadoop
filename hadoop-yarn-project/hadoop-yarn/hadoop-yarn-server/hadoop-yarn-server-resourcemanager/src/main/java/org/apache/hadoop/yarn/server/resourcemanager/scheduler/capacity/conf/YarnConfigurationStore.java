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
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 容量调度器配置存储抽象基类，提供基于预写日志的配置持久化能力，
 * 支持配置变更的写入、确认和恢复流程。
 * 
 * 配置变更流程：调用方先调用{@code logMutation}记录待变更到持久化存储，
 * 仅当调用{@code confirmMutation}确认后，才会将变更合并到最终配置中。
 * 
 * 启动恢复流程：调用方先调用{@code retrieve}获取所有已确认配置，
 * 再调用{@code getPendingMutations}获取未确认的变更，最后通过{@code confirmMutation}重放确认。
 */
public abstract class YarnConfigurationStore implements AutoCloseable {

  public static final Logger LOG =
      LoggerFactory.getLogger(YarnConfigurationStore.class);

  /**
   * 封装配置变更日志所需的字段，用于审计和恢复。
   */
  public static class LogMutation implements Serializable {
    private static final long serialVersionUID = 7754046036718906356L;
    private Map<String, String> updates;
    private String user;

    /**
     * 创建配置变更日志实例。
     * @param updates 键值对形式的配置更新
     * @param user 发起配置变更的用户
     */
    LogMutation(Map<String, String> updates, String user) {
      this.updates = updates;
      this.user = user;
    }

    /**
     * 获取键值对形式的配置更新。
     * @return 配置更新映射
     */
    public Map<String, String> getUpdates() {
      return updates;
    }

    /**
     * 获取发起配置变更的用户。
     * @return 发起变更的用户名
     */
    public String getUser() {
      return user;
    }
  }

  /**
   * 初始化配置存储，如果存储已经存在，则使用存储中已有的配置忽略初始配置。
   * @param conf 用于初始化存储的配置
   * @param schedConf 待持久化的初始调度配置
   * @param rmContext ResourceManager上下文对象
   * @throws Exception 初始化失败抛出异常
   */
  public abstract void initialize(Configuration conf, Configuration schedConf,
      RMContext rmContext) throws Exception;

  /**
   * 关闭配置存储，释放相关资源。
   * @throws IOException 关闭失败抛出IO异常
   */
  public abstract void close() throws IOException;

  /**
   * 将配置变更写入后端存储的预写日志。
   * @param logMutation 需要预写的配置变更
   * @throws Exception 写入日志失败抛出异常
   */
  public abstract void logMutation(LogMutation logMutation) throws Exception;

  /**
   * 确认已记录的配置变更，在{@code logMutation}之后调用。
   * 将最后记录的待处理变更标记为完成，如果isValid为true则合并变更到持久化配置。
   * @param pendingMutation 需要确认的变更日志
   * @param isValid 是否有效，为true则将变更合并到持久化配置
   * @throws Exception 确认变更失败抛出异常
   */
  public abstract void confirmMutation(LogMutation pendingMutation,
      boolean isValid) throws Exception;

  /**
   * 获取持久化存储中的最终配置。
   * @return 键值对形式的配置对象
   * @throws IOException 获取配置失败抛出IO异常
   */
  public abstract Configuration retrieve() throws IOException;


  /**
   * 格式化持久化配置存储。
   * @throws Exception 格式化失败抛出异常
   */
  public abstract void format() throws Exception;

  /**
   * 获取最后一次更新的配置版本号。
   * @return 最后更新的配置版本号
   * @throws Exception 获取版本失败抛出异常
   */
  public abstract long getConfigVersion() throws Exception;

  /**
   * 获取从指定版本ID开始的所有已确认配置变更历史。
   * @param fromId 起始版本ID（包含）
   * @return 配置变更日志列表
   */
  public abstract List<LogMutation> getConfirmedConfHistory(long fromId);

  /**
   * 获取持久化存储的schema版本，用于兼容性检测。
   * @return 当前持久化存储使用的schema版本
   * @throws Exception 获取版本失败抛出异常
   */
  protected abstract Version getConfStoreVersion() throws Exception;

  /**
   * 获取所有配置变更日志列表。
   * @return 配置变更日志链表
   * @throws Exception 获取变更日志失败抛出异常
   */
  protected abstract LinkedList<LogMutation> getLogs() throws Exception;

  /**
   * 将当前硬编码的schema版本持久化存储到配置存储。
   * @throws Exception 存储版本失败抛出异常
   */
  protected abstract void storeVersion() throws Exception;

  /**
   * 获取硬编码的当前schema版本，用于和持久化版本对比。
   * @return 当前硬编码的schema版本
   */
  protected abstract Version getCurrentVersion();

  /**
   * 检查配置存储schema版本兼容性，如果不兼容则抛出异常。
   * @throws Exception 版本不兼容或检查失败抛出异常
   */
  public void checkVersion() throws Exception {
    // 读取存储中已有的schema版本
    Version loadedVersion = getConfStoreVersion();
    // 获取当前代码硬编码的schema版本
    Version currentVersion = getCurrentVersion();
    LOG.info("Loaded configuration store version info {}", loadedVersion);

    // 如果当前版本为null，跳过版本检查
    if (currentVersion == null || currentVersion.equals(loadedVersion)) {
      return;
    }
    // 如果无已存储版本或已存储版本兼容当前版本，存储当前版本号
    if (loadedVersion == null || loadedVersion.isCompatibleTo(currentVersion)) {
      LOG.info("Storing configuration store version info {}", currentVersion);
      storeVersion();
    } else {
      // 版本不兼容，抛出异常
      throw new YarnConfStoreVersionIncompatibleException(
          "Expecting configuration store version " + currentVersion
              + ", but loading version " + loadedVersion);
    }
  }

}