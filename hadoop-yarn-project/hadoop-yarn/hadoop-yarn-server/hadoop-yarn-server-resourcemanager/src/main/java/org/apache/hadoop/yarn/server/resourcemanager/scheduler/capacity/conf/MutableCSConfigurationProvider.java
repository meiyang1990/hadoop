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

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ConfigurationMutationACLPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ConfigurationMutationACLPolicyFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore.LogMutation;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 容量调度器可修改配置提供者，实现了{@link MutableConfigurationProvider}接口，
 * 支持在线动态修改容量调度器的队列配置。
 */
public class MutableCSConfigurationProvider implements CSConfigurationProvider,
    MutableConfigurationProvider {

  public static final Logger LOG =
      LoggerFactory.getLogger(MutableCSConfigurationProvider.class);

  // 当前内存中的调度器配置
  private Configuration schedConf;
  // 修改前的旧配置，用于回滚
  private Configuration oldConf;
  // 配置持久化存储实现
  private YarnConfigurationStore confStore;
  // 配置修改访问控制策略
  private ConfigurationMutationACLPolicy aclMutationPolicy;
  // RM上下文对象
  private RMContext rmContext;

  // 格式化/重新加载配置的读写锁，保障并发安全
  private final ReentrantReadWriteLock formatLock =
      new ReentrantReadWriteLock();

  /**
   * 构造函数，初始化配置提供者，绑定RM上下文。
   * @param rmContext RM上下文
   */
  public MutableCSConfigurationProvider(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  /**
   * 获取初始调度器配置，从文件系统加载 capacity-scheduler.xml。
   * 单元测试可覆盖此方法注入测试配置。
   * @return 初始调度器配置对象
   */
  // Unit test can overwrite this method
  protected Configuration getInitSchedulerConfig() {
    Configuration initialSchedConf = new Configuration(false);
    initialSchedConf.
        addResource(YarnConfiguration.CS_CONFIGURATION_FILE);
    return initialSchedConf;
  }

  @Override
  public void init(Configuration config) throws IOException {
    // 创建配置存储实例
    this.confStore = YarnConfigurationStoreFactory.getStore(config);
    // 初始化调度器配置
    initializeSchedConf();
    try {
      // 初始化配置存储
      confStore.initialize(config, schedConf, rmContext);
      // 检查配置存储版本兼容性
      confStore.checkVersion();
    } catch (Exception e) {
      throw new IOException(e);
    }
    // 配置存储中已有持久化配置，使用存储中的配置覆盖初始文件配置
    schedConf = confStore.retrieve();
    // 初始化配置修改ACL策略
    this.aclMutationPolicy = ConfigurationMutationACLPolicyFactory
        .getPolicy(config);
    aclMutationPolicy.init(config, rmContext);
  }

  @Override
  public void close() throws IOException {
    confStore.close();
  }

  @VisibleForTesting
  protected YarnConfigurationStore getConfStore() {
    return confStore;
  }

  @Override
  public CapacitySchedulerConfiguration loadConfiguration(Configuration
      configuration) throws IOException {
    // 合并传入的全局配置与当前调度器配置
    Configuration loadedConf = new Configuration(schedConf);
    loadedConf.addResource(configuration);
    // 包装为容量调度器配置对象返回
    return new CapacitySchedulerConfiguration(loadedConf, false);
  }

  @Override
  public Configuration getConfiguration() {
    // 返回当前配置的副本，避免外部修改内部状态
    return new Configuration(schedConf);
  }

  @Override
  public long getConfigVersion() throws Exception {
    return confStore.getConfigVersion();
  }

  @Override
  public ConfigurationMutationACLPolicy getAclMutationPolicy() {
    return aclMutationPolicy;
  }

  @Override
  public LogMutation logAndApplyMutation(UserGroupInformation user,
      SchedConfUpdateInfo confUpdate) throws Exception {
    // 保存修改前配置用于回滚
    oldConf = new Configuration(schedConf);
    // 基于当前配置创建待修改配置对象
    CapacitySchedulerConfiguration proposedConf =
            new CapacitySchedulerConfiguration(schedConf, false);
    // 将更新请求转换为键值对格式的配置变更
    Map<String, String> kvUpdate
            = ConfigurationUpdateAssembler.constructKeyValueConfUpdate(proposedConf, confUpdate);
    // 创建变更日志记录
    LogMutation log = new LogMutation(kvUpdate, user.getShortUserName());
    // 将变更记录持久化到存储
    confStore.logMutation(log);
    // 应用变更到内存配置
    applyMutation(proposedConf, kvUpdate);
    // 更新内存配置为修改后的配置
    schedConf = proposedConf;
    return log;
  }

  /**
   * 在给定配置上应用变更，不修改当前提供者的内存配置，仅用于预览变更结果。
   * @param oldConfiguration 原始配置
   * @param confUpdate 配置更新信息
   * @return 应用变更后的配置对象
   * @throws IOException 应用配置变更失败时抛出
   */
  public Configuration applyChanges(Configuration oldConfiguration,
                           SchedConfUpdateInfo confUpdate) throws IOException {
    CapacitySchedulerConfiguration proposedConf =
            new CapacitySchedulerConfiguration(oldConfiguration, false);
    Map<String, String> kvUpdate
            = ConfigurationUpdateAssembler.constructKeyValueConfUpdate(proposedConf, confUpdate);
    applyMutation(proposedConf, kvUpdate);
    return proposedConf;
  }

  /**
   * 批量应用键值对配置变更到指定配置对象。
   * @param conf 目标配置对象
   * @param kvUpdate 键值对变更集合，值为null则删除对应配置
   */
  private void applyMutation(Configuration conf, Map<String, String> kvUpdate) {
    for (Map.Entry<String, String> kv : kvUpdate.entrySet()) {
      if (kv.getValue() == null) {
        conf.unset(kv.getKey());
      } else {
        conf.set(kv.getKey(), kv.getValue());
      }
    }
  }

  @Override
  public void formatConfigurationInStore(Configuration config)
      throws Exception {
    // 获取格式化写锁，阻塞并发操作
    formatLock.writeLock().lock();
    try {
      // 清空配置存储
      confStore.format();
      // 保存当前配置用于回滚
      oldConf = new Configuration(schedConf);
      // 重新从初始文件加载配置
      initializeSchedConf();
      // 重新初始化配置存储
      confStore.initialize(config, schedConf, rmContext);
      // 检查版本兼容性
      confStore.checkVersion();
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      // 释放写锁
      formatLock.writeLock().unlock();
    }
  }

  /**
   * 从初始配置文件加载调度器配置，将所有配置键值显式存入当前配置对象，
   * 保障后续配置重载时可以正确删除已有配置项。
   */
  private void initializeSchedConf() {
    Configuration initialSchedConf = getInitSchedulerConfig();
    this.schedConf = new Configuration(false);
    // We need to explicitly set the key-values in schedConf, otherwise
    // these configuration keys cannot be deleted when
    // configuration is reloaded.
    for (Map.Entry<String, String> kv : initialSchedConf) {
      schedConf.set(kv.getKey(), kv.getValue());
    }
  }

  @Override
  public void revertToOldConfig(Configuration config) throws Exception {
    // 获取回滚写锁，阻塞并发操作
    formatLock.writeLock().lock();
    try {
      // 恢复内存配置为修改前的旧配置
      schedConf = oldConf;
      // 清空配置存储
      confStore.format();
      // 将旧配置重新持久化到存储
      confStore.initialize(config, oldConf, rmContext);
      // 检查版本兼容性
      confStore.checkVersion();
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      // 释放写锁
      formatLock.writeLock().unlock();
    }
  }

  @Override
  public void confirmPendingMutation(LogMutation pendingMutation,
      boolean isValid) throws Exception {
    // 获取确认读锁
    formatLock.readLock().lock();
    try {
      // 确认变更生效/失败
      confStore.confirmMutation(pendingMutation, isValid);
      // 如果变更无效，回滚内存配置
      if (!isValid) {
        schedConf = oldConf;
      }
    } finally {
      // 释放读锁
      formatLock.readLock().unlock();
    }
  }

  @Override
  public void reloadConfigurationFromStore() throws Exception {
    // 获取重新加载读锁
    formatLock.readLock().lock();
    try {
      // 从持久化存储重新加载配置到内存
      schedConf = confStore.retrieve();
    } finally {
      // 释放读锁
      formatLock.readLock().unlock();
    }
  }
}