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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredAMRMProxyState;
import org.apache.hadoop.yarn.server.security.MasterKeyData;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件说明：AMRMProxyTokenSecretManager 用于AMRMProxyService生成和管理AMRM令牌的密钥管理器，支持主密钥轮转和NM状态恢复
 * 
 * This secret manager instance is used by the AMRMProxyService to generate and
 * manage tokens.
 */
public class AMRMProxyTokenSecretManager extends
    SecretManager<AMRMTokenIdentifier> {

  private static final Logger LOG =
       LoggerFactory.getLogger(AMRMProxyTokenSecretManager.class);

  // 序列号，用于生成新主密钥ID
  private int serialNo = new SecureRandom().nextInt();
  // 下一个即将激活的主密钥
  private MasterKeyData nextMasterKey;
  // 当前生效的主密钥
  private MasterKeyData currentMasterKey;

  // 读写锁，保证主密钥操作的线程安全
  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();

  // 定时器，用于执行主密钥轮转和激活任务
  private final Timer timer;
  // 主密钥轮转间隔，单位毫秒
  private long rollingInterval;
  // 新密钥激活延迟，单位毫秒，确保所有活跃AM获取到新密钥
  private long activationDelay;

  // NM状态存储服务，用于持久化主密钥信息
  private NMStateStoreService nmStateStore;

  // 保存当前本节点上运行的应用尝试ID集合
  private final Set<ApplicationAttemptId> appAttemptSet = new HashSet<>();

  /**
   * 构造AMRMProxy令牌密钥管理器
   * Create an {@link AMRMProxyTokenSecretManager}.
   * @param nmStateStoreService NM状态存储服务
   */
  public AMRMProxyTokenSecretManager(NMStateStoreService nmStateStoreService) {
    this.timer = new Timer();
    this.nmStateStore = nmStateStoreService;
  }

  /**
   * 初始化配置，读取密钥轮转间隔和激活延迟配置
   * @param conf 配置对象
   */
  public void init(Configuration conf) {
    this.rollingInterval =
        conf.getLong(
            YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
            YarnConfiguration.DEFAULT_RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS) * 1000;
    // Adding delay = 1.5 * expiry interval makes sure that all active AMs get
    // the updated shared-key.
    String rmAmExpiryIntervalMS = conf.get(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS);
    // 检查配置是否为纯数字格式
    if (NumberUtils.isDigits(rmAmExpiryIntervalMS)) {
      this.activationDelay = (long) (conf.getLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS) * 1.5);
    } else {
      // 解析带时间单位的配置
      this.activationDelay = (long) (conf.getTimeDuration(
          YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS, TimeUnit.MILLISECONDS) * 1.5);
    }
    LOG.info("AMRMTokenKeyRollingInterval: {} ms and AMRMTokenKeyActivationDelay: {} ms.",
        this.rollingInterval, this.activationDelay);
    // 校验配置：轮转间隔必须大于3倍过期间隔，否则抛出异常
    if (rollingInterval <= activationDelay * 2) {
      throw new IllegalArgumentException(
          YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS
              + " should be more than 3 X "
              + YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS);
    }
  }

  /**
   * 启动密钥管理器，初始化当前主密钥并启动定时轮转任务
   */
  public void start() {
    if (this.currentMasterKey == null) {
      this.currentMasterKey = createNewMasterKey();
      if (this.nmStateStore != null) {
        try {
          // 持久化当前主密钥到状态存储
          this.nmStateStore.storeAMRMProxyCurrentMasterKey(
              this.currentMasterKey.getMasterKey());
        } catch (IOException e) {
          LOG.error("Unable to update current master key in state store", e);
        }
      }
    }
    // 启动固定间隔的主密钥轮转定时任务
    this.timer.scheduleAtFixedRate(new MasterKeyRoller(), rollingInterval,
        rollingInterval);
  }

  /**
   * 停止密钥管理器，取消定时任务
   */
  public void stop() {
    this.timer.cancel();
  }

  @VisibleForTesting
  public void setNMStateStoreService(NMStateStoreService nmStateStoreService) {
    this.nmStateStore = nmStateStoreService;
  }

  /**
   * 应用尝试完成后，从集合中移除对应记录，清理令牌信息
   * @param appAttemptId 完成的应用尝试ID
   */
  public void applicationMasterFinished(ApplicationAttemptId appAttemptId) {
    this.writeLock.lock();
    try {
      LOG.info("Application finished, removing password for "
          + appAttemptId);
      this.appAttemptSet.remove(appAttemptId);
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * 主密钥轮转定时任务
   */
  private class MasterKeyRoller extends TimerTask {
    @Override
    public void run() {
      rollMasterKey();
    }
  }

  /**
   * 执行主密钥轮转，生成新的下一个主密钥，延迟后激活
   */
  @Private
  @VisibleForTesting
  public void rollMasterKey() {
    this.writeLock.lock();
    try {
      LOG.info("Rolling master-key for amrm-tokens");
      this.nextMasterKey = createNewMasterKey();
      if (this.nmStateStore != null) {
        try {
          // 持久化下一个主密钥到状态存储
          this.nmStateStore
              .storeAMRMProxyNextMasterKey(this.nextMasterKey.getMasterKey());
        } catch (IOException e) {
          LOG.error("Unable to update next master key in state store", e);
        }
      }

      // 延迟激活新密钥，确保所有活跃AM获取到更新
      this.timer.schedule(new NextKeyActivator(), this.activationDelay);
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * 新密钥激活定时任务
   */
  private class NextKeyActivator extends TimerTask {
    @Override
    public void run() {
      activateNextMasterKey();
    }
  }

  /**
   * 将预先生成的下一个主密钥切换为当前生效主密钥
   */
  @Private
  @VisibleForTesting
  public void activateNextMasterKey() {
    this.writeLock.lock();
    try {
      LOG.info("Activating next master key with id: "
          + this.nextMasterKey.getMasterKey().getKeyId());
      // 切换当前主密钥为下一个密钥
      this.currentMasterKey = this.nextMasterKey;
      this.nextMasterKey = null;
      if (this.nmStateStore != null) {
        try {
          // 更新状态存储中的当前和下一个主密钥
          this.nmStateStore.storeAMRMProxyCurrentMasterKey(
              this.currentMasterKey.getMasterKey());
          this.nmStateStore.storeAMRMProxyNextMasterKey(null);
        } catch (IOException e) {
          LOG.error("Unable to update current master key in state store", e);
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * 创建新的主密钥，递增序列号生成密钥ID
   * @return 新创建的主密钥数据
   */
  @Private
  @VisibleForTesting
  public MasterKeyData createNewMasterKey() {
    this.writeLock.lock();
    try {
      return new MasterKeyData(serialNo++, generateSecret());
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * 为指定应用尝试创建并返回AMRM令牌
   * @param appAttemptId 应用尝试ID
   * @return 创建好的AMRM令牌
   */
  public Token<AMRMTokenIdentifier> createAndGetAMRMToken(
      ApplicationAttemptId appAttemptId) {
    this.writeLock.lock();
    try {
      LOG.info("Create AMRMToken for ApplicationAttempt: " + appAttemptId);
      // 创建AMRM令牌标识符
      AMRMTokenIdentifier identifier =
          new AMRMTokenIdentifier(appAttemptId, getMasterKey()
              .getMasterKey().getKeyId());
      // 生成令牌密码
      byte[] password = this.createPassword(identifier);
      // 将应用尝试加入活跃集合
      appAttemptSet.add(appAttemptId);
      return new Token<>(identifier.getBytes(), password, identifier.getKind(), new Text());
    } finally {
      this.writeLock.unlock();
    }
  }

  // If nextMasterKey is not Null, then return nextMasterKey
  // otherwise return currentMasterKey.
  @VisibleForTesting
  public MasterKeyData getMasterKey() {
    this.readLock.lock();
    try {
      // 有下一个密钥则返回下一个，否则返回当前生效密钥
      return nextMasterKey == null ? currentMasterKey : nextMasterKey;
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * Retrieve the password for the given {@link AMRMTokenIdentifier}. Used by
   * RPC layer to validate a remote {@link AMRMTokenIdentifier}.
   * 根据令牌标识符获取对应的密码，用于RPC层验证远程AMRM令牌有效性
   */
  @Override
  public byte[] retrievePassword(AMRMTokenIdentifier identifier)
      throws InvalidToken {
    this.readLock.lock();
    try {
      ApplicationAttemptId applicationAttemptId =
          identifier.getApplicationAttemptId();
      LOG.debug("Trying to retrieve password for {}", applicationAttemptId);
      // 检查应用尝试是否在本节点活跃集合中
      if (!appAttemptSet.contains(applicationAttemptId)) {
        throw new InvalidToken(applicationAttemptId
            + " not found in AMRMProxyTokenSecretManager.");
      }
      // 匹配当前主密钥ID
      if (identifier.getKeyId() == this.currentMasterKey.getMasterKey()
          .getKeyId()) {
        return createPassword(identifier.getBytes(),
            this.currentMasterKey.getSecretKey());
      } 
      // 匹配下一个主密钥ID
      else if (nextMasterKey != null
          && identifier.getKeyId() == this.nextMasterKey.getMasterKey()
              .getKeyId()) {
        return createPassword(identifier.getBytes(),
            this.nextMasterKey.getSecretKey());
      }
      // 密钥ID不匹配，令牌无效
      throw new InvalidToken("Invalid AMRMToken from "
          + applicationAttemptId);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * Creates an empty TokenId to be used for de-serializing an
   * {@link AMRMTokenIdentifier} by the RPC layer.
   * 创建空的AMRM令牌标识符，用于RPC层反序列化
   */
  @Override
  public AMRMTokenIdentifier createIdentifier() {
    return new AMRMTokenIdentifier();
  }

  @Private
  @VisibleForTesting
  public MasterKeyData getCurrentMasterKeyData() {
    this.readLock.lock();
    try {
      return this.currentMasterKey;
    } finally {
      this.readLock.unlock();
    }
  }

  @Private
  @VisibleForTesting
  public MasterKeyData getNextMasterKeyData() {
    this.readLock.lock();
    try {
      return this.nextMasterKey;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  @Private
  protected byte[] createPassword(AMRMTokenIdentifier identifier) {
    this.readLock.lock();
    try {
      ApplicationAttemptId applicationAttemptId =
          identifier.getApplicationAttemptId();
      LOG.info("Creating password for " + applicationAttemptId);
      // 使用当前生效主密钥生成密码
      return createPassword(identifier.getBytes(), getMasterKey()
          .getSecretKey());
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * Recover secretManager from state store. Called after serviceInit before
   * serviceStart.
   * 从NM状态存储恢复密钥管理器状态，在服务初始化后启动前调用
   *
   * @param state 恢复状态数据
   */
  public void recover(RecoveredAMRMProxyState state) {
    if (state != null) {
      // 恢复当前生效主密钥
      MasterKey currentKey = state.getCurrentMasterKey();
      if (currentKey != null) {
        this.currentMasterKey = new MasterKeyData(currentKey,
            createSecretKey(currentKey.getBytes().array()));
      } else {
        LOG.warn("No current master key recovered from NM StateStore"
            + " for AMRMProxyTokenSecretManager");
      }

      // 恢复待激活的下一个主密钥
      MasterKey nextKey = state.getNextMasterKey();
      if (nextKey != null) {
        this.nextMasterKey = new MasterKeyData(nextKey,
            createSecretKey(nextKey.getBytes().array()));
        // 按原延迟计划安排激活任务
        this.timer.schedule(new NextKeyActivator(), this.activationDelay);
      }
    }
  }

}