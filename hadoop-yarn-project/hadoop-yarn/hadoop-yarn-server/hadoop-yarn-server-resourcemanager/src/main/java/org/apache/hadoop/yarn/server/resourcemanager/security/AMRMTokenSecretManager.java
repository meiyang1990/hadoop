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

package org.apache.hadoop.yarn.server.resourcemanager.security;

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
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.security.MasterKeyData;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * AMRMToken密钥管理器，负责ApplicationMaster与ResourceManager之间认证令牌的生成、轮换和验证。
 * AMRM令牌按应用尝试分配，密钥会定期轮换提高安全性，支持RM重启后状态恢复。
 * 每个应用尝试完成后会清理对应令牌信息，密钥轮换后旧密钥会逐步淘汰。
 */
public class AMRMTokenSecretManager extends
    SecretManager<AMRMTokenIdentifier> {

  private static final Logger LOG = LoggerFactory
      .getLogger(AMRMTokenSecretManager.class);

  // 下一个主密钥序列号，每次生成新密钥自增
  private int serialNo = new SecureRandom().nextInt();
  // 待激活的下一个主密钥
  private MasterKeyData nextMasterKey;
  // 当前正在使用的主密钥
  private MasterKeyData currentMasterKey;

  // 读写锁，保护主密钥和应用尝试集合的并发访问
  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();

  // 定时器，用于执行密钥轮换和激活任务
  private final Timer timer;
  // 密钥轮换间隔（毫秒）
  private final long rollingInterval;
  // 新密钥激活延迟时间（毫秒），确保所有运行中的AM都能获取到新密钥
  private final long activationDelay;
  // ResourceManager上下文对象
  private RMContext rmContext;

  // 当前存在的所有应用尝试集合，用于验证令牌有效性
  private final Set<ApplicationAttemptId> appAttemptSet =
      new HashSet<ApplicationAttemptId>();

  /**
   * 构造AMRMToken密钥管理器，从配置中加载轮换间隔和激活延迟参数。
   * @param conf YARN配置对象
   * @param rmContext RM上下文对象
   */
  public AMRMTokenSecretManager(Configuration conf, RMContext rmContext) {
    this.rmContext = rmContext;
    this.timer = new Timer();
    this.rollingInterval =
        conf
          .getLong(
            YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
            YarnConfiguration.DEFAULT_RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS) * 1000;
    // 激活延迟设置为AM令牌过期时间的1.5倍，确保所有活跃AM都能更新到新密钥
    String rmAmExpiryIntervalMS = conf.get(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS);
    if (NumberUtils.isDigits(rmAmExpiryIntervalMS)) {
      this.activationDelay = (long) (conf.getLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS) * 1.5);
    } else {
      this.activationDelay =
          (long) (conf.getTimeDuration(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS, TimeUnit.MILLISECONDS) * 1.5);
    }

    LOG.info("AMRMTokenKeyRollingInterval: {} ms and AMRMTokenKeyActivationDelay: {} ms",
        this.rollingInterval, this.activationDelay);
    // 校验配置：轮换间隔必须大于3倍过期时间（激活延迟1.5倍，加上安全余量）
    if (rollingInterval <= activationDelay * 2) {
      throw new IllegalArgumentException(
          YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS
              + " should be more than 3 X "
              + YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS);
    }
  }

  /**
   * 启动密钥管理器，初始化当前主密钥并启动定时轮换任务。
   */
  public void start() {
    if (this.currentMasterKey == null) {
      this.currentMasterKey = createNewMasterKey();
      AMRMTokenSecretManagerState state =
          AMRMTokenSecretManagerState.newInstance(
            this.currentMasterKey.getMasterKey(), null);
      // 存储初始密钥状态到状态存储，支持重启恢复
      rmContext.getStateStore().storeOrUpdateAMRMTokenSecretManager(state,
          false);
    }
    // 按固定间隔安排密钥轮换任务
    this.timer.scheduleAtFixedRate(new MasterKeyRoller(), rollingInterval,
      rollingInterval);
  }

  /**
   * 停止密钥管理器，取消定时任务。
   */
  public void stop() {
    this.timer.cancel();
  }

  /**
   * 应用尝试完成后，清理对应令牌信息。
   * @param appAttemptId 完成的应用尝试ID
   */
  public void applicationMasterFinished(ApplicationAttemptId appAttemptId) {
    this.writeLock.lock();
    try {
      LOG.info("Application finished, removing password for " + appAttemptId);
      this.appAttemptSet.remove(appAttemptId);
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * 定时执行主密钥轮换的任务类
   */
  private class MasterKeyRoller extends TimerTask {
    @Override
    public void run() {
      rollMasterKey();
    }
  }

  /**
   * 执行主密钥轮换，生成新密钥并安排延迟激活任务。
   */
  @Private
  void rollMasterKey() {
    this.writeLock.lock();
    try {
      LOG.info("Rolling master-key for amrm-tokens");
      this.nextMasterKey = createNewMasterKey();
      AMRMTokenSecretManagerState state =
          AMRMTokenSecretManagerState.newInstance(
            this.currentMasterKey.getMasterKey(),
            this.nextMasterKey.getMasterKey());
      // 更新存储的密钥状态
      rmContext.getStateStore()
          .storeOrUpdateAMRMTokenSecretManager(state, true);
      // 延迟激活新密钥，给现有AM时间获取新密钥
      this.timer.schedule(new NextKeyActivator(), this.activationDelay);
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * 延迟激活新密钥的任务类
   */
  private class NextKeyActivator extends TimerTask {
    @Override
    public void run() {
      activateNextMasterKey();
    }
  }

  /**
   * 将待激活的下一个主密钥激活为当前主密钥，淘汰旧密钥。
   */
  public void activateNextMasterKey() {
    this.writeLock.lock();
    try {
      LOG.info("Activating next master key with id: "
          + this.nextMasterKey.getMasterKey().getKeyId());
      this.currentMasterKey = this.nextMasterKey;
      this.nextMasterKey = null;
      AMRMTokenSecretManagerState state =
          AMRMTokenSecretManagerState.newInstance(
            this.currentMasterKey.getMasterKey(), null);
      // 更新存储的密钥状态
      rmContext.getStateStore()
          .storeOrUpdateAMRMTokenSecretManager(state, true);
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * 生成新的主密钥，序列号自增。
   * @return 新生成的主密钥数据
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
   * 为指定应用尝试创建并返回AMRM令牌。
   * @param appAttemptId 应用尝试ID
   * @return 创建完成的AMRM令牌
   */
  public Token<AMRMTokenIdentifier> createAndGetAMRMToken(
      ApplicationAttemptId appAttemptId) {
    this.writeLock.lock();
    try {
      LOG.info("Create AMRMToken for ApplicationAttempt: " + appAttemptId);
      AMRMTokenIdentifier identifier =
          new AMRMTokenIdentifier(appAttemptId, getMasterKey().getMasterKey()
            .getKeyId());
      byte[] password = this.createPassword(identifier);
      // 将应用尝试加入有效集合
      appAttemptSet.add(appAttemptId);
      return new Token<AMRMTokenIdentifier>(identifier.getBytes(), password,
        identifier.getKind(), new Text());
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * 获取当前应该使用的主密钥：如果有待激活的下一个密钥则返回下一个，否则返回当前密钥。
   * @return 要使用的主密钥数据
   */
  @VisibleForTesting
  public MasterKeyData getMasterKey() {
    this.readLock.lock();
    try {
      return nextMasterKey == null ? currentMasterKey : nextMasterKey;
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * 从RM重启恢复中添加持久化的AMRM令牌信息。
   * @param token 持久化存储的AMRM令牌
   * @throws IOException 反序列化标识符失败时抛出
   */
  public void addPersistedPassword(Token<AMRMTokenIdentifier> token)
      throws IOException {
    this.writeLock.lock();
    try {
      AMRMTokenIdentifier identifier = token.decodeIdentifier();
      LOG.debug("Adding password for " + identifier.getApplicationAttemptId());
      // 将恢复的应用尝试重新加入有效集合
      appAttemptSet.add(identifier.getApplicationAttemptId());
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * 根据AMRM令牌标识符获取对应密码，用于RPC层验证令牌合法性。
   * 支持当前和下一个（待激活）主密钥生成的令牌，平滑过渡密钥轮换。
   */
  @Override
  public byte[] retrievePassword(AMRMTokenIdentifier identifier)
      throws InvalidToken {
    this.readLock.lock();
    try {
      ApplicationAttemptId applicationAttemptId =
          identifier.getApplicationAttemptId();
      LOG.debug("Trying to retrieve password for {}", applicationAttemptId);
      // 检查应用尝试是否仍处于活跃状态
      if (!appAttemptSet.contains(applicationAttemptId)) {
        throw new InvalidToken(applicationAttemptId
            + " not found in AMRMTokenSecretManager.");
      }
      // 匹配当前主密钥
      if (identifier.getKeyId() == this.currentMasterKey.getMasterKey()
        .getKeyId()) {
        return createPassword(identifier.getBytes(),
          this.currentMasterKey.getSecretKey());
      // 匹配待激活的下一个主密钥，支持平滑过渡
      } else if (nextMasterKey != null
          && identifier.getKeyId() == this.nextMasterKey.getMasterKey()
            .getKeyId()) {
        return createPassword(identifier.getBytes(),
          this.nextMasterKey.getSecretKey());
      }
      // 密钥ID不匹配，令牌无效（已过期的旧密钥）
      throw new InvalidToken("Invalid AMRMToken from " + applicationAttemptId);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * 创建空的AMRMTokenIdentifier，供RPC层反序列化使用。
   */
  @Override
  public AMRMTokenIdentifier createIdentifier() {
    return new AMRMTokenIdentifier();
  }

  @Private
  @VisibleForTesting
  public MasterKeyData getCurrnetMasterKeyData() {
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
      // 使用当前生效的主密钥生成密码
      return createPassword(identifier.getBytes(), getMasterKey()
        .getSecretKey());
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * 从RM恢复状态中恢复AMRMToken密钥管理器的状态。
   * @param state RM恢复状态对象
   */
  public void recover(RMState state) {
    AMRMTokenSecretManagerState tokenState = getTokenState(state);
    if (tokenState != null) {
      // 恢复当前主密钥
      MasterKey currentKey = tokenState.getCurrentMasterKey();
      this.currentMasterKey =
          new MasterKeyData(currentKey, createSecretKey(currentKey.getBytes()
            .array()));

      // 如果存在待激活的下一个主密钥，也恢复它
      MasterKey nextKey = tokenState.getNextMasterKey();
      if (nextKey != null) {
        this.nextMasterKey =
            new MasterKeyData(nextKey, createSecretKey(nextKey.getBytes()
              .array()));
        // 恢复激活延迟任务
        this.timer.schedule(new NextKeyActivator(), this.activationDelay);
      }
    }
  }

  /**
   * 从RM状态中获取AMRMToken密钥管理器状态。
   * @param state RM恢复状态对象
   * @return 验证更新后的密钥管理器状态，不存在则返回null
   */
  private AMRMTokenSecretManagerState getTokenState(RMState state) {
    AMRMTokenSecretManagerState result = state.getAMRMTokenSecretManagerState();
    return result == null ? null : validateAndUpdateState(result);
  }

  /**
   * 验证恢复的密钥状态，如果密钥无效则重新生成并更新存储。
   * @param state 待验证的密钥状态
   * @return 验证更新后的密钥状态
   */
  private AMRMTokenSecretManagerState validateAndUpdateState(AMRMTokenSecretManagerState state) {
    MasterKey currentKey = state.getCurrentMasterKey();
    MasterKey nextKey = state.getNextMasterKey();
    boolean updateRequired = false;
    // 验证当前密钥有效性，无效则重新生成
    if (!validateMasterKey(currentKey)) {
      state.setCurrentMasterKey(createNewMasterKey().getMasterKey());
      updateRequired = true;
    }
    // 验证下一个密钥有效性，无效则重新生成
    if (!validateMasterKey(nextKey)) {
      state.setNextMasterKey(createNewMasterKey().getMasterKey());
      updateRequired = true;
    }
    // 如果有更新，持久化到状态存储
    if (updateRequired) {
      rmContext.getStateStore().storeOrUpdateAMRMTokenSecretManager(state, true);
    }
    return state;
  }

  /**
   * 验证主密钥的密钥长度是否合法。
   * @param masterKey 待验证的主密钥
   * @return 合法返回true，否则返回false
   */
  private boolean validateMasterKey(MasterKey masterKey) {
    return masterKey == null || validateSecretKeyLength(masterKey.getBytes().array());
  }
}