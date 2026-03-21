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

package org.apache.hadoop.yarn.server.nodemanager.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.server.nodemanager.recovery.RecoveryIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredNMTokensState;
import org.apache.hadoop.yarn.server.security.BaseNMTokenSecretManager;
import org.apache.hadoop.yarn.server.security.MasterKeyData;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * NodeManager节点上的NMToken密钥管理器，负责管理NMToken的生成、验证和持久化恢复
 * NMToken用于验证容器启动请求的合法性，保障NodeManager的容器启动安全
 */
public class NMTokenSecretManagerInNM extends BaseNMTokenSecretManager {

  private static final Logger LOG =
       LoggerFactory.getLogger(NMTokenSecretManagerInNM.class);
  
  // 上一代主密钥，用于验证滚动密钥前生成的旧NMToken
  private MasterKeyData previousMasterKey;
  
  // 按应用尝试保存的旧主密钥映射，用于验证不同尝试的旧NMToken
  private final Map<ApplicationAttemptId, MasterKeyData> oldMasterKeys;
  // 应用到其所有尝试的映射，用于应用完成后批量清理密钥
  private final Map<ApplicationId, List<ApplicationAttemptId>> appToAppAttemptMap;
  // NM状态存储服务，用于持久化NMToken密钥信息支持故障恢复
  private final NMStateStoreService stateStore;
  // 当前NodeManager的节点ID，用于验证NMToken是否归属本节点
  private NodeId nodeId;
  
  /**
   * 空存储构造函数，不持久化密钥状态
   */
  public NMTokenSecretManagerInNM() {
    this(new NMNullStateStoreService());
  }

  /**
   * 带状态存储的构造函数，指定用于持久化的状态存储服务
   * @param stateStore NM状态存储服务
   */
  public NMTokenSecretManagerInNM(NMStateStoreService stateStore) {
    this.oldMasterKeys =
        new HashMap<ApplicationAttemptId, MasterKeyData>();
    appToAppAttemptMap =         
        new HashMap<ApplicationId, List<ApplicationAttemptId>>();
    this.stateStore = stateStore;
  }
  
  /**
   * 从状态存储恢复之前持久化的NMToken密钥状态
   * @throws IOException 恢复过程IO异常
   */
  public synchronized void recover()
      throws IOException {
    // 加载持久化的NMToken状态
    RecoveredNMTokensState state = stateStore.loadNMTokensState();
    // 恢复当前主密钥
    MasterKey key = state.getCurrentMasterKey();
    if (key != null) {
      super.currentMasterKey =
          new MasterKeyData(key, createSecretKey(key.getBytes().array()));
    }

    // 恢复上一代主密钥
    key = state.getPreviousMasterKey();
    if (key != null) {
      previousMasterKey =
          new MasterKeyData(key, createSecretKey(key.getBytes().array()));
    }

    // 从当前主密钥恢复序列号
    if (super.currentMasterKey != null) {
      super.serialNo = super.currentMasterKey.getMasterKey().getKeyId() + 1;
    }

    // 遍历恢复所有应用尝试的旧主密钥
    try (RecoveryIterator<Map.Entry<ApplicationAttemptId, MasterKey>> it =
             state.getIterator()) {
      while (it.hasNext()) {
        Map.Entry<ApplicationAttemptId, MasterKey> entry = it.next();
        key = entry.getValue();
        oldMasterKeys.put(entry.getKey(),
            new MasterKeyData(key, createSecretKey(key.getBytes().array())));
      }
    }

    // 重建应用到尝试的映射关系
    appToAppAttemptMap.clear();
    for (ApplicationAttemptId attempt : oldMasterKeys.keySet()) {
      ApplicationId app = attempt.getApplicationId();
      List<ApplicationAttemptId> attempts = appToAppAttemptMap.get(app);
      if (attempts == null) {
        attempts = new ArrayList<ApplicationAttemptId>();
        appToAppAttemptMap.put(app, attempts);
      }
      attempts.add(attempt);
    }
  }

  // 更新当前主密钥并持久化到状态存储
  private void updateCurrentMasterKey(MasterKeyData key) {
    super.currentMasterKey = key;
    try {
      stateStore.storeNMTokenCurrentMasterKey(key.getMasterKey());
    } catch (IOException e) {
      LOG.error("Unable to update current master key in state store", e);
    }
  }

  // 更新上一代主密钥并持久化到状态存储
  private void updatePreviousMasterKey(MasterKeyData key) {
    previousMasterKey = key;
    try {
      stateStore.storeNMTokenPreviousMasterKey(key.getMasterKey());
    } catch (IOException e) {
      LOG.error("Unable to update previous master key in state store", e);
    }
  }

  /**
   * Used by NodeManagers to create a token-secret-manager with the key
   * obtained from the RM. This can happen during registration or when the RM
   * rolls the master-key and signal the NM.
   */
  @Private
  /**
   * 设置从RM获取的主密钥，处理主密钥滚动更新
   * @param masterKey RM下发的新主密钥
   */
  public synchronized void setMasterKey(MasterKey masterKey) {
    // 仅当密钥ID变化时才更新
    if (super.currentMasterKey == null || super.currentMasterKey.getMasterKey()
          .getKeyId() != masterKey.getKeyId()) {
      LOG.info("Rolling master-key for container-tokens, got key with id "
          + masterKey.getKeyId());
      // 将原当前密钥降级为前一代密钥
      if (super.currentMasterKey != null) {
        updatePreviousMasterKey(super.currentMasterKey);
      }
      // 设置新的当前密钥
      updateCurrentMasterKey(new MasterKeyData(masterKey,
          createSecretKey(masterKey.getBytes().array())));
    }
  }

  /**
   * This method will be used to verify NMTokens generated by different master
   * keys.
   */
  @Override
  /**
   * 根据NMToken标识符提取验证密码，验证NMToken合法性
   * @param identifier NMToken标识符
   * @return 验证密码字节数组
   * @throws InvalidToken 令牌非法时抛出异常
   */
  public synchronized byte[] retrievePassword(NMTokenIdentifier identifier)
      throws InvalidToken {
    int keyId = identifier.getKeyId();
    ApplicationAttemptId appAttemptId = identifier.getApplicationAttemptId();

    /*
     * MasterKey used for retrieving password will be as follows. 1) By default
     * older saved master key will be used. 2) If identifier's master key id
     * matches that of previous master key id then previous key will be used. 3)
     * If identifier's master key id matches that of current master key id then
     * current key will be used.
     */
    // 默认使用该应用尝试保存的旧主密钥
    MasterKeyData oldMasterKey = oldMasterKeys.get(appAttemptId);
    MasterKeyData masterKeyToUse = oldMasterKey;
    // 匹配上一代主密钥ID则使用上一代
    if (previousMasterKey != null
        && keyId == previousMasterKey.getMasterKey().getKeyId()) {
      masterKeyToUse = previousMasterKey;
    // 匹配当前主密钥ID则使用当前
    } else if (keyId == currentMasterKey.getMasterKey().getKeyId()) {
      masterKeyToUse = currentMasterKey;
    }
    
    // 验证NMToken是否归属当前NodeManager
    if (nodeId != null && !identifier.getNodeId().equals(nodeId)) {
      throw new InvalidToken("Given NMToken for application : "
          + appAttemptId.toString() + " is not valid for current node manager."
          + "expected : " + nodeId.toString() + " found : "
          + identifier.getNodeId().toString());
    }
    
    // 使用选中的密钥提取密码
    if (masterKeyToUse != null) {
      byte[] password = retrivePasswordInternal(identifier, masterKeyToUse);
      LOG.debug("NMToken password retrieved successfully!!");
      return password;
    }

    // 未找到对应密钥，令牌非法
    throw new InvalidToken("Given NMToken for application : "
        + appAttemptId.toString() + " seems to have been generated illegally.");
  }

  /**
   * 应用完成后清理该应用所有尝试的NMToken密钥
   * @param appId 已完成的应用ID
   */
  public synchronized void appFinished(ApplicationId appId) {
    List<ApplicationAttemptId> appAttemptList = appToAppAttemptMap.get(appId);
    if (appAttemptList != null) {
      LOG.debug("Removing application attempts NMToken keys for"
          + " application {}", appId);
      // 逐个删除应用尝试的密钥
      for (ApplicationAttemptId appAttemptId : appAttemptList) {
        removeAppAttemptKey(appAttemptId);
      }
      // 从映射中移除应用
      appToAppAttemptMap.remove(appId);
    } else {
      LOG.error("No application Attempt for application : " + appId
          + " started on this NM.");
    }
  }

  /**
   * This will be called by startContainer. It will add the master key into
   * the cache used for starting this container. This should be called before
   * validating the startContainer request.
   */
  /**
   * 容器启动前注册应用尝试的NMToken主密钥，缓存密钥用于后续验证
   * @param identifier NMToken标识符
   * @throws org.apache.hadoop.security.token.SecretManager.InvalidToken 令牌无效时抛出异常
   */
  public synchronized void appAttemptStartContainer(
      NMTokenIdentifier identifier)
      throws org.apache.hadoop.security.token.SecretManager.InvalidToken {
    ApplicationAttemptId appAttemptId = identifier.getApplicationAttemptId();
    // 应用首次启动时初始化映射条目
    if (!appToAppAttemptMap.containsKey(appAttemptId.getApplicationId())) {
      appToAppAttemptMap.put(appAttemptId.getApplicationId(),
        new ArrayList<ApplicationAttemptId>());
    }
    // 获取已有密钥
    MasterKeyData oldKey = oldMasterKeys.get(appAttemptId);

    // 新应用尝试，添加到映射
    if (oldKey == null) {
      appToAppAttemptMap.get(appAttemptId.getApplicationId()).add(appAttemptId);
    }
    // 密钥不存在或已更新，更新缓存
    if (oldKey == null
        || oldKey.getMasterKey().getKeyId() != identifier.getKeyId()) {
      LOG.debug("NMToken key updated for application attempt : {}",
          identifier.getApplicationAttemptId().toString());
      // 根据密钥ID选择对应主密钥
      if (identifier.getKeyId() == currentMasterKey.getMasterKey()
        .getKeyId()) {
        updateAppAttemptKey(appAttemptId, currentMasterKey);
      } else if (previousMasterKey != null
          && identifier.getKeyId() == previousMasterKey.getMasterKey()
            .getKeyId()) {
        updateAppAttemptKey(appAttemptId, previousMasterKey);
      } else {
        // 旧密钥不允许用于新容器启动，拒绝请求
        throw new InvalidToken(
          "Older NMToken should not be used while starting the container.");
      }
    }
  }
  
  /**
   * 设置当前NodeManager的节点ID
   * @param nodeId 当前节点ID
   */
  public synchronized void setNodeId(NodeId nodeId) {
    LOG.debug("updating nodeId : {}", nodeId);
    this.nodeId = nodeId;
  }
  
  @Private
  @VisibleForTesting
  /**
   * 检查指定应用尝试是否存在NMToken密钥，仅用于测试
   * @param appAttemptId 应用尝试ID
   * @return 是否存在密钥
   */
  public synchronized boolean
      isAppAttemptNMTokenKeyPresent(ApplicationAttemptId appAttemptId) {
    return oldMasterKeys.containsKey(appAttemptId);
  }
  
  @Private
  @VisibleForTesting
  /**
   * 获取当前节点ID，仅用于测试
   * @return 当前节点ID
   */
  public synchronized NodeId getNodeId() {
    return this.nodeId;
  }

  // 更新应用尝试的密钥并持久化到状态存储
  private void updateAppAttemptKey(ApplicationAttemptId attempt,
      MasterKeyData key) {
    this.oldMasterKeys.put(attempt, key);
    try {
      stateStore.storeNMTokenApplicationMasterKey(attempt,
          key.getMasterKey());
    } catch (IOException e) {
      LOG.error("Unable to store master key for application " + attempt, e);
    }
  }

  // 删除应用尝试的密钥并从状态存储移除
  private void removeAppAttemptKey(ApplicationAttemptId attempt) {
    this.oldMasterKeys.remove(attempt);
    try {
      stateStore.removeNMTokenApplicationMasterKey(attempt);
    } catch (IOException e) {
      LOG.error("Unable to remove master key for application " + attempt, e);
    }
  }

  /**
   * Used by the Distributed Scheduler framework to generate NMTokens
   * @param applicationSubmitter
   * @param container
   * @return NMToken
   */
  /**
   * 为容器生成NMToken，供ResourceManager调度分发
   * @param applicationSubmitter 应用提交者
   * @param container 目标容器
   * @return 生成的NMToken
   */
  public NMToken generateNMToken(
      String applicationSubmitter, Container container) {
    this.readLock.lock();
    try {
      Token token =
          createNMToken(container.getId().getApplicationAttemptId(),
              container.getNodeId(), applicationSubmitter);
      return NMToken.newInstance(container.getNodeId(), token);
    } finally {
      this.readLock.unlock();
    }
  }
}