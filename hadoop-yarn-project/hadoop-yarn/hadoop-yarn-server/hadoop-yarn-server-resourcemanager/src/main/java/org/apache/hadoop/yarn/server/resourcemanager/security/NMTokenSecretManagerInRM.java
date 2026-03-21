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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.security.BaseNMTokenSecretManager;
import org.apache.hadoop.yarn.server.security.MasterKeyData;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * ResourceManager端的NodeManager令牌密钥管理器，负责NMToken的生成、密钥轮换和缓存管理
 * NMToken用于ApplicationMaster与NodeManager之间的身份认证
 */
public class NMTokenSecretManagerInRM extends BaseNMTokenSecretManager {

  private static final Logger LOG = LoggerFactory
      .getLogger(NMTokenSecretManagerInRM.class);

  // 下一个待激活的主密钥
  private MasterKeyData nextMasterKey;
  private Configuration conf;

  // 密钥轮换定时器
  private final Timer timer;
  // 主密钥轮换间隔（毫秒）
  private final long rollingInterval;
  // 新密钥激活延迟（毫秒），解决密钥同步问题
  private final long activationDelay;
  // 应用尝试 -> 该应用已生成NMToken的Node集合，缓存避免重复生成
  private final ConcurrentHashMap<ApplicationAttemptId, HashSet<NodeId>> appAttemptToNodeKeyMap;
  
  /**
   * 构造函数，从配置中初始化密钥管理器参数
   */
  public NMTokenSecretManagerInRM(Configuration conf) {
    this.conf = conf;
    timer = new Timer();
    rollingInterval = this.conf.getLong(
        YarnConfiguration.RM_NMTOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
        YarnConfiguration.DEFAULT_RM_NMTOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS)
        * 1000;
    // Add an activation delay. This is to address the following race: RM may
    // roll over master-key, scheduling may happen at some point of time, an
    // NMToken created with a password generated off new master key, but NM
    // might not have come again to RM to update the shared secret: so AM has a
    // valid password generated off new secret but NM doesn't know about the
    // secret yet.
    // Adding delay = 1.5 * expiry interval makes sure that all active NMs get
    // the updated shared-key.
    this.activationDelay =
        (long) (conf.getLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS) * 1.5);
    LOG.info("NMTokenKeyRollingInterval: " + this.rollingInterval
        + "ms and NMTokenKeyActivationDelay: " + this.activationDelay
        + "ms");
    // 检查配置合法性：轮换间隔必须大于3倍节点过期间隔，避免密钥轮转过快
    if (rollingInterval <= activationDelay * 2) {
      throw new IllegalArgumentException(
          YarnConfiguration.RM_NMTOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS
              + " should be more than 3 X "
              + YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS);
    }
    appAttemptToNodeKeyMap =
        new ConcurrentHashMap<ApplicationAttemptId, HashSet<NodeId>>();
  }
  
  /**
   * 生成新的主密钥，准备轮换
   */
  @Private
  public void rollMasterKey() {
    super.writeLock.lock();
    try {
      LOG.info("Rolling master-key for nm-tokens");
      if (this.currentMasterKey == null) { // 首次初始化，直接设置为主密钥
        this.currentMasterKey = createNewMasterKey();
      } else {
        this.nextMasterKey = createNewMasterKey();
        LOG.info("Going to activate master-key with key-id "
            + this.nextMasterKey.getMasterKey().getKeyId() + " in "
            + this.activationDelay + "ms");
        // 延迟激活新密钥，给所有NM足够时间拉取新密钥
        this.timer.schedule(new NextKeyActivator(), this.activationDelay);
      }
    } finally {
      super.writeLock.unlock();
    }
  }

  /**
   * 获取下一个待激活的主密钥，供NM拉取
   */
  @Private
  public MasterKey getNextKey() {
    super.readLock.lock();
    try {
      if (this.nextMasterKey == null) {
        return null;
      } else {
        return this.nextMasterKey.getMasterKey();
      }
    } finally {
      super.readLock.unlock();
    }
  }

  /**
   * 将下一个主密钥激活为当前使用的主密钥
   */
  @Private
  public void activateNextMasterKey() {
    super.writeLock.lock();
    try {
      LOG.info("Activating next master key with id: "
          + this.nextMasterKey.getMasterKey().getKeyId());
      this.currentMasterKey = this.nextMasterKey;
      this.nextMasterKey = null;
      // 清空所有应用的NMToken缓存，密钥更换后需要重新生成令牌
      clearApplicationNMTokenKeys();
    } finally {
      super.writeLock.unlock();
    }
  }

  /**
   * 清空指定应用尝试的节点NMToken缓存
   */
  public void clearNodeSetForAttempt(ApplicationAttemptId attemptId) {
    super.writeLock.lock();
    try {
      HashSet<NodeId> nodeSet = this.appAttemptToNodeKeyMap.get(attemptId);
      if (nodeSet != null) {
        LOG.info("Clear node set for " + attemptId);
        nodeSet.clear();
      }
    } finally {
      super.writeLock.unlock();
    }
  }

  /**
   * 清空所有应用的NMToken节点缓存
   */
  private void clearApplicationNMTokenKeys() {
    // We should clear all node entries from this set.
    // TODO : Once we have per node master key then it will change to only
    // remove specific node from it.
    Iterator<HashSet<NodeId>> nodeSetI =
        this.appAttemptToNodeKeyMap.values().iterator();
    while (nodeSetI.hasNext()) {
      nodeSetI.next().clear();
    }
  }

  /**
   * 启动密钥管理器，开始定时轮换主密钥
   */
  public void start() {
    rollMasterKey();
    // 启动定时任务，按固定间隔轮换主密钥
    this.timer.scheduleAtFixedRate(new MasterKeyRoller(), rollingInterval,
        rollingInterval);
  }

  /**
   * 停止密钥管理器，关闭定时器
   */
  public void stop() {
    this.timer.cancel();
  }

  /**
   * 主密钥轮换定时任务
   */
  private class MasterKeyRoller extends TimerTask {
    @Override
    public void run() {
      rollMasterKey();
    }
  }
  
  /**
   * 延迟激活新主密钥定时任务
   */
  private class NextKeyActivator extends TimerTask {
    @Override
    public void run() {
      // Activation will happen after an absolute time interval. It will be good
      // if we can force activation after an NM updates and acknowledges a
      // roll-over. But that is only possible when we move to per-NM keys. TODO:
      activateNextMasterKey();
    }
  }

  /**
   * 为容器所在节点创建并返回NMToken，同一应用同一节点仅生成一次
   */
  public NMToken createAndGetNMToken(String applicationSubmitter,
      ApplicationAttemptId appAttemptId, Container container) {
    this.writeLock.lock();
    try {
      HashSet<NodeId> nodeSet = this.appAttemptToNodeKeyMap.get(appAttemptId);
      NMToken nmToken = null;
      if (nodeSet != null) {
        // 仅当该节点还没有为当前应用生成过令牌时才创建
        if (!nodeSet.contains(container.getNodeId())) {
          LOG.debug("Sending NMToken for nodeId : {} for container : {}",
              container.getNodeId(), container.getId());
          Token token =
              createNMToken(container.getId().getApplicationAttemptId(),
                container.getNodeId(), applicationSubmitter);
          nmToken = NMToken.newInstance(container.getNodeId(), token);
          nodeSet.add(container.getNodeId());
        }
      }
      return nmToken;
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * 注册新的应用尝试，初始化其节点缓存
   */
  public void registerApplicationAttempt(ApplicationAttemptId appAttemptId) {
    this.writeLock.lock();
    try {
      this.appAttemptToNodeKeyMap.put(appAttemptId, new HashSet<NodeId>());
    } finally {
      this.writeLock.unlock();
    }
  }
  
  @Private
  @VisibleForTesting
  public boolean isApplicationAttemptRegistered(
      ApplicationAttemptId appAttemptId) {
    this.readLock.lock();
    try {
      return this.appAttemptToNodeKeyMap.containsKey(appAttemptId);
    } finally {
      this.readLock.unlock();
    }
  }
  
  @Private
  @VisibleForTesting
  public boolean isApplicationAttemptNMTokenPresent(
      ApplicationAttemptId appAttemptId, NodeId nodeId) {
    this.readLock.lock();
    try {
      HashSet<NodeId> nodes = this.appAttemptToNodeKeyMap.get(appAttemptId);
      if (nodes != null && nodes.contains(nodeId)) {
        return true;
      } else {
        return false;
      }
    } finally {
      this.readLock.unlock();
    }
  }
  
  /**
   * 注销应用尝试，清理其节点缓存
   */
  public void unregisterApplicationAttempt(ApplicationAttemptId appAttemptId) {
    this.writeLock.lock();
    try {
      this.appAttemptToNodeKeyMap.remove(appAttemptId);
    } finally {
      this.writeLock.unlock();
    }
  }
  
  /**
   * 当NodeManager重连或下线时，移除所有应用中该节点的NMToken缓存，后续需要重新生成
   * @param nodeId 节点ID
   */
  public void removeNodeKey(NodeId nodeId) {
    this.writeLock.lock();
    try {
      Iterator<HashSet<NodeId>> appNodeKeySetIterator =
          this.appAttemptToNodeKeyMap.values().iterator();
      while (appNodeKeySetIterator.hasNext()) {
        appNodeKeySetIterator.next().remove(nodeId);
      }
    } finally {
      this.writeLock.unlock();
    }
  }
}