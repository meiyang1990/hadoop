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

import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

/**
 * RM 容器令牌密钥管理器，负责容器令牌主密钥的定期轮换，是 RM 专属实现。
 * 
 */
public class RMContainerTokenSecretManager extends
    BaseContainerTokenSecretManager {

  private static final Logger LOG = LoggerFactory
      .getLogger(RMContainerTokenSecretManager.class);

  // 待激活的下一个主密钥
  private MasterKeyData nextMasterKey;

  private final Timer timer;
  private final long rollingInterval;
  private final long activationDelay;

  /**
   * 构造容器令牌密钥管理器，从配置加载轮换间隔和激活延迟参数。
   * @param conf YARN 配置
   */
  public RMContainerTokenSecretManager(Configuration conf) {
    super(conf);

    this.timer = new Timer();
    this.rollingInterval = conf.getLong(
            YarnConfiguration.RM_CONTAINER_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
            YarnConfiguration.DEFAULT_RM_CONTAINER_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS) * 1000;
    // Add an activation delay. This is to address the following race: RM may
    // roll over master-key, scheduling may happen at some point of time, a
    // container created with a password generated off new master key, but NM
    // might not have come again to RM to update the shared secret: so AM has a
    // valid password generated off new secret but NM doesn't know about the
    // secret yet.
    // Adding delay = 1.5 * expiry interval makes sure that all active NMs get
    // the updated shared-key.
    this.activationDelay =
        (long) (conf.getLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS) * 1.5);
    LOG.info("ContainerTokenKeyRollingInterval: " + this.rollingInterval
        + "ms and ContainerTokenKeyActivationDelay: " + this.activationDelay
        + "ms");
    if (rollingInterval <= activationDelay * 2) {
      throw new IllegalArgumentException(
          YarnConfiguration.RM_CONTAINER_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS
              + " should be more than 3 X "
              + YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS);
    }
  }

  /**
   * 启动密钥轮换任务，首次生成主密钥并启动定时轮换。
   */
  public void start() {
    rollMasterKey();
    this.timer.scheduleAtFixedRate(new MasterKeyRoller(), rollingInterval,
        rollingInterval);
  }

  /**
   * 停止密钥轮换，关闭定时器。
   */
  public void stop() {
    this.timer.cancel();
  }

  /**
   * 创建新主密钥，准备后续激活。
   */
  @Private
  public void rollMasterKey() {
    super.writeLock.lock();
    try {
      LOG.info("Rolling master-key for container-tokens");
      if (this.currentMasterKey == null) { // 第一次启动，初始化主密钥
        this.currentMasterKey = createNewMasterKey();
      } else {
        this.nextMasterKey = createNewMasterKey();
        LOG.info("Going to activate master-key with key-id "
            + this.nextMasterKey.getMasterKey().getKeyId() + " in "
            + this.activationDelay + "ms");
        // 延迟指定时间后激活新密钥，确保所有NM都有足够时间拉取新密钥
        this.timer.schedule(new NextKeyActivator(), this.activationDelay);
      }
    } finally {
      super.writeLock.unlock();
    }
  }

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
   * 激活预先生成的新主密钥，替换当前生效主密钥。
   */
  @Private
  public void activateNextMasterKey() {
    super.writeLock.lock();
    try {
      LOG.info("Activating next master key with id: "
          + this.nextMasterKey.getMasterKey().getKeyId());
      this.currentMasterKey = this.nextMasterKey;
      this.nextMasterKey = null;
    } finally {
      super.writeLock.unlock();
    }
  }

  // 定时执行主密钥轮换的任务
  private class MasterKeyRoller extends TimerTask {
    @Override
    public void run() {
      rollMasterKey();
    }
  }
  
  // 延迟激活新主密钥的任务
  private class NextKeyActivator extends TimerTask {
    @Override
    public void run() {
      // Activation will happen after an absolute time interval. It will be good
      // if we can force activation after an NM updates and acknowledges a
      // roll-over. But that is only possible when we move to per-NM keys. TODO:
      activateNextMasterKey();
    }
  }

  @VisibleForTesting
  public Token createContainerToken(ContainerId containerId,
      int containerVersion, NodeId nodeId, String appSubmitter,
      Resource capability, Priority priority, long createTime) {
    return createContainerToken(containerId, containerVersion, nodeId,
        appSubmitter, capability, priority, createTime,
        null, null, ContainerType.TASK,
        ExecutionType.GUARANTEED, -1, null);
  }

  /**
   * 创建容器令牌，用于NM对容器的身份认证。
   *
   * @param containerId 容器ID
   * @param containerVersion 容器版本
   * @param nodeId 目标节点ID
   * @param appSubmitter 应用提交者
   * @param capability 容器资源
   * @param priority 容器优先级
   * @param createTime 创建时间
   * @param logAggregationContext 日志聚合上下文
   * @param nodeLabelExpression 节点标签表达式
   * @param containerType 容器类型
   * @param execType 执行类型
   * @param allocationRequestId 分配请求ID
   * @param allocationTags 分配标签
   * @return 生成的容器令牌
   */
  public Token createContainerToken(ContainerId containerId,
      int containerVersion, NodeId nodeId, String appSubmitter,
      Resource capability, Priority priority, long createTime,
      LogAggregationContext logAggregationContext, String nodeLabelExpression,
      ContainerType containerType, ExecutionType execType,
      long allocationRequestId, Set<String> allocationTags) {
    byte[] password;
    ContainerTokenIdentifier tokenIdentifier;
    // 计算令牌过期时间
    long expiryTimeStamp =
        System.currentTimeMillis() + containerTokenExpiryInterval;

    // Lock so that we use the same MasterKey's keyId and its bytes
    this.readLock.lock();
    try {
      // 构建令牌标识符
      tokenIdentifier =
          new ContainerTokenIdentifier(containerId, containerVersion,
              nodeId.toString(), appSubmitter, capability, expiryTimeStamp,
              this.currentMasterKey.getMasterKey().getKeyId(),
              ResourceManager.getClusterTimeStamp(), priority, createTime,
              logAggregationContext, nodeLabelExpression, containerType,
              execType, allocationRequestId, allocationTags);
      // 用当前主密钥生成令牌密码
      password = this.createPassword(tokenIdentifier);

    } finally {
      this.readLock.unlock();
    }

    return BuilderUtils.newContainerToken(nodeId, password, tokenIdentifier);
  }
}