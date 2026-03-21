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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * ResourceManager 专用的委派令牌密钥管理器。
 * 负责生成和验证每个委派令牌的密码，支持RM HA场景下的令牌状态持久化与恢复。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RMDelegationTokenSecretManager extends
    AbstractDelegationTokenSecretManager<RMDelegationTokenIdentifier> implements
    Recoverable {
  private static final Logger LOG = LoggerFactory
      .getLogger(RMDelegationTokenSecretManager.class);

  // 当前关联的ResourceManager实例
  private final ResourceManager rm;

  /**
   * 创建RM委派令牌密钥管理器
   * @param delegationKeyUpdateInterval 滚动生成新密钥的间隔毫秒数
   * @param delegationTokenMaxLifetime 委派令牌最大生命周期毫秒数
   * @param delegationTokenRenewInterval 令牌必须续期的间隔毫秒数
   * @param delegationTokenRemoverScanInterval 扫描过期令牌的间隔毫秒数
   * @param rmContext 当前ResourceManager上下文
   */
  public RMDelegationTokenSecretManager(long delegationKeyUpdateInterval,
                                      long delegationTokenMaxLifetime,
                                      long delegationTokenRenewInterval,
                                      long delegationTokenRemoverScanInterval,
                                      RMContext rmContext) {
    super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
          delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    this.rm = rmContext.getResourceManager();
  }

  @Override
  public RMDelegationTokenIdentifier createIdentifier() {
    return new RMDelegationTokenIdentifier();
  }

  /**
   * 判断是否应该忽略该异常
   * @param e 捕获的异常
   * @return 当服务已停止且异常由中断引起时返回true
   */
  private boolean shouldIgnoreException(Exception e) {
    return !running && e.getCause() instanceof InterruptedException;
  }

  @Override
  protected void storeNewMasterKey(DelegationKey newKey) {
    try {
      LOG.info("storing master key with keyID " + newKey.getKeyId());
      // 将新生成的主密钥持久化到RM状态存储
      rm.getRMContext().getStateStore().storeRMDTMasterKey(newKey);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error(
            "Error in storing master key with KeyID: " + newKey.getKeyId());
        // 存储失败终止进程
        ExitUtil.terminate(1, e);
      }
    }
  }

  @Override
  protected void removeStoredMasterKey(DelegationKey key) {
    try {
      LOG.info("removing master key with keyID " + key.getKeyId());
      // 从RM状态存储中移除过期主密钥
      rm.getRMContext().getStateStore().removeRMDTMasterKey(key);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in removing master key with KeyID: " + key.getKeyId());
        ExitUtil.terminate(1, e);
      }
    }
  }

  @Override
  protected void storeNewToken(RMDelegationTokenIdentifier identifier,
      long renewDate) {
    try {
      LOG.info("storing RMDelegation token with sequence number: "
          + identifier.getSequenceNumber());
      // 将新生成的委派令牌持久化到RM状态存储
      rm.getRMContext().getStateStore().storeRMDelegationToken(identifier,
          renewDate);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in storing RMDelegationToken with sequence number: "
            + identifier.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  @Override
  protected void updateStoredToken(RMDelegationTokenIdentifier id,
      long renewDate) {
    try {
      LOG.info("updating RMDelegation token with sequence number: "
          + id.getSequenceNumber());
      // 更新持久化存储中令牌的续期时间
      rm.getRMContext().getStateStore().updateRMDelegationToken(id, renewDate);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in updating persisted RMDelegationToken"
            + " with sequence number: " + id.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  @Override
  protected void removeStoredToken(RMDelegationTokenIdentifier ident)
      throws IOException {
    try {
      LOG.info("removing RMDelegation token with sequence number: "
          + ident.getSequenceNumber());
      // 从持久化存储中移除已取消/过期的令牌
      rm.getRMContext().getStateStore().removeRMDelegationToken(ident);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error(
            "Error in removing RMDelegationToken with sequence number: "
                + ident.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  @Private
  @VisibleForTesting
  public synchronized Set<DelegationKey> getAllMasterKeys() {
    HashSet<DelegationKey> keySet = new HashSet<DelegationKey>();
    keySet.addAll(allKeys.values());
    return keySet;
  }

  @Private
  @VisibleForTesting
  public synchronized Map<RMDelegationTokenIdentifier, Long> getAllTokens() {
    Map<RMDelegationTokenIdentifier, Long> allTokens =
        new HashMap<RMDelegationTokenIdentifier, Long>();

    for (Map.Entry<RMDelegationTokenIdentifier,
        DelegationTokenInformation> entry : currentTokens.entrySet()) {
      allTokens.put(entry.getKey(), entry.getValue().getRenewDate());
    }
    return allTokens;
  }

  @Private
  @VisibleForTesting
  public int getLatestDTSequenceNumber() {
    return delegationTokenSequenceNumber;
  }

  @Override
  public void recover(RMState rmState) throws Exception {

    LOG.info("recovering RMDelegationTokenSecretManager.");
    // 恢复所有主密钥
    for (DelegationKey dtKey : rmState.getRMDTSecretManagerState()
      .getMasterKeyState()) {
      addKey(dtKey);
    }

    // 恢复所有委派令牌
    Map<RMDelegationTokenIdentifier, Long> rmDelegationTokens =
        rmState.getRMDTSecretManagerState().getTokenState();
    this.delegationTokenSequenceNumber =
        rmState.getRMDTSecretManagerState().getDTSequenceNumber();
    for (Map.Entry<RMDelegationTokenIdentifier, Long> entry : rmDelegationTokens
      .entrySet()) {
      addPersistedDelegationToken(entry.getKey(), entry.getValue());
    }
  }

  /**
   * 获取指定委派令牌的续期时间
   * @param ident 委派令牌标识符
   * @return 续期时间戳
   * @throws InvalidToken 令牌不存在时抛出异常
   */
  public long getRenewDate(RMDelegationTokenIdentifier ident)
      throws InvalidToken {
    DelegationTokenInformation info = currentTokens.get(ident);
    if (info == null) {
      throw new InvalidToken("token (" + ident.toString()
          + ") can't be found in cache");
    }
    return info.getRenewDate();
  }
}