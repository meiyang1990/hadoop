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

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.hs.HistoryServerStateStoreService.HistoryServerState;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 历史服务器专用的MapReduce委派令牌密钥管理器
 * 负责生成和验证委派令牌的密码，为MapReduce历史服务器提供安全认证能力
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JHSDelegationTokenSecretManager
    extends AbstractDelegationTokenSecretManager<MRDelegationTokenIdentifier> {

  private static final Logger LOG = LoggerFactory.getLogger(
      JHSDelegationTokenSecretManager.class);

  // 历史服务器状态存储服务，用于持久化令牌密钥相关状态
  private HistoryServerStateStoreService store;

  /**
   * 构造历史服务器委派令牌密钥管理器
   * @param delegationKeyUpdateInterval 滚动生成新密钥的间隔毫秒数
   * @param delegationTokenMaxLifetime 委派令牌的最大存活时间毫秒数
   * @param delegationTokenRenewInterval 令牌必须更新的间隔毫秒数
   * @param delegationTokenRemoverScanInterval 扫描过期令牌的间隔毫秒数
   * @param store 持久化状态的历史服务器存储服务
   */
  public JHSDelegationTokenSecretManager(long delegationKeyUpdateInterval,
                                      long delegationTokenMaxLifetime, 
                                      long delegationTokenRenewInterval,
                                      long delegationTokenRemoverScanInterval,
                                      HistoryServerStateStoreService store) {
    super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
          delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    this.store = store;
  }

  @Override
  /**
   * 创建MapReduce委派令牌标识符实例
   * @return 新建的MRDelegationTokenIdentifier实例
   */
  public MRDelegationTokenIdentifier createIdentifier() {
    return new MRDelegationTokenIdentifier();
  }

  @Override
  /**
   * 持久化存储新的主密钥
   * @param key 需要存储的主密钥
   * @throws IOException 存储失败抛出IO异常
   */
  protected void storeNewMasterKey(DelegationKey key) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing master key " + key.getKeyId());
    }
    try {
      // 调用存储服务存储主密钥
      store.storeTokenMasterKey(key);
    } catch (IOException e) {
      LOG.error("Unable to store master key " + key.getKeyId(), e);
      throw e;
    }
  }

  @Override
  /**
   * 从存储中移除已过期的主密钥
   * @param key 需要移除的主密钥
   */
  protected void removeStoredMasterKey(DelegationKey key) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing master key " + key.getKeyId());
    }
    try {
      // 调用存储服务移除主密钥
      store.removeTokenMasterKey(key);
    } catch (IOException e) {
      LOG.error("Unable to remove master key " + key.getKeyId(), e);
    }
  }

  @Override
  /**
   * 持久化存储新的委派令牌
   * @param tokenId 委派令牌标识符
   * @param renewDate 令牌更新时间
   */
  protected void storeNewToken(MRDelegationTokenIdentifier tokenId,
      long renewDate) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing token " + tokenId.getSequenceNumber());
    }
    try {
      // 调用存储服务存储新令牌
      store.storeToken(tokenId, renewDate);
    } catch (IOException e) {
      LOG.error("Unable to store token " + tokenId.getSequenceNumber(), e);
    }
  }

  @Override
  /**
   * 从存储中移除指定委派令牌
   * @param tokenId 需要移除的委派令牌标识符
   * @throws IOException 移除失败抛出IO异常
   */
  protected void removeStoredToken(MRDelegationTokenIdentifier tokenId)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing token " + tokenId.getSequenceNumber());
    }
    try {
      // 调用存储服务移除令牌
      store.removeToken(tokenId);
    } catch (IOException e) {
      LOG.error("Unable to remove token " + tokenId.getSequenceNumber(), e);
      throw e;
    }
  }

  @Override
  /**
   * 更新存储中已有委派令牌的更新时间
   * @param tokenId 委派令牌标识符
   * @param renewDate 新的更新时间
   */
  protected void updateStoredToken(MRDelegationTokenIdentifier tokenId,
      long renewDate) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating token " + tokenId.getSequenceNumber());
    }
    try {
      // 调用存储服务更新令牌更新时间
      store.updateToken(tokenId, renewDate);
    } catch (IOException e) {
      LOG.error("Unable to update token " + tokenId.getSequenceNumber(), e);
    }
  }

  /**
   * 从历史服务器持久化状态中恢复令牌密钥信息
   * @param state 从存储加载的历史服务器状态
   * @throws IOException 恢复过程IO异常
   */
  public void recover(HistoryServerState state) throws IOException {
    LOG.info("Recovering " + getClass().getSimpleName());
    // 恢复所有主密钥到内存
    for (DelegationKey key : state.tokenMasterKeyState) {
      addKey(key);
    }
    // 恢复所有持久化的委派令牌到内存
    for (Entry<MRDelegationTokenIdentifier, Long> entry :
        state.tokenState.entrySet()) {
      addPersistedDelegationToken(entry.getKey(), entry.getValue());
    }
  }
}