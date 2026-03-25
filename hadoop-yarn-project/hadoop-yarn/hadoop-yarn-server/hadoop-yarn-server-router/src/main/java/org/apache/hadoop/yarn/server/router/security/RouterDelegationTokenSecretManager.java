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
package org.apache.hadoop.yarn.server.router.security;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.security.token.delegation.RouterDelegationTokenSupport;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Base64;

/**
 * Router 专用的代理令牌密钥管理器，负责生成和验证代理令牌密码，
 * 将密钥和令牌信息持久化存储到联邦状态存储中，支持联邦集群多Router共享认证信息。
 */
public class RouterDelegationTokenSecretManager
    extends AbstractDelegationTokenSecretManager<RMDelegationTokenIdentifier> {

  private static final Logger LOG = LoggerFactory
      .getLogger(RouterDelegationTokenSecretManager.class);

  // 联邦状态存储门面，提供对状态存储的高层访问接口
  private FederationStateStoreFacade federationFacade;

  /**
   * 构造Router代理令牌密钥管理器。
   *
   * @param delegationKeyUpdateInterval        滚动生成新密钥的间隔毫秒数
   * @param delegationTokenMaxLifetime         代理令牌最大生命周期毫秒数
   * @param delegationTokenRenewInterval       代理令牌必须更新的间隔毫秒数
   * @param delegationTokenRemoverScanInterval 过期令牌扫描间隔毫秒数
   * @param conf 配置对象
   */
  public RouterDelegationTokenSecretManager(long delegationKeyUpdateInterval,
      long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
      long delegationTokenRemoverScanInterval, Configuration conf) {
    super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
        delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    this.federationFacade = FederationStateStoreFacade.getInstance(conf);
  }

  @Override
  public RMDelegationTokenIdentifier createIdentifier() {
    return new RMDelegationTokenIdentifier();
  }

  /**
   * 判断异常是否为停止服务时的中断异常，可以忽略。
   * @param e 捕获的异常
   * @return 是否可以忽略该异常
   */
  private boolean shouldIgnoreException(Exception e) {
    return !running && e.getCause() instanceof InterruptedException;
  }

  /**
   * 将新生成的主密钥存储到联邦状态存储。
   */
  @Override
  public void storeNewMasterKey(DelegationKey newKey) {
    try {
      // 通过门面存储新主密钥
      federationFacade.storeNewMasterKey(newKey);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in storing master key with KeyID: {}.", newKey.getKeyId());
        // 存储失败直接终止进程
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * 从联邦状态存储删除指定主密钥。
   */
  @Override
  public void removeStoredMasterKey(DelegationKey delegationKey) {
    try {
      federationFacade.removeStoredMasterKey(delegationKey);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in removing master key with KeyID: {}.", delegationKey.getKeyId());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * 将新代理令牌存储到联邦状态存储。
   */
  @Override
  public void storeNewToken(RMDelegationTokenIdentifier identifier,
      long renewDate) throws IOException {
    try {
      federationFacade.storeNewToken(identifier, renewDate);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in storing RMDelegationToken with sequence number: {}.",
            identifier.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * 将包含完整令牌信息的新代理令牌存储到联邦状态存储。
   *
   * @param identifier RM代理令牌标识符
   * @param tokenInfo 代理令牌信息
   */
  public void storeNewToken(RMDelegationTokenIdentifier identifier,
      DelegationTokenInformation tokenInfo) {
    try {
      // 编码令牌信息为字符串
      String token =
          RouterDelegationTokenSupport.encodeDelegationTokenInformation(tokenInfo);
      long renewDate = tokenInfo.getRenewDate();

      federationFacade.storeNewToken(identifier, renewDate, token);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in storing RMDelegationToken with sequence number: {}.",
            identifier.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * 更新联邦状态存储中已有代理令牌的更新时间。
   */
  @Override
  public void updateStoredToken(RMDelegationTokenIdentifier id, long renewDate) throws IOException {
    try {
      federationFacade.updateStoredToken(id, renewDate);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in updating persisted RMDelegationToken with sequence number: {}.",
            id.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * 更新联邦状态存储中已有代理令牌的完整信息。
   *
   * @param identifier RM代理令牌标识符
   * @param tokenInfo 更新后的代理令牌信息
   */
  public void updateStoredToken(RMDelegationTokenIdentifier identifier,
      DelegationTokenInformation tokenInfo) {
    try {
      long renewDate = tokenInfo.getRenewDate();
      // 重新编码令牌信息
      String token = RouterDelegationTokenSupport.encodeDelegationTokenInformation(tokenInfo);
      federationFacade.updateStoredToken(identifier, renewDate, token);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in updating persisted RMDelegationToken with sequence number: {}.",
            identifier.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * 从联邦状态存储删除指定代理令牌。
   */
  @Override
  public void removeStoredToken(RMDelegationTokenIdentifier identifier) throws IOException {
    try {
      federationFacade.removeStoredToken(identifier);
    } catch (Exception e) {
      if (!shouldIgnoreException(e)) {
        LOG.error("Error in removing RMDelegationToken with sequence number: {}",
            identifier.getSequenceNumber());
        ExitUtil.terminate(1, e);
      }
    }
  }

  /**
   * 根据指定密钥信息从联邦状态存储获取完整主密钥。
   *
   * @param key 待查询的主密钥信息
   * @return 完整的主密钥对象
   * @throws YarnException 获取过程中YARN内部错误
   * @throws IOException 获取过程中IO错误
   */
  public DelegationKey getMasterKeyByDelegationKey(DelegationKey key)
      throws YarnException, IOException {
    try {
      RouterMasterKeyResponse response = federationFacade.getMasterKeyByDelegationKey(key);
      RouterMasterKey masterKey = response.getRouterMasterKey();
      ByteBuffer keyByteBuf = masterKey.getKeyBytes();
      // 读取字节数组
      byte[] keyBytes = new byte[keyByteBuf.remaining()];
      keyByteBuf.get(keyBytes);
      // 构造返回完整DelegationKey对象
      DelegationKey delegationKey =
          new DelegationKey(masterKey.getKeyId(), masterKey.getExpiryDate(), keyBytes);
      return delegationKey;
    } catch (IOException ex) {
      throw new IOException(ex);
    } catch (YarnException ex) {
      throw new YarnException(ex);
    }
  }

  /**
   * 根据令牌标识符从联邦状态存储获取完整RM代理令牌标识符。
   *
   * @param identifier 待查询的令牌标识符
   * @return 完整的RM代理令牌标识符
   * @throws YarnException 获取过程中YARN内部错误
   * @throws IOException 获取过程中IO错误
   */
  public RMDelegationTokenIdentifier getTokenByRouterStoreToken(
      RMDelegationTokenIdentifier identifier) throws YarnException, IOException {
    try {
      RouterRMTokenResponse response = federationFacade.getTokenByRouterStoreToken(identifier);
      YARNDelegationTokenIdentifier responseIdentifier =
          response.getRouterStoreToken().getTokenIdentifier();
      return (RMDelegationTokenIdentifier) responseIdentifier;
    } catch (Exception ex) {
      throw new YarnException(ex);
    }
  }

  /**
   * 设置联邦状态存储门面，用于测试注入。
   * @param federationFacade 要设置的门面实例
   */
  public void setFederationFacade(FederationStateStoreFacade federationFacade) {
    this.federationFacade = federationFacade;
  }

  @Public
  @VisibleForTesting
  public int getLatestDTSequenceNumber() {
    return delegationTokenSequenceNumber;
  }

  @Public
  @VisibleForTesting
  public synchronized Set<DelegationKey> getAllMasterKeys() {
    return new HashSet<>(allKeys.values());
  }

  @Public
  @VisibleForTesting
  public synchronized Map<RMDelegationTokenIdentifier, Long> getAllTokens() {
    Map<RMDelegationTokenIdentifier, Long> allTokens = new HashMap<>();
    for (Map.Entry<RMDelegationTokenIdentifier,
         DelegationTokenInformation> entry : currentTokens.entrySet()) {
      RMDelegationTokenIdentifier keyIdentifier = entry.getKey();
      DelegationTokenInformation tokenInformation = entry.getValue();
      allTokens.put(keyIdentifier, tokenInformation.getRenewDate());
    }
    return allTokens;
  }

  /**
   * 获取指定令牌的更新日期。
   * @param ident 令牌标识符
   * @return 更新日期毫秒时间戳
   * @throws InvalidToken 令牌不存在于缓存时抛出
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

  @Override
  protected synchronized int incrementDelegationTokenSeqNum() {
    // 代理令牌序列号递增操作交由状态存储处理，保证全局唯一
    return federationFacade.incrementDelegationTokenSeqNum();
  }

  @Override
  protected void storeToken(RMDelegationTokenIdentifier rmDelegationTokenIdentifier,
      DelegationTokenInformation tokenInfo) throws IOException {
    // 先存入本地缓存
    this.currentTokens.put(rmDelegationTokenIdentifier, tokenInfo);
    // 更新用户令牌统计
    this.addTokenForOwnerStats(rmDelegationTokenIdentifier);
    // 持久化到状态存储
    storeNewToken(rmDelegationTokenIdentifier, tokenInfo);
  }

  @Override
  protected void updateToken(RMDelegationTokenIdentifier rmDelegationTokenIdentifier,
      DelegationTokenInformation tokenInfo) throws IOException {
    // 更新本地缓存
    this.currentTokens.put(rmDelegationTokenIdentifier, tokenInfo);
    // 更新持久化存储
    updateStoredToken(rmDelegationTokenIdentifier, tokenInfo);
  }

  @Override
  protected DelegationTokenInformation getTokenInfo(
      RMDelegationTokenIdentifier ident) {
    // 先查询本地缓存
    DelegationTokenInformation tokenInfo = currentTokens.get(ident);
    if (tokenInfo == null) {
      try {
        // 本地缓存未命中，从状态存储加载
        RouterRMTokenResponse response = federationFacade.getTokenByRouterStoreToken(ident);
        RouterStoreToken routerStoreToken = response.getRouterStoreToken();
        String tokenStr = routerStoreToken.getTokenInfo();
        // Base64解码得到字节数组
        byte[] tokenBytes = Base64.getUrlDecoder().decode(tokenStr);
        // 反序列化得到令牌信息
        tokenInfo = RouterDelegationTokenSupport.decodeDelegationTokenInformation(tokenBytes);
      } catch (Exception e) {
        LOG.error("Error retrieving tokenInfo [{}] from StateStore.", ident.getSequenceNumber(), e);
        throw new YarnRuntimeException(e);
      }
    }
    return tokenInfo;
  }

  @Override
  protected synchronized int getDelegationTokenSeqNum() {
    // 从状态存储获取当前最大代理令牌序列号
    return federationFacade.getDelegationTokenSeqNum();
  }

  @Override
  protected synchronized void setDelegationTokenSeqNum(int seqNum) {
    // 更新状态存储中的代理令牌序列号
    federationFacade.setDelegationTokenSeqNum(seqNum);
  }

  @Override
  protected synchronized int getCurrentKeyId() {
    // 从状态存储获取当前主密钥ID
    return federationFacade.getCurrentKeyId();
  }

  @Override
  protected synchronized int incrementCurrentKeyId() {
    // 主密钥ID递增，由状态存储保证一致性
    return federationFacade.incrementCurrentKeyId();
  }
}