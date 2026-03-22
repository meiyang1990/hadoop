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

package org.apache.hadoop.hdfs.security.token.delegation;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SecretManagerSection;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.protobuf.ByteString;

/**
 * HDFS 特定的代理令牌密钥管理器，负责生成和验证每个代理令牌的密码
 */
@InterfaceAudience.Private
public class DelegationTokenSecretManager
    extends AbstractDelegationTokenSecretManager<DelegationTokenIdentifier> {

  private static final Logger LOG = LoggerFactory
      .getLogger(DelegationTokenSecretManager.class);
  
  // 关联的FSNamesystem对象，用于操作命名空间和日志记录
  private final FSNamesystem namesystem;
  // 兼容旧版本fsimage序列化工具实例
  private final SerializerCompat serializerCompat = new SerializerCompat();

  /**
   * 构造代理令牌密钥管理器
   * @param delegationKeyUpdateInterval 滚动生成新密钥的间隔（毫秒）
   * @param delegationTokenMaxLifetime 代理令牌最大生命周期（毫秒）
   * @param delegationTokenRenewInterval 令牌必须续订的间隔（毫秒）
   * @param delegationTokenRemoverScanInterval 扫描过期令牌的间隔（毫秒）
   * @param namesystem 关联的FSNamesystem实例
   */
  public DelegationTokenSecretManager(long delegationKeyUpdateInterval,
      long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
      long delegationTokenRemoverScanInterval, FSNamesystem namesystem) {
    this(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
        delegationTokenRenewInterval, delegationTokenRemoverScanInterval, false,
        namesystem);
  }

  /**
   * 构造代理令牌密钥管理器
   * @param delegationKeyUpdateInterval 滚动生成新密钥的间隔（毫秒）
   * @param delegationTokenMaxLifetime 代理令牌最大生命周期（毫秒）
   * @param delegationTokenRenewInterval 令牌必须续订的间隔（毫秒）
   * @param delegationTokenRemoverScanInterval 扫描过期令牌的间隔（毫秒）
   * @param storeTokenTrackingId 是否存储令牌追踪ID
   * @param namesystem 关联的FSNamesystem实例
   */
  public DelegationTokenSecretManager(long delegationKeyUpdateInterval,
      long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
      long delegationTokenRemoverScanInterval, boolean storeTokenTrackingId,
      FSNamesystem namesystem) {
    super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
        delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    this.namesystem = namesystem;
    this.storeTokenTrackingId = storeTokenTrackingId;
  }

  @Override //SecretManager
  public DelegationTokenIdentifier createIdentifier() {
    return new DelegationTokenIdentifier();
  }
  
  @Override
  public byte[] retrievePassword(
      DelegationTokenIdentifier identifier) throws InvalidToken {
    try {
      // 检查当前节点是否允许读操作，HA备节点会抛出StandbyException
      namesystem.checkOperation(OperationCategory.READ);
    } catch (StandbyException se) {
      // 将StandbyException包装为InvalidToken抛出，RPC服务端会解包还原原异常
      InvalidToken wrappedStandby = new InvalidToken("StandbyException");
      wrappedStandby.initCause(se);
      throw wrappedStandby;
    }
    return super.retrievePassword(identifier);
  }
  
  @Override
  public byte[] retriableRetrievePassword(DelegationTokenIdentifier identifier)
      throws InvalidToken, StandbyException, RetriableException, IOException {
    // 检查当前节点是否允许读操作
    namesystem.checkOperation(OperationCategory.READ);
    try {
      return super.retrievePassword(identifier);
    } catch (InvalidToken it) {
      // 如果命名空间正在切换到激活状态，可能编辑日志还未应用，让客户端重试
      if (namesystem.inTransitionToActive()) {
        throw new RetriableException(it);
      } else {
        throw it;
      }
    }
  }
  
  /**
   * 根据令牌标识符获取令牌过期时间
   * 
   * @param dtId 代理令牌标识符
   * @return 令牌的过期时间
   * @throws IOException 当找不到对应令牌时抛出IO异常
   */
  public synchronized long getTokenExpiryTime(
      DelegationTokenIdentifier dtId) throws IOException {
    DelegationTokenInformation info = currentTokens.get(dtId);
    if (info != null) {
      return info.getRenewDate();
    } else {
      throw new IOException("No delegation token found for this identifier");
    }
  }

  /**
   * 从旧版本fsimage加载密钥管理器状态
   * 
   * @param in fsimage输入流
   * @throws IOException 加载失败时抛出IO异常
   */
  public synchronized void loadSecretManagerStateCompat(DataInput in)
      throws IOException {
    if (running) {
      // 安全检查：运行中的密钥管理器不允许加载状态
      throw new IOException(
          "Can't load state from image in a running SecretManager.");
    }
    serializerCompat.load(in);
  }

  /**
   * 保存密钥管理器持久化状态的数据容器
   */
  public static class SecretManagerState {
    public final SecretManagerSection section;
    public final List<SecretManagerSection.DelegationKey> keys;
    public final List<SecretManagerSection.PersistToken> tokens;

    /**
     * 构造状态容器
     * @param s  protobuf格式的密钥管理器根section
     * @param keys  代理密钥列表
     * @param tokens 持久化代理令牌列表
     */
    public SecretManagerState(
        SecretManagerSection s,
        List<SecretManagerSection.DelegationKey> keys,
        List<SecretManagerSection.PersistToken> tokens) {
      this.section = s;
      this.keys = keys;
      this.tokens = tokens;
    }
  }

  /**
   * 从protobuf格式的状态对象加载密钥管理器状态
   * @param state  从fsimage解析出的状态对象
   * @param counter 启动进度计数器
   * @throws IOException 运行中加载时抛出异常
   */
  public synchronized void loadSecretManagerState(SecretManagerState state, Counter counter)
      throws IOException {
    Preconditions.checkState(!running,
        "Can't load state from image in a running SecretManager.");

    currentId = state.section.getCurrentId();
    delegationTokenSequenceNumber = state.section.getTokenSequenceNumber();
    // 加载所有代理密钥
    for (SecretManagerSection.DelegationKey k : state.keys) {
      addKey(new DelegationKey(k.getId(), k.getExpiryDate(), k.hasKey() ? k
          .getKey().toByteArray() : null));
    }

    // 加载所有持久化代理令牌
    for (SecretManagerSection.PersistToken t : state.tokens) {
      DelegationTokenIdentifier id = new DelegationTokenIdentifier(new Text(
          t.getOwner()), new Text(t.getRenewer()), new Text(t.getRealUser()));
      id.setIssueDate(t.getIssueDate());
      id.setMaxDate(t.getMaxDate());
      id.setSequenceNumber(t.getSequenceNumber());
      id.setMasterKeyId(t.getMasterKeyId());
      addPersistedDelegationToken(id, t.getExpiryDate());
      counter.increment();
    }
  }

  /**
   * 将密钥管理器状态保存到旧版本格式的fsimage
   *
   * @param out  fsimage输出流
   * @param sdPath 存储目录路径，用于启动进度显示
   * @throws IOException 保存失败抛出IO异常
   */
  public synchronized void saveSecretManagerStateCompat(DataOutputStream out,
      String sdPath) throws IOException {
    serializerCompat.save(out, sdPath);
  }

  /**
   * 将当前密钥管理器状态转换为protobuf格式对象用于持久化
   * @return 包含所有状态的SecretManagerState对象
   */
  public synchronized SecretManagerState saveSecretManagerState() {
    SecretManagerSection s = SecretManagerSection.newBuilder()
        .setCurrentId(currentId)
        .setTokenSequenceNumber(delegationTokenSequenceNumber)
        .setNumKeys(allKeys.size()).setNumTokens(currentTokens.size()).build();
    ArrayList<SecretManagerSection.DelegationKey> keys = Lists
        .newArrayListWithCapacity(allKeys.size());
    ArrayList<SecretManagerSection.PersistToken> tokens = Lists
        .newArrayListWithCapacity(currentTokens.size());

    // 序列化所有代理密钥
    for (DelegationKey v : allKeys.values()) {
      SecretManagerSection.DelegationKey.Builder b = SecretManagerSection.DelegationKey
          .newBuilder().setId(v.getKeyId()).setExpiryDate(v.getExpiryDate());
      if (v.getEncodedKey() != null) {
        b.setKey(ByteString.copyFrom(v.getEncodedKey()));
      }
      keys.add(b.build());
    }

    // 序列化所有代理令牌
    for (Entry<DelegationTokenIdentifier, DelegationTokenInformation> e : currentTokens
        .entrySet()) {
      DelegationTokenIdentifier id = e.getKey();
      SecretManagerSection.PersistToken.Builder b = SecretManagerSection.PersistToken
          .newBuilder().setOwner(id.getOwner().toString())
          .setRenewer(id.getRenewer().toString())
          .setRealUser(id.getRealUser().toString())
          .setIssueDate(id.getIssueDate()).setMaxDate(id.getMaxDate())
          .setSequenceNumber(id.getSequenceNumber())
          .setMasterKeyId(id.getMasterKeyId())
          .setExpiryDate(e.getValue().getRenewDate());
      tokens.add(b.build());
    }

    return new SecretManagerState(s, keys, tokens);
  }

  /**
   * 从编辑日志或fsimage加载持久化代理令牌，仅在NameNode启动加载时使用
   * 
   * @param identifier  代理令牌标识符
   * @param expiryTime  令牌过期时间
   * @throws IOException 运行中添加或重复添加时抛出异常
   */
  public synchronized void addPersistedDelegationToken(
      DelegationTokenIdentifier identifier, long expiryTime) throws IOException {
    if (running) {
      // 安全检查：运行中的密钥管理器不允许添加持久化令牌
      throw new IOException(
          "Can't add persisted delegation token to a running SecretManager.");
    }
    int keyId = identifier.getMasterKeyId();
    DelegationKey dKey = allKeys.get(keyId);
    if (dKey == null) {
      LOG
          .warn("No KEY found for persisted identifier "
              + identifier.toString());
      return;
    }
    // 根据密钥生成令牌密码
    byte[] password = createPassword(identifier.getBytes(), dKey.getKey());
    // 更新最大序列号
    if (identifier.getSequenceNumber() > this.delegationTokenSequenceNumber) {
      this.delegationTokenSequenceNumber = identifier.getSequenceNumber();
    }
    if (currentTokens.get(identifier) == null) {
      currentTokens.put(identifier, new DelegationTokenInformation(expiryTime,
          password, getTrackingIdIfEnabled(identifier)));
    } else {
      throw new IOException(
          "Same delegation token being added twice; invalid entry in fsimage or editlogs");
    }
  }

  /**
   * 添加持久化主密钥，用于编辑日志回放
   * 
   * @param key 代理密钥对象
   * @throws IOException 从不抛出，保留接口签名
   */
  public synchronized void updatePersistedMasterKey(DelegationKey key)
      throws IOException {
    addKey(key);
  }
  
  /**
   * 更新缓存中令牌的续订信息，用于编辑日志回放
   * 
   * @param identifier  已续订令牌的标识符
   * @param expiryTime  新的过期时间（毫秒）
   * @throws IOException 运行中更新时抛出异常
   */
  public synchronized void updatePersistedTokenRenewal(
      DelegationTokenIdentifier identifier, long expiryTime) throws IOException {
    if (running) {
      // 安全检查：运行中的密钥管理器不允许更新持久化信息
      throw new IOException(
          "Can't update persisted delegation token renewal to a running SecretManager.");
    }
    DelegationTokenInformation info = null;
    info = currentTokens.get(identifier);
    if (info != null) {
      int keyId = identifier.getMasterKeyId();
      byte[] password = createPassword(identifier.getBytes(), allKeys
          .get(keyId).getKey());
      currentTokens.put(identifier, new DelegationTokenInformation(expiryTime,
          password, getTrackingIdIfEnabled(identifier)));
    }
  }

  /**
   * 从缓存中删除已取消的令牌，用于编辑日志回放
   *  
   *  @param identifier 已取消令牌的标识符
   *  @throws IOException 运行中更新时抛出异常
   */
  public synchronized void updatePersistedTokenCancellation(
      DelegationTokenIdentifier identifier) throws IOException {
    if (running) {
      // 安全检查：运行中的密钥管理器不允许更新持久化信息
      throw new IOException(
          "Can't update persisted delegation token renewal to a running SecretManager.");
    }
    currentTokens.remove(identifier);
  }
  
  /**
   * 获取当前存储的代理密钥数量
   * @return 代理密钥数量
   */
  public synchronized int getNumberOfKeys() {
    return allKeys.size();
  }

  /**
   * 调用FSNamesystem记录新主密钥到编辑日志
   */
  @Override //AbstractDelegationTokenManager
  protected void logUpdateMasterKey(DelegationKey key)
      throws IOException {
    try {
      // 获取FS命名空间读锁，可中断
      namesystem.readLockInterruptibly(RwLockMode.FS);
      try {
        // 加锁避免停止密钥管理器时被中断，防止损坏编辑日志
        synchronized (noInterruptsLock) {
          if (Thread.currentThread().isInterrupted()) {
            return; // 保留中断标志，让密钥管理器退出
          }
          namesystem.logUpdateMasterKey(key);
        }
      } finally {
        // 释放读锁
        namesystem.readUnlock(RwLockMode.FS, "logUpdateMasterKey");
      }
    } catch (InterruptedException ie) {
      // 保留中断状态，密钥管理器下次休眠会检测到并退出
      Thread.currentThread().interrupt();
    }
  }
  
  @Override //AbstractDelegationTokenManager
  protected void logExpireToken(final DelegationTokenIdentifier dtId)
      throws IOException {
    try {
      // 获取FS命名空间读锁，可中断
      namesystem.readLockInterruptibly(RwLockMode.FS);
      try {
        // 加锁避免停止密钥管理器时被中断，防止损坏编辑日志
        synchronized (noInterruptsLock) {
          if (Thread.currentThread().isInterrupted()) {
            return; // 保留中断标志，让密钥管理器退出
          }
          namesystem.logExpireDelegationToken(dtId);
        }
      } finally {
        // 释放读锁
        namesystem.readUnlock(RwLockMode.FS, "logExpireToken");
      }
    } catch (InterruptedException ie) {
      // 保留中断状态，密钥管理器下次休眠会检测到并退出
      Thread.currentThread().interrupt();