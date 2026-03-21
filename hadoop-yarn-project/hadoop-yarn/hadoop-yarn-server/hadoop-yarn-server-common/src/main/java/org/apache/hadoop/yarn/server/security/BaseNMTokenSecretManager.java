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

package org.apache.hadoop.yarn.server.security;

import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NodeManager令牌密钥管理器基类，负责NM令牌的生成、密码计算和验证逻辑
 * 为AM访问NodeManager提供身份认证的密钥管理能力
 */
public class BaseNMTokenSecretManager extends
    SecretManager<NMTokenIdentifier> {

  private static final Logger LOG =
      LoggerFactory.getLogger(BaseNMTokenSecretManager.class);

  // 主密钥序列号，用于生成新主密钥的ID
  protected int serialNo = new SecureRandom().nextInt();

  // 读写锁，保护主密钥的并发访问，支持多线程读、单线程写
  protected final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  protected final Lock readLock = readWriteLock.readLock();
  protected final Lock writeLock = readWriteLock.writeLock();

  // 当前生效的主密钥数据
  protected MasterKeyData currentMasterKey;
  
  /**
   * 创建新的主密钥，更新序列号
   * @return 新生成的主密钥数据
   */
  protected MasterKeyData createNewMasterKey() {
    this.writeLock.lock();
    try {
      return new MasterKeyData(serialNo++, generateSecret());
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * 获取当前生效的主密钥
   * @return 当前主密钥对象
   */
  @Private
  public MasterKey getCurrentKey() {
    this.readLock.lock();
    try {
      return this.currentMasterKey.getMasterKey();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  protected byte[] createPassword(NMTokenIdentifier identifier) {
    LOG.debug("creating password for {} for user {} to run on NM {}",
        identifier.getApplicationAttemptId(),
        identifier.getApplicationSubmitter(), identifier.getNodeId());
    readLock.lock();
    try {
      // 使用当前主密钥为令牌标识符生成密码
      return createPassword(identifier.getBytes(),
          currentMasterKey.getSecretKey());
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public byte[] retrievePassword(NMTokenIdentifier identifier)
      throws org.apache.hadoop.security.token.SecretManager.InvalidToken {
    readLock.lock();
    try {
      // 调用内部方法验证令牌密码
      return retrivePasswordInternal(identifier, currentMasterKey);
    } finally {
      readLock.unlock();
    }
  }

  protected byte[] retrivePasswordInternal(NMTokenIdentifier identifier,
      MasterKeyData masterKey) {
    LOG.debug("retriving password for {} for user {} to run on NM {}",
        identifier.getApplicationAttemptId(),
        identifier.getApplicationSubmitter(), identifier.getNodeId());
    // 重新计算密码，和传入令牌中的密码比对完成验证
    return createPassword(identifier.getBytes(), masterKey.getSecretKey());
  }

  /**
   * It is required for RPC
   */
  @Override
  public NMTokenIdentifier createIdentifier() {
    return new NMTokenIdentifier();
  }
  
  /**
   * Helper function for creating NMTokens.
   *
   * @param applicationAttemptId application AttemptId.
   * @param nodeId node Id.
   * @param applicationSubmitter application Submitter.
   * @return NMToken.
   */
  public Token createNMToken(ApplicationAttemptId applicationAttemptId,
      NodeId nodeId, String applicationSubmitter) {
    byte[] password;
    NMTokenIdentifier identifier;
    
    this.readLock.lock();
    try {
      // 使用当前主密钥ID构造令牌标识符
      identifier =
          new NMTokenIdentifier(applicationAttemptId, nodeId,
              applicationSubmitter, this.currentMasterKey.getMasterKey()
                  .getKeyId());
      // 为标识符生成对应密码
      password = this.createPassword(identifier);
    } finally {
      this.readLock.unlock();
    }
    // 实例化并返回最终NM令牌对象
    return newInstance(password, identifier);
  }
  
  /**
   * 实例化NM令牌对象，设置正确的服务标识
   * @param password 令牌密码
   * @param identifier NM令牌标识符
   * @return 构造完成的NM令牌
   */
  public static Token newInstance(byte[] password,
      NMTokenIdentifier identifier) {
    NodeId nodeId = identifier.getNodeId();
    // RPC层要求令牌服务地址格式为ip:port
    InetSocketAddress addr =
        NetUtils.createSocketAddrForHost(nodeId.getHost(), nodeId.getPort());
    Token nmToken =
        Token.newInstance(identifier.getBytes(),
          NMTokenIdentifier.KIND.toString(), password, SecurityUtil
            .buildTokenService(addr).toString());
    return nmToken;
  }
}