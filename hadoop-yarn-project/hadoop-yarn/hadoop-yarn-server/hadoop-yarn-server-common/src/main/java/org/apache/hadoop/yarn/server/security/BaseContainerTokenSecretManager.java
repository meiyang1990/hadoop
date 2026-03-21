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

import java.security.SecureRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 容器令牌密钥管理器基类，提供容器令牌生成与验证的核心逻辑。
 * ResourceManager和NodeManager都继承此类，因此放置在yarn-server-common公共模块中。
 * 
 */
public class BaseContainerTokenSecretManager extends
    SecretManager<ContainerTokenIdentifier> {

  private static final Logger LOG =
      LoggerFactory.getLogger(BaseContainerTokenSecretManager.class);

  // 主密钥序列号，使用安全随机数初始化
  protected int serialNo = new SecureRandom().nextInt();

  // 读写锁，保护主密钥并发访问
  protected final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  protected final Lock readLock = readWriteLock.readLock();
  protected final Lock writeLock = readWriteLock.writeLock();

  /**
   * 当前生效的主密钥。ResourceManager重启时需要持久化恢复此密钥，
   * NodeManager从ResourceManager获取此密钥，用于验证容器令牌。
   */
  protected MasterKeyData currentMasterKey;

  // 容器令牌过期时间间隔
  protected final long containerTokenExpiryInterval;

  /**
   * 构造函数，从配置中读取容器令牌过期时间。
   * @param conf YARN配置对象
   */
  public BaseContainerTokenSecretManager(Configuration conf) {
    this.containerTokenExpiryInterval =
        conf.getInt(YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS);
  }

  // 需要写锁保护序列号递增等操作
  protected MasterKeyData createNewMasterKey() {
    this.writeLock.lock();
    try {
      return new MasterKeyData(serialNo++, generateSecret());
    } finally {
      this.writeLock.unlock();
    }
  }
  
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
  public byte[] createPassword(ContainerTokenIdentifier identifier) {
    LOG.debug("Creating password for {} for user {} to be run on NM {}",
        identifier.getContainerID(), identifier.getUser(),
        identifier.getNmHostAddress());
    this.readLock.lock();
    try {
      return createPassword(identifier.getBytes(),
        this.currentMasterKey.getSecretKey());
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public byte[] retrievePassword(ContainerTokenIdentifier identifier)
      throws SecretManager.InvalidToken {
    this.readLock.lock();
    try {
      return retrievePasswordInternal(identifier, this.currentMasterKey);
    } finally {
      this.readLock.unlock();
    }
  }

  protected byte[] retrievePasswordInternal(ContainerTokenIdentifier identifier,
      MasterKeyData masterKey)
      throws org.apache.hadoop.security.token.SecretManager.InvalidToken {
    LOG.debug("Retrieving password for {} for user {} to be run on NM {}",
        identifier.getContainerID(), identifier.getUser(),
        identifier.getNmHostAddress());
    return createPassword(identifier.getBytes(), masterKey.getSecretKey());
  }

  /**
   * Used by the RPC layer.
   */
  @Override
  public ContainerTokenIdentifier createIdentifier() {
    return new ContainerTokenIdentifier();
  }
}