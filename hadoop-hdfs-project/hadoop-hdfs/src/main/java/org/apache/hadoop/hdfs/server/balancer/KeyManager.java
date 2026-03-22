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
package org.apache.hadoop.hdfs.server.balancer;

import java.io.Closeable;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件概要：HDFS数据均衡服务的密钥和令牌管理器，负责管理块令牌生成、密钥同步更新和数据传输加密密钥生成
 *
 * 该类为Balancer提供块访问令牌生成能力，定期从NameNode同步更新块密钥，同时支持数据传输加密密钥生成，
 * 保障Balancer在数据块移动过程中的身份认证和数据传输安全。
 */
@InterfaceAudience.Private
public class KeyManager implements Closeable, DataEncryptionKeyFactory {
  private static final Logger LOG = LoggerFactory.getLogger(KeyManager.class);

  /** 与NameNode的RPC协议接口，用于获取块密钥信息 */
  private final NamenodeProtocol namenode;

  /** 是否启用块令牌认证 */
  private final boolean isBlockTokenEnabled;
  /** 是否启用数据传输加密 */
  private final boolean encryptDataTransfer;
  /** 控制密钥更新线程是否继续运行 */
  private boolean shouldRun;

  /** 本地块令牌密钥管理器，维护从NameNode同步的块密钥 */
  private final BlockTokenSecretManager blockTokenSecretManager;
  /** 定期更新块密钥的后台线程封装 */
  private final BlockKeyUpdater blockKeyUpdater;
  /** 当前使用的数据加密密钥 */
  private DataEncryptionKey encryptionKey;
  /**
   * 时间查询对象，分离实现以支持单元测试注入模拟时间
   */
  private Timer timer;

  /**
   * 构造KeyManager，从NameNode获取初始块密钥信息并初始化本地密钥管理器
   * 
   * @param blockpoolID 块池ID
   * @param namenode 与NameNode通信的RPC协议
   * @param encryptDataTransfer 是否启用数据传输加密
   * @param conf Hadoop配置对象
   * @throws IOException 从NameNode获取块密钥失败时抛出
   */
  public KeyManager(String blockpoolID, NamenodeProtocol namenode,
      boolean encryptDataTransfer, Configuration conf) throws IOException {
    this.namenode = namenode;
    this.encryptDataTransfer = encryptDataTransfer;
    this.timer = new Timer();

    final ExportedBlockKeys keys = namenode.getBlockKeys();
    this.isBlockTokenEnabled = keys.isBlockTokenEnabled();
    if (isBlockTokenEnabled) {
      long updateInterval = keys.getKeyUpdateInterval();
      long tokenLifetime = keys.getTokenLifetime();
      LOG.info("Block token params received from NN: update interval="
          + StringUtils.formatTime(updateInterval)
          + ", token lifetime=" + StringUtils.formatTime(tokenLifetime));
      String encryptionAlgorithm = conf.get(
          DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY);
      final boolean enableProtobuf = conf.getBoolean(
          DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_PROTOBUF_ENABLE,
          DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_PROTOBUF_ENABLE_DEFAULT);
      this.blockTokenSecretManager = new BlockTokenSecretManager(
          updateInterval, tokenLifetime, blockpoolID, encryptionAlgorithm,
          enableProtobuf);
      this.blockTokenSecretManager.addKeys(keys);

      // 同步块密钥的频率高于NameNode更新自身密钥的频率，保证本地密钥最新
      this.blockKeyUpdater = new BlockKeyUpdater(updateInterval / 4);
      this.shouldRun = true;
    } else {
      this.blockTokenSecretManager = null;
      this.blockKeyUpdater = null;
    }
  }
  
  /**
   * 启动后台线程，定期从NameNode同步更新块密钥
   */
  public void startBlockKeyUpdater() {
    if (blockKeyUpdater != null) {
      blockKeyUpdater.daemon.start();
    }
  }

  /**
   * 为指定数据块生成访问令牌，供Balancer移动数据块时使用
   * 
   * @param eb 扩展数据块信息
   * @param storageTypes 存储类型数组
   * @param storageIds 存储ID数组
   * @return 数据块访问令牌
   * @throws IOException 块令牌未启用或更新线程未运行时抛出
   */
  public Token<BlockTokenIdentifier> getAccessToken(ExtendedBlock eb,
      StorageType[] storageTypes, String[] storageIds) throws IOException {
    if (!isBlockTokenEnabled) {
      return BlockTokenSecretManager.DUMMY_TOKEN;
    } else {
      if (!shouldRun) {
        throw new IOException(
            "Cannot get access token since BlockKeyUpdater is not running");
      }
      return blockTokenSecretManager.generateToken(null, eb,
          EnumSet.of(BlockTokenIdentifier.AccessMode.REPLACE,
              BlockTokenIdentifier.AccessMode.COPY), storageTypes, storageIds);
    }
  }

  @Override
  public DataEncryptionKey newDataEncryptionKey() {
    if (encryptDataTransfer) {
      synchronized (this) {
        if (encryptionKey == null ||
            encryptionKey.expiryDate < timer.now()) {
          // 加密密钥(EK)由块密钥(BK)生成。检查EK是否过期，如果过期则使用当前BK生成新EK，否则继续使用之前生成的EK。
          // 必须保证EK未过期时，生成EK所用的BK不会过期被删除，因为BlockTokenSecretManager会使用同一个BK重新生成EK。
          // 当前实现保证：当EK未过期（在tokenLifetime范围内），生成它的BK在过期删除前至少还有keyUpdateInterval的生命周期，
          // 详情参见BlockTokenSecretManager。
          LOG.debug("Generating new data encryption key because current key "
              + (encryptionKey == null ?
              "is null." : "expired on " + encryptionKey.expiryDate));
          encryptionKey = blockTokenSecretManager.generateDataEncryptionKey();
        }
        return encryptionKey;
      }
    } else {
      return null;
    }
  }

  @Override
  public void close() {
    shouldRun = false;
    try {
      if (blockKeyUpdater != null) {
        blockKeyUpdater.daemon.interrupt();
      }
    } catch(Exception e) {
      LOG.warn("Exception shutting down access key updater thread", e);
    }
  }

  /**
   * 定期从NameNode同步更新块密钥的后台任务
   *
   * 核心职责：在后台线程中周期性请求NameNode获取最新的块密钥，更新到本地BlockTokenSecretManager，
   * 保证Balancer生成的块令牌始终有效，避免因为密钥过期导致块访问失败。
   */
  class BlockKeyUpdater implements Runnable, Closeable {
    private final Daemon daemon = new Daemon(this);
    private final long sleepInterval;

    BlockKeyUpdater(final long sleepInterval) {
      this.sleepInterval = sleepInterval;
      LOG.info("Update block keys every " + StringUtils.formatTime(sleepInterval));
    }

    @Override
    public void run() {
      try {
        while (shouldRun) {
          try {
            // 从NameNode获取最新块密钥并更新到本地管理器
            blockTokenSecretManager.addKeys(namenode.getBlockKeys());
          } catch (IOException e) {
            LOG.error("Failed to set keys", e);
          }
          Thread.sleep(sleepInterval);
        }
      } catch (InterruptedException e) {
        LOG.debug("InterruptedException in block key updater thread", e);
      } catch (Throwable e) {
        LOG.error("Exception in block key updater thread", e);
        shouldRun = false;
      }
    }

    @Override
    public void close() throws IOException {
      try {
        daemon.interrupt();
      } catch(Exception e) {
        LOG.warn("Exception shutting down key updater thread", e);
      }
    }
  }
}