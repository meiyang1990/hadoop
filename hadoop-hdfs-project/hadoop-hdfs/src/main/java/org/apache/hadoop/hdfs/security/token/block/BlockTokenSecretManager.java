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

package org.apache.hadoop.hdfs.security.token.block;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.ipc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Timer;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.HashMultiset;
import org.apache.hadoop.thirdparty.com.google.common.collect.Multiset;

/**
 * 文件概述：HDFS数据块令牌密钥管理器，负责数据块访问令牌的生成、验证和密钥轮换。
 * 支持主/工作两种模式：主模式用于NameNode生成和导出密钥，工作模式用于DataNode导入和使用密钥。
 * 核心功能：生成数据块访问令牌、验证令牌权限、管理密钥生命周期、支持数据传输加密密钥生成。
 * 
 * BlockTokenSecretManager can be instantiated in 2 modes, master mode
 * and worker mode. Master can generate new block keys and export block
 * keys to workers, while workers can only import and use block keys
 * received from master. Both master and worker can generate and verify
 * block tokens. Typically, master mode is used by NN and worker mode
 * is used by DN.
 */
@InterfaceAudience.Private
public class BlockTokenSecretManager extends
    SecretManager<BlockTokenIdentifier> {
  public static final Logger LOG =
      LoggerFactory.getLogger(BlockTokenSecretManager.class);

  public static final Token<BlockTokenIdentifier> DUMMY_TOKEN = new Token<BlockTokenIdentifier>();

  private final boolean isMaster;

  /**
   * keyUpdateInterval is the interval that NN updates its block keys. It should
   * be set long enough so that all live DN's and Balancer should have sync'ed
   * their block keys with NN at least once during each interval.
   */
  private long keyUpdateInterval;
  private volatile long tokenLifetime;
  private int serialNo;
  private BlockKey currentKey;
  private BlockKey nextKey;
  private final Map<Integer, BlockKey> allKeys;
  private String blockPoolId;
  private final String encryptionAlgorithm;

  private final int intRange;
  private final int nnRangeStart;
  private final boolean useProto;

  private final boolean shouldWrapQOP;

  private final SecureRandom nonceGenerator = new SecureRandom();

  /**
   * Timer object for querying the current time. Separated out for
   * unit testing.
   */
  private Timer timer;

  /**
   * 工作模式构造函数，用于DataNode侧创建密钥管理器
   *
   * @param keyUpdateInterval 密钥更新间隔
   * @param tokenLifetime 单个令牌的有效期
   * @param blockPoolId 块池ID
   * @param encryptionAlgorithm 加密算法名称
   * @param useProto 是否使用protobuf格式令牌
   */
  public BlockTokenSecretManager(long keyUpdateInterval,
      long tokenLifetime, String blockPoolId, String encryptionAlgorithm,
      boolean useProto) {
    this(false, keyUpdateInterval, tokenLifetime, blockPoolId,
        encryptionAlgorithm, 0, 1, useProto, false);
  }

  /**
   * 主模式构造函数，用于NameNode侧创建单NameNode场景的密钥管理器
   * 
   * @param keyUpdateInterval 密钥更新间隔
   * @param tokenLifetime 单个令牌的有效期
   * @param nnIndex 当前NameNode在HA集群中的索引
   * @param numNNs HA集群中NameNode的总数量
   * @param blockPoolId 块池ID
   * @param encryptionAlgorithm 加密算法名称
   * @param useProto 是否使用protobuf格式令牌
   */
  public BlockTokenSecretManager(long keyUpdateInterval,
      long tokenLifetime, int nnIndex, int numNNs, String blockPoolId,
      String encryptionAlgorithm, boolean useProto) {
    this(keyUpdateInterval, tokenLifetime, nnIndex, numNNs,
        blockPoolId, encryptionAlgorithm, useProto, false);
  }

  /**
   * 主模式构造函数，支持QOP封装配置，用于NameNode侧创建HA集群场景的密钥管理器
   * 
   * @param keyUpdateInterval 密钥更新间隔
   * @param tokenLifetime 单个令牌的有效期
   * @param nnIndex 当前NameNode在HA集群中的索引
   * @param numNNs HA集群中NameNode的总数量
   * @param blockPoolId 块池ID
   * @param encryptionAlgorithm 加密算法名称
   * @param useProto 是否使用protobuf格式令牌
   * @param shouldWrapQOP 是否在块访问令牌中封装QOP信息
   */
  public BlockTokenSecretManager(long keyUpdateInterval,
      long tokenLifetime, int nnIndex, int numNNs,  String blockPoolId,
      String encryptionAlgorithm, boolean useProto, boolean shouldWrapQOP) {
    this(true, keyUpdateInterval, tokenLifetime, blockPoolId,
        encryptionAlgorithm, nnIndex, numNNs, useProto, shouldWrapQOP);
    Preconditions.checkArgument(nnIndex >= 0);
    Preconditions.checkArgument(numNNs > 0);
  }

  /**
   * 通用私有构造函数，根据模式创建块令牌密钥管理器实例，分配密钥序列号范围避免HA场景冲突
   *
   * @param isMaster 是否为主模式
   * @param keyUpdateInterval 密钥更新间隔
   * @param tokenLifetime 单个令牌的有效期
   * @param blockPoolId 块池ID
   * @param encryptionAlgorithm 加密算法名称
   * @param nnIndex 当前NameNode在HA集群中的索引
   * @param numNNs HA集群中NameNode总数量
   * @param useProto 是否使用protobuf格式令牌
   * @param shouldWrapQOP 是否在块访问令牌中封装QOP信息
   */
  private BlockTokenSecretManager(boolean isMaster, long keyUpdateInterval,
      long tokenLifetime, String blockPoolId, String encryptionAlgorithm,
      int nnIndex, int numNNs, boolean useProto, boolean shouldWrapQOP) {
    // 为每个NameNode划分独立的序列号区间，避免HA场景下序列号冲突
    this.intRange = Integer.MAX_VALUE / numNNs;
    this.nnRangeStart = intRange * nnIndex;
    this.isMaster = isMaster;
    this.keyUpdateInterval = keyUpdateInterval;
    this.tokenLifetime = tokenLifetime;
    this.allKeys = new HashMap<Integer, BlockKey>();
    this.blockPoolId = blockPoolId;
    this.encryptionAlgorithm = encryptionAlgorithm;
    this.useProto = useProto;
    this.shouldWrapQOP = shouldWrapQOP;
    this.timer = new Timer();
    setSerialNo(new SecureRandom().nextInt(Integer.MAX_VALUE));
    LOG.info("Block token key range: [{}, {})",
        nnRangeStart, nnRangeStart + intRange);
    generateKeys();
  }

  @VisibleForTesting
  public synchronized void setSerialNo(int nextNo) {
    // 根据区间偏移计算最终序列号，保证落在当前NameNode分配的范围内
    this.serialNo = (nextNo % intRange) + (nnRangeStart);
    assert serialNo >= nnRangeStart && serialNo < (nnRangeStart + intRange) :
      "serialNo " + serialNo + " is not in the designated range: [" +
      nnRangeStart + ", " + (nnRangeStart + intRange) + ")";
  }

  public void setBlockPoolId(String blockPoolId) {
    this.blockPoolId = blockPoolId;
  }

  /** 初始化生成当前密钥和下一个密钥，仅主模式执行 */
  private synchronized void generateKeys() {
    if (!isMaster) {
      return;
    }
    /*
     * Need to set estimated expiry dates for currentKey and nextKey so that if
     * NN crashes, DN can still expire those keys. NN will stop using the newly
     * generated currentKey after the first keyUpdateInterval, however it may
     * still be used by DN and Balancer to generate new tokens before they get a
     * chance to sync their keys with NN. Since we require keyUpdInterval to be
     * long enough so that all live DN's and Balancer will sync their keys with
     * NN at least once during the period, the estimated expiry date for
     * currentKey is set to now() + 2 * keyUpdateInterval + tokenLifetime.
     * Similarly, the estimated expiry date for nextKey is one keyUpdateInterval
     * more.
     */
    // 生成当前密钥，预估计过期时间，保证NN重启后DN仍能正确过期旧密钥
    setSerialNo(serialNo + 1);
    currentKey = new BlockKey(serialNo, timer.now() + 2
        * keyUpdateInterval + tokenLifetime, generateSecret());
    // 预先生成下一个密钥，用于密钥轮换
    setSerialNo(serialNo + 1);
    nextKey = new BlockKey(serialNo, timer.now() + 3
        * keyUpdateInterval + tokenLifetime, generateSecret());
    // 将两个密钥添加到密钥集合
    allKeys.put(currentKey.getKeyId(), currentKey);
    allKeys.put(nextKey.getKeyId(), nextKey);
  }

  /** 导出所有块密钥，仅主模式使用，供DN同步拉取 */
  public synchronized ExportedBlockKeys exportKeys() {
    if (!isMaster) {
      return null;
    }
    LOG.debug("Exporting access keys");
    return new ExportedBlockKeys(true, keyUpdateInterval, tokenLifetime,
        currentKey, allKeys.values().toArray(new BlockKey[0]));
  }

  /** 移除已过期的密钥，清理本地密钥集合 */
  private synchronized void removeExpiredKeys() {
    long now = timer.now();
    // 遍历所有密钥，移除过期密钥
    for (Iterator<Map.Entry<Integer, BlockKey>> it = allKeys.entrySet()
        .iterator(); it.hasNext();) {
      Map.Entry<Integer, BlockKey> e = it.next();
      if (e.getValue().getExpiryDate() < now) {
        it.remove();
      }
    }
  }

  public synchronized void addKeys(ExportedBlockKeys exportedKeys) throws IOException {
    addKeys(exportedKeys, true);
  }

  /**
   * 导入从主节点获取的块密钥，仅工作模式使用
   */
  public synchronized void addKeys(ExportedBlockKeys exportedKeys,
      boolean updateCurrentKey) throws IOException {
    if (isMaster || exportedKeys == null) {
      return;
    }
    LOG.info("Setting block keys. BlockPool = {} .", blockPoolId);
    // 先清理本地已过期密钥
    removeExpiredKeys();
    // 更新当前密钥
    if (updateCurrentKey || currentKey == null) {
      this.currentKey = exportedKeys.getCurrentKey();
    }
    // 添加所有接收到的密钥到本地集合
    BlockKey[] receivedKeys = exportedKeys.getAllKeys();
    for (int i = 0; i < receivedKeys.length; i++) {
      if (receivedKeys[i] != null) {
        this.allKeys.put(receivedKeys[i].getKeyId(), receivedKeys[i]);
      }
    }
  }

  /**
   * 如果更新时间超过间隔，触发密钥更新，仅主模式使用
   * @return true 如果密钥成功更新
   */
  public synchronized boolean updateKeys(final long updateTime) throws IOException {
    if (updateTime > keyUpdateInterval) {
      return updateKeys();
    }
    return false;
  }

  /**
   * 执行密钥轮换更新，仅主模式使用
   */
  synchronized boolean updateKeys() throws IOException {
    if (!isMaster) {
      return false;
    }

    LOG.info("Updating block keys");
    // 清理过期密钥
    removeExpiredKeys();
    // 更新即将退役的当前密钥的过期时间
    allKeys.put(currentKey.getKeyId(), new BlockKey(currentKey.getKeyId(),
        timer.now() + keyUpdateInterval + tokenLifetime,
        currentKey.getKey()));
    // 将预先生成的nextKey提升为当前密钥，更新其过期时间
    currentKey = new BlockKey(nextKey.getKeyId(), timer.now()
        + 2 * keyUpdateInterval + tokenLifetime, nextKey.getKey());
    allKeys.put(currentKey.getKeyId(), currentKey);
    // 生成新的下一代密钥，为下一次轮换做准备
    setSerialNo(serialNo + 1);
    nextKey = new BlockKey(serialNo, timer.now() + 3
        * keyUpdateInterval + tokenLifetime, generateSecret());
    allKeys.put(nextKey.getKeyId(), nextKey);
    return true;
  }

  /** 为当前请求用户生成指定数据块的访问令牌 */
  public Token<BlockTokenIdentifier> generateToken(ExtendedBlock block,
      EnumSet<BlockTokenIdentifier.AccessMode> modes,
      StorageType[] storageTypes, String[] storageIds) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String userID = (ugi == null ? null : ugi.getShortUserName());
    return generateToken(userID, block, modes, storageTypes, storageIds);
  }

  /** 为指定用户生成指定数据块的访问令牌 */
  public Token<BlockTokenIdentifier> generateToken(String userId,
      ExtendedBlock block, EnumSet<BlockTokenIdentifier.AccessMode> modes,
      StorageType[] storageTypes, String[] storageIds) {
    // 创建块令牌标识符，填充用户、块、访问权限、存储信息
    BlockTokenIdentifier id = new BlockTokenIdentifier(userId, block
        .getBlockPoolId(), block.getBlockId(), modes, storageTypes,
        storageIds, useProto);
    // 如果需要封装QOP信息，从当前RPC连接获取QOP并写入令牌
    if (shouldWrapQOP) {
      String qop = Server.getAuxiliaryPortEstablishedQOP();
      if (qop != null) {
        id.setHandshakeMsg(qop.getBytes(StandardCharsets.UTF_8));
      }
    }
    // 使用当前密钥生成令牌密码，返回完整令牌
    return new Token<BlockTokenIdentifier>(id, this);
  }

  /**
   * 检查访问是否允许，不验证令牌密码（密码已在RPC层验证），用户名如果为null则不检查。
   * 验证用户、块ID、有效期、访问模式，并检查存储类型和存储ID是否匹配。
   *
   * @param id 块令牌标识符
   * @param userId 请求用户名
   * @param block 请求访问的数据块
   * @param mode 请求的访问模式
   * @param storageTypes 请求使用的存储类型
   * @param storageIds 请求使用的存储ID
   * @throws InvalidToken 访问不被允许时抛出异常
   */
  public void checkAccess(BlockTokenIdentifier id, String userId,
      ExtendedBlock block, BlockTokenIdentifier.AccessMode mode,
      StorageType[] storageTypes, String[] storageIds) throws InvalidToken {
    checkAccess(id, userId, block, mode);
    if (ArrayUtils.isNotEmpty(storageTypes)) {
      checkAccess(id.getStorageTypes(), storageTypes, "StorageTypes");
    }
    if (ArrayUtils.isNotEmpty(storageIds)) {
      checkAccess(id.getStorageIds(), storageIds, "StorageIDs");
    }
  }

  /**
   * 检查访问是否允许，不验证令牌密码（密码已在RPC层验证），用户名如果为null则不检查。
   * 验证用户、块ID、有效期、访问模式，并检查存储类型是否匹配。
   *
   * @param id 块令牌标识符
   * @param userId 请求用户名
   * @param block 请求访问的数据块
   * @param mode 请求的访问模式
   * @param storageTypes 请求使用的存储类型
   * @throws InvalidToken 访问不被允许时抛出异常
   */
  public void checkAccess(BlockTokenIdentifier id, String userId,
      ExtendedBlock block, BlockTokenIdentifier.AccessMode mode,
      StorageType[] storageTypes) throws InvalidToken {
    checkAccess(id, userId, block, mode);
    if (ArrayUtils.is