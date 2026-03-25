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

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier.AccessMode;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.StorageType;

/**
 * 文件级注释：块池级块令牌密钥管理器，为每个块池维护独立的块令牌密钥管理器，
 * 将请求路由到对应块池的密钥管理器实例，支持HDFS联邦多块池场景下的块令牌管理。
 * 
 * 为每个块池维护独立的{@link BlockTokenSecretManager}实例，根据块池ID将请求路由到对应实例。
 */
public class BlockPoolTokenSecretManager extends
    SecretManager<BlockTokenIdentifier> {
  
  // 存储块池ID到对应块令牌密钥管理器的映射
  private final Map<String, BlockTokenSecretManager> map =
      new ConcurrentHashMap<>();

  /**
   * 添加一个块池及其对应的块令牌密钥管理器到映射表
   * @param bpid 块池ID
   * @param secretMgr 对应块池的块令牌密钥管理器
   */
  public void addBlockPool(String bpid, BlockTokenSecretManager secretMgr) {
    map.put(bpid, secretMgr);
  }

  /**
   * 根据块池ID获取对应的块令牌密钥管理器，仅用于测试
   * @param bpid 块池ID
   * @return 对应块池的块令牌密钥管理器
   */
  @VisibleForTesting
  public BlockTokenSecretManager get(String bpid) {
    BlockTokenSecretManager secretMgr = map.get(bpid);
    if (secretMgr == null) {
      throw new IllegalArgumentException(
          "Block pool " + bpid + " is not found");
    }
    return secretMgr;
  }
  
  /**
   * 检查指定块池是否已注册到管理器
   * @param bpid 块池ID
   * @return 如果块池已注册返回true，否则返回false
   */
  public boolean isBlockPoolRegistered(String bpid) {
    return map.containsKey(bpid);
  }

  /**
   * 创建空的块令牌标识符实例
   * @return 空的BlockTokenIdentifier实例
   */
  @Override
  public BlockTokenIdentifier createIdentifier() {
    return new BlockTokenIdentifier();
  }

  @Override
  public byte[] createPassword(BlockTokenIdentifier identifier) {
    // 路由到对应块池的密钥管理器生成密码
    return get(identifier.getBlockPoolId()).createPassword(identifier);
  }

  @Override
  public byte[] retrievePassword(BlockTokenIdentifier identifier)
      throws InvalidToken {
    // 路由到对应块池的密钥管理器获取密码
    return get(identifier.getBlockPoolId()).retrievePassword(identifier);
  }

  /**
   * 检查块令牌访问权限，路由到对应块池的权限检查逻辑
   * 详情见{@link BlockTokenSecretManager#checkAccess(BlockTokenIdentifier,
   *                String, ExtendedBlock, BlockTokenIdentifier.AccessMode,
   *                StorageType[], String[])}
   */
  public void checkAccess(BlockTokenIdentifier id, String userId,
      ExtendedBlock block, AccessMode mode,
      StorageType[] storageTypes, String[] storageIds)
      throws InvalidToken {
    get(block.getBlockPoolId()).checkAccess(id, userId, block, mode,
        storageTypes, storageIds);
  }

  /**
   * 检查块令牌访问权限，路由到对应块池的权限检查逻辑
   * 详情见{@link BlockTokenSecretManager#checkAccess(BlockTokenIdentifier,
   * String, ExtendedBlock, BlockTokenIdentifier.AccessMode,
   * StorageType[])}
   */
  public void checkAccess(BlockTokenIdentifier id, String userId,
      ExtendedBlock block, AccessMode mode, StorageType[] storageTypes)
      throws InvalidToken {
    get(block.getBlockPoolId()).checkAccess(id, userId, block, mode,
        storageTypes);
  }

  /**
   * 检查块令牌访问权限，路由到对应块池的权限检查逻辑
   * 详情见{@link BlockTokenSecretManager#checkAccess(BlockTokenIdentifier,
   * String, ExtendedBlock, BlockTokenIdentifier.AccessMode)}.
   */
  public void checkAccess(BlockTokenIdentifier id, String userId,
                          ExtendedBlock block, AccessMode mode)
      throws InvalidToken {
    get(block.getBlockPoolId()).checkAccess(id, userId, block, mode);
  }

  /**
   * 检查块令牌访问权限，路由到对应块池的权限检查逻辑
   * 详情见{@link BlockTokenSecretManager#checkAccess(Token, String,
   *                ExtendedBlock, BlockTokenIdentifier.AccessMode)}.
   */
  public void checkAccess(Token<BlockTokenIdentifier> token,
      String userId, ExtendedBlock block, AccessMode mode)
      throws InvalidToken {
    get(block.getBlockPoolId()).checkAccess(token, userId, block, mode);
  }

  /**
   * 检查块令牌访问权限，路由到对应块池的权限检查逻辑
   * 详情见{@link BlockTokenSecretManager#checkAccess(Token, String,
   *                ExtendedBlock, BlockTokenIdentifier.AccessMode,
   *                StorageType[], String[])}
   */
  public void checkAccess(Token<BlockTokenIdentifier> token,
      String userId, ExtendedBlock block, AccessMode mode,
      StorageType[] storageTypes, String[] storageIds)
      throws InvalidToken {
    get(block.getBlockPoolId()).checkAccess(token, userId, block, mode,
        storageTypes, storageIds);
  }

  /**
   * 添加导出的块密钥到对应块池的密钥管理器
   * 详情见{@link BlockTokenSecretManager#addKeys(ExportedBlockKeys)}.
   */
  public void addKeys(String bpid, ExportedBlockKeys exportedKeys,
      boolean updateCurrentKey) throws IOException {
    get(bpid).addKeys(exportedKeys, updateCurrentKey);
  }

  /**
   * 为指定块生成块令牌，路由到对应块池的生成逻辑
   * 详情见{@link BlockTokenSecretManager#generateToken(ExtendedBlock, EnumSet,
   *  StorageType[], String[])}.
   */
  public Token<BlockTokenIdentifier> generateToken(ExtendedBlock b,
      EnumSet<AccessMode> of, StorageType[] storageTypes, String[] storageIds)
      throws IOException {
    return get(b.getBlockPoolId()).generateToken(b, of, storageTypes,
        storageIds);
  }
  
  /**
   * 清空所有块池的所有密钥，仅用于测试
   */
  @VisibleForTesting
  public void clearAllKeysForTesting() {
    for (BlockTokenSecretManager btsm : map.values()) {
      btsm.clearAllKeysForTesting();
    }
  }

  /**
   * 为指定块池生成数据加密密钥
   * @param blockPoolId 目标块池ID
   * @return 生成的数据加密密钥
   */
  public DataEncryptionKey generateDataEncryptionKey(String blockPoolId) {
    return get(blockPoolId).generateDataEncryptionKey();
  }
  
  /**
   * 根据密钥ID获取对应块池的数据加密密钥
   * @param keyId 加密密钥ID
   * @param blockPoolId 块池ID
   * @param nonce 随机数用于验证
   * @return 加密密钥字节数组
   * @throws IOException 获取密钥失败或验证失败时抛出
   */
  public byte[] retrieveDataEncryptionKey(int keyId, String blockPoolId,
      byte[] nonce) throws IOException {
    return get(blockPoolId).retrieveDataEncryptionKey(keyId, nonce);
  }
}