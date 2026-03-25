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

import javax.crypto.SecretKey;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.token.delegation.DelegationKey;

/**
 * 块令牌（Block Token）系统中使用的加密密钥，用于生成和验证HDFS数据块访问令牌
 * 继承DelegationKey通用实现，扩展用于块令牌场景
 */
@InterfaceAudience.Private
public class BlockKey extends DelegationKey {

  /**
   * 构造空的BlockKey对象，用于反序列化
   */
  public BlockKey() {
    super();
  }

  /**
   * 构造带完整参数的BlockKey对象
   * @param keyId 密钥ID，全局唯一标识
   * @param expiryDate 密钥过期时间戳
   * @param key JCE标准SecretKey对象
   */
  public BlockKey(int keyId, long expiryDate, SecretKey key) {
    super(keyId, expiryDate, key);
  }
  
  /**
   * 构造带编码密钥字节的BlockKey对象
   * @param keyId 密钥ID，全局唯一标识
   * @param expiryDate 密钥过期时间戳
   * @param encodedKey 密钥的编码字节数组
   */
  public BlockKey(int keyId, long expiryDate, byte[] encodedKey) {
    super(keyId, expiryDate, encodedKey);
  }
}