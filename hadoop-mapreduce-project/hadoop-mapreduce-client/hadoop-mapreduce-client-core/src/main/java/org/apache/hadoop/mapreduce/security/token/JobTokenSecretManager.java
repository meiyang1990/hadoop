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

package org.apache.hadoop.mapreduce.security.token;

import java.util.Map;
import java.util.TreeMap;

import javax.crypto.SecretKey;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;

/**
 * 文件说明：MapReduce作业令牌密钥管理器，负责生成、缓存和验证作业令牌，保障MapReduce作业内部通信安全
 * 
 * 作业令牌SecretManager实现，可缓存已生成的作业令牌，用于MapReduce作业的身份认证
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobTokenSecretManager extends SecretManager<JobTokenIdentifier> {
  // 主密钥，用于生成作业令牌密码
  private final SecretKey masterKey;
  // 缓存当前活跃作业的令牌密钥，key为作业ID，value为对应作业的密钥
  private final Map<String, SecretKey> currentJobTokens;

  /**
   * 根据字节数组构造SecretKey对象
   * @param key 用于生成密钥的字节数组
   * @return 构造完成的SecretKey对象
   */
  public static SecretKey createSecretKey(byte[] key) {
    return SecretManager.createSecretKey(key);
  }
  
  /**
   * 使用给定密钥计算消息的HMAC哈希值
   * @param msg 待计算哈希的消息
   * @param key 用于计算的密钥
   * @return 计算得到的哈希值
   */
  public static byte[] computeHash(byte[] msg, SecretKey key) {
    return createPassword(msg, key);
  }
  
  /**
   * 构造作业令牌密钥管理器，生成主密钥并初始化缓存
   */
  public JobTokenSecretManager() {
    this.masterKey = generateSecret();
    this.currentJobTokens = new TreeMap<String, SecretKey>();
  }
  
  /**
   * 为指定作业令牌标识符生成对应的密码
   * @param identifier 作业令牌标识符
   * @return 生成的令牌密码字节数组
   */
  @Override
  public byte[] createPassword(JobTokenIdentifier identifier) {
    byte[] result = createPassword(identifier.getBytes(), masterKey);
    return result;
  }

  /**
   * 将作业的令牌添加到本地缓存，方便后续验证使用
   * @param jobId 作业ID
   * @param token 作业令牌对象
   */
  public void addTokenForJob(String jobId, Token<JobTokenIdentifier> token) {
    SecretKey tokenSecret = createSecretKey(token.getPassword());
    synchronized (currentJobTokens) {
      currentJobTokens.put(jobId, tokenSecret);
    }
  }

  /**
   * 从缓存中移除已完成作业的令牌，清理缓存空间
   * @param jobId 待移除令牌的作业ID
   */
  public void removeTokenForJob(String jobId) {
    synchronized (currentJobTokens) {
      currentJobTokens.remove(jobId);
    }
  }
  
  /**
   * 根据作业ID从缓存中查询对应作业的令牌密钥
   * @param jobId 待查询的作业ID
   * @return 查询到的令牌密钥SecretKey对象
   * @throws InvalidToken 找不到对应令牌时抛出异常
   */
  public SecretKey retrieveTokenSecret(String jobId) throws InvalidToken {
    SecretKey tokenSecret = null;
    synchronized (currentJobTokens) {
      tokenSecret = currentJobTokens.get(jobId);
    }
    if (tokenSecret == null) {
      throw new InvalidToken("Can't find job token for job " + jobId + " !!");
    }
    return tokenSecret;
  }
  
  /**
   * 根据作业令牌标识符查询对应令牌密码字节数组
   * @param identifier 待查询的作业令牌标识符
   * @return 查询到的令牌密码字节数组
   * @throws InvalidToken 找不到对应令牌时抛出异常
   */
  @Override
  public byte[] retrievePassword(JobTokenIdentifier identifier)
      throws InvalidToken {
    return retrieveTokenSecret(identifier.getJobId().toString()).getEncoded();
  }

  /**
   * 创建一个空的作业令牌标识符实例，用于反序列化
   * @return 新建的空作业令牌标识符对象
   */
  @Override
  public JobTokenIdentifier createIdentifier() {
    return new JobTokenIdentifier();
  }
}