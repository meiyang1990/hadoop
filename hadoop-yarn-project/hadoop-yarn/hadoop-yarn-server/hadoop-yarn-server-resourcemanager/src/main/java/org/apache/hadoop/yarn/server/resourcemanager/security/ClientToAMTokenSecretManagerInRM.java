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

package org.apache.hadoop.yarn.server.resourcemanager.security;

import java.util.HashMap;
import java.util.Map;

import javax.crypto.SecretKey;
import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.security.client.BaseClientToAMTokenSecretManager;

/**
 * ResourceManager端管理Client到ApplicationMaster的令牌密钥管理器
 * 负责为每个应用尝试生成、存储、维护客户端与AM通信的认证密钥
 */
public class ClientToAMTokenSecretManagerInRM extends
    BaseClientToAMTokenSecretManager {

  // 按应用尝试维度存储各应用的客户端令牌主密钥
  private Map<ApplicationAttemptId, SecretKey> masterKeys =
      new HashMap<ApplicationAttemptId, SecretKey>();

  /**
   * 为指定应用尝试生成新的主密钥
   * @param applicationAttemptID 应用尝试ID
   * @return 生成的密钥
   */
  public synchronized SecretKey createMasterKey(
      ApplicationAttemptId applicationAttemptID) {
    return generateSecret();
  }

  /**
   * 注册应用尝试，存储其对应的主密钥
   * @param applicationAttemptID 应用尝试ID
   * @param key 应用的主密钥
   */
  public synchronized void registerApplication(
      ApplicationAttemptId applicationAttemptID, SecretKey key) {
    this.masterKeys.put(applicationAttemptID, key);
  }

  /**
   * RM恢复场景使用：根据已有密钥数据注册主密钥
   * @param applicationAttemptID 应用尝试ID
   * @param keyData 密钥字节数据
   * @return 恢复生成的密钥
   */
  // Only for RM recovery
  public synchronized SecretKey registerMasterKey(
      ApplicationAttemptId applicationAttemptID, byte[] keyData) {
    SecretKey key = createSecretKey(keyData);
    registerApplication(applicationAttemptID, key);
    return key;
  }

  /**
   * 注销应用尝试，移除其对应的主密钥
   * @param applicationAttemptID 应用尝试ID
   */
  public synchronized void unRegisterApplication(
      ApplicationAttemptId applicationAttemptID) {
    this.masterKeys.remove(applicationAttemptID);
  }

  @Override
  public synchronized SecretKey getMasterKey(
      ApplicationAttemptId applicationAttemptID) {
    return this.masterKeys.get(applicationAttemptID);
  }

  /**
   * 检查指定应用尝试是否存在主密钥，仅用于测试
   * @param applicationAttemptID 应用尝试ID
   * @return 是否存在主密钥
   */
  @VisibleForTesting
  public synchronized boolean hasMasterKey(
      ApplicationAttemptId applicationAttemptID) {
    return this.masterKeys.containsKey(applicationAttemptID);
  }
}