// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.DelegationKey;

/**
 * 空实现的作业历史服务器状态存储服务
 * 不实际持久化任何状态信息，所有存储操作都是空实现，仅用于不需要持久化状态的场景
 */
@Private
@Unstable
public class HistoryServerNullStateStoreService
    extends HistoryServerStateStoreService {

  /**
   * 初始化空存储，不做任何操作
   * @param conf 配置对象
   * @throws IOException 不会抛出异常
   */
  @Override
  protected void initStorage(Configuration conf) throws IOException {
    // Do nothing
  }

  /**
   * 启动空存储，不做任何操作
   * @throws IOException 不会抛出异常
   */
  @Override
  protected void startStorage() throws IOException {
    // Do nothing
  }

  /**
   * 关闭空存储，不做任何操作
   * @throws IOException 不会抛出异常
   */
  @Override
  protected void closeStorage() throws IOException {
    // Do nothing
  }

  /**
   * 从空存储加载历史服务器状态，不支持该操作
   * @return 永远不会返回，直接抛出异常
   * @throws IOException 总是抛出不支持操作异常
   */
  @Override
  public HistoryServerState loadState() throws IOException {
    throw new UnsupportedOperationException(
        "Cannot load state from null store");
  }

  /**
   * 存储MR委托令牌信息，空实现不做任何操作
   * @param tokenId 委托令牌标识符
   * @param renewDate 更新时间
   * @throws IOException 不会抛出异常
   */
  @Override
  public void storeToken(MRDelegationTokenIdentifier tokenId, Long renewDate)
      throws IOException {
    // Do nothing
  }

  /**
   * 更新MR委托令牌信息，空实现不做任何操作
   * @param tokenId 委托令牌标识符
   * @param renewDate 更新时间
   * @throws IOException 不会抛出异常
   */
  @Override
  public void updateToken(MRDelegationTokenIdentifier tokenId, Long renewDate)
      throws IOException {
    // Do nothing
  }

  /**
   * 删除MR委托令牌信息，空实现不做任何操作
   * @param tokenId 委托令牌标识符
   * @throws IOException 不会抛出异常
   */
  @Override
  public void removeToken(MRDelegationTokenIdentifier tokenId)
      throws IOException {
    // Do nothing
  }

  /**
   * 存储委托令牌主密钥，空实现不做任何操作
   * @param key 委托密钥对象
   * @throws IOException 不会抛出异常
   */
  @Override
  public void storeTokenMasterKey(DelegationKey key) throws IOException {
    // Do nothing
  }

  /**
   * 删除委托令牌主密钥，空实现不做任何操作
   * @param key 委托密钥对象
   * @throws IOException 不会抛出异常
   */
  @Override
  public void removeTokenMasterKey(DelegationKey key) throws IOException {
    // Do nothing
  }
}