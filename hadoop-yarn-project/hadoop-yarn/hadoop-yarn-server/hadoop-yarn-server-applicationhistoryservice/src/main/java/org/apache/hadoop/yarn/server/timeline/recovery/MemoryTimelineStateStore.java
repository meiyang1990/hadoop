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
package org.apache.hadoop.yarn.server.timeline.recovery;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;

/**
 * 基于内存实现的时间线服务状态存储，主要用于单元测试
 */
public class MemoryTimelineStateStore
    extends TimelineStateStore {

  // 内存存储的时间线服务状态对象
  private TimelineServiceState state;

  @Override
  protected void initStorage(Configuration conf) throws IOException {
    // 内存存储无需额外初始化，留空实现
  }

  @Override
  protected void startStorage() throws IOException {
    // 创建新的状态对象，初始化内存存储
    state = new TimelineServiceState();
  }

  @Override
  protected void closeStorage() throws IOException {
    // 清空状态引用，释放内存
    state = null;
  }

  @Override
  public TimelineServiceState loadState() throws IOException {
    // 创建新的状态对象用于返回
    TimelineServiceState result = new TimelineServiceState();
    // 复制所有令牌状态
    result.tokenState.putAll(state.tokenState);
    // 复制所有主密钥状态
    result.tokenMasterKeyState.addAll(state.tokenMasterKeyState);
    // 复制最新序列号
    result.latestSequenceNumber = state.latestSequenceNumber;
    return result;
  }

  @Override
  public void storeToken(TimelineDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException {
    // 检查令牌是否已存在，避免重复存储
    if (state.tokenState.containsKey(tokenId)) {
      throw new IOException("token " + tokenId + " was stored twice");
    }
    // 存储令牌ID和更新时间
    state.tokenState.put(tokenId, renewDate);
    // 更新最新序列号
    state.latestSequenceNumber = tokenId.getSequenceNumber();
  }

  @Override
  public void updateToken(TimelineDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException {
    // 检查令牌是否存在，不存在则抛出异常
    if (!state.tokenState.containsKey(tokenId)) {
      throw new IOException("token " + tokenId + " not in store");
    }
    // 更新令牌更新时间
    state.tokenState.put(tokenId, renewDate);
  }

  @Override
  public void removeToken(TimelineDelegationTokenIdentifier tokenId)
      throws IOException {
    // 从内存中移除指定令牌
    state.tokenState.remove(tokenId);
  }

  @Override
  public void storeTokenMasterKey(DelegationKey key)
      throws IOException {
    // 检查主密钥是否已存在，避免重复存储
    if (state.tokenMasterKeyState.contains(key)) {
      throw new IOException("token master key " + key + " was stored twice");
    }
    // 存储令牌主密钥
    state.tokenMasterKeyState.add(key);
  }

  @Override
  public void removeTokenMasterKey(DelegationKey key)
      throws IOException {
    // 从内存中移除指定主密钥
    state.tokenMasterKeyState.remove(key);
  }
}