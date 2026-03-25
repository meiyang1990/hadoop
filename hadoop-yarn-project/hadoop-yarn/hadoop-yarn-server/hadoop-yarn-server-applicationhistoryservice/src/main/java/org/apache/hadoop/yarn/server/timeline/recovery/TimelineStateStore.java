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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;

@Private
@Unstable
/**
 * 时间线服务状态存储的抽象基类。
 * 具体存储实现需要实现阻塞式的存储和加载方法，完成状态的持久化与恢复。
 */
public abstract class TimelineStateStore extends AbstractService {

  /**
   * 时间线服务状态容器，保存所有需要持久化的状态数据。
   */
  public static class TimelineServiceState {
    int latestSequenceNumber = 0;
    Map<TimelineDelegationTokenIdentifier, Long> tokenState =
        new HashMap<TimelineDelegationTokenIdentifier, Long>();
    Set<DelegationKey> tokenMasterKeyState = new HashSet<DelegationKey>();

    /**
     * 获取最新的令牌序列号。
     * @return 最新序列号
     */
    public int getLatestSequenceNumber() {
      return latestSequenceNumber;
    }

    /**
     * 获取所有代理令牌的状态信息。
     * @return 令牌ID与过期时间的映射表
     */
    public Map<TimelineDelegationTokenIdentifier, Long> getTokenState() {
      return tokenState;
    }

    /**
     * 获取所有代理令牌主密钥集合。
     * @return 主密钥集合
     */
    public Set<DelegationKey> getTokenMasterKeyState() {
      return tokenMasterKeyState;
    }
  }

  public TimelineStateStore() {
    super(TimelineStateStore.class.getName());
  }

  public TimelineStateStore(String name) {
    super(name);
  }

  /**
   * 初始化状态存储，调用具体实现的初始化逻辑。
   *
   * @param conf 配置对象
   * @throws IOException 初始化失败时抛出IO异常
   */
  @Override
  public void serviceInit(Configuration conf) throws IOException {
    initStorage(conf);
  }

  /**
   * 启动状态存储，调用具体实现的启动逻辑。
   *
   * @throws IOException 启动失败时抛出IO异常
   */
  @Override
  public void serviceStart() throws IOException {
    startStorage();
  }

  /**
   * 停止状态存储，调用具体实现的关闭逻辑。
   *
   * @throws IOException 关闭失败时抛出IO异常
   */
  @Override
  public void serviceStop() throws IOException {
    closeStorage();
  }

  /**
   * 存储实现特定的初始化逻辑，由子类实现。
   *
   * @param conf 配置对象
   * @throws IOException 初始化失败抛出IO异常
   */
  protected abstract void initStorage(Configuration conf) throws IOException;

  /**
   * 存储实现特定的启动逻辑，由子类实现。
   *
   * @throws IOException 启动失败抛出IO异常
   */
  protected abstract void startStorage() throws IOException;

  /**
   * 存储实现特定的关闭逻辑，由子类实现。
   *
   * @throws IOException 关闭失败抛出IO异常
   */
  protected abstract void closeStorage() throws IOException;

  /**
   * 从状态存储加载时间线服务的全量状态。
   *
   * @return 加载完成的服务状态对象
   * @throws IOException 加载失败抛出IO异常
   */
  public abstract TimelineServiceState loadState() throws IOException;

  /**
   * 阻塞式存储代理令牌与当前序列号到状态存储。
   * 实现必须等待令牌持久化完成后才返回。
   *
   * @param tokenId 要存储的代理令牌ID
   * @param renewDate 令牌更新截止时间
   * @throws IOException 存储失败抛出IO异常
   */
  public abstract void storeToken(TimelineDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException;

  /**
   * 阻塞式更新状态存储中代理令牌的过期时间。
   * 实现必须等待过期时间更新完成后才返回。
   *
   * @param tokenId 要更新的代理令牌ID
   * @param renewDate 新的令牌更新截止时间
   * @throws IOException 更新失败抛出IO异常
   */
  public abstract void updateToken(TimelineDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException;

  /**
   * 阻塞式从状态存储删除代理令牌。
   * 实现必须等待令牌删除完成后才返回。
   *
   * @param tokenId 要删除的代理令牌ID
   * @throws IOException 删除失败抛出IO异常
   */
  public abstract void removeToken(TimelineDelegationTokenIdentifier tokenId)
      throws IOException;

  /**
   * 阻塞式存储代理令牌主密钥到状态存储。
   * 实现必须等待主密钥持久化完成后才返回。
   *
   * @param key 要存储的主密钥对象
   * @throws IOException 存储失败抛出IO异常
   */
  public abstract void storeTokenMasterKey(
      DelegationKey key) throws IOException;

  /**
   * 阻塞式从状态存储删除代理令牌主密钥。
   * 实现必须等待主密钥删除完成后才返回。
   *
   * @param key 要删除的主密钥对象
   * @throws IOException 删除失败抛出IO异常
   */
  public abstract void removeTokenMasterKey(DelegationKey key)
      throws IOException;
}