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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.service.AbstractService;

/**
 * 历史服务器状态存储抽象基类，定义了MapReduce历史服务器状态存储的统一接口
 * 具体存储实现需要实现存储和加载方法来完成实际的状态持久化操作，支持不同存储后端扩展
 */
@Private
@Unstable
public abstract class HistoryServerStateStoreService extends AbstractService {

  /**
   * 历史服务器状态容器，承载所有需要持久化的MR代理令牌状态数据
   */
  public static class HistoryServerState {
    Map<MRDelegationTokenIdentifier, Long> tokenState =
        new HashMap<MRDelegationTokenIdentifier, Long>();
    Set<DelegationKey> tokenMasterKeyState = new HashSet<DelegationKey>();

    public Map<MRDelegationTokenIdentifier, Long> getTokenState() {
      return tokenState;
    }

    public Set<DelegationKey> getTokenMasterKeyState() {
      return tokenMasterKeyState;
    }
  }

  /**
   * 构造方法，初始化状态存储服务
   */
  public HistoryServerStateStoreService() {
    super(HistoryServerStateStoreService.class.getName());
  }

  /**
   * 服务初始化入口，调用存储初始化逻辑
   * @param conf 配置对象
   * @throws IOException 初始化异常
   */
  @Override
  public void serviceInit(Configuration conf) throws IOException {
    initStorage(conf);
  }

  /**
   * 服务启动入口，调用存储启动逻辑
   * @throws IOException 启动异常
   */
  @Override
  public void serviceStart() throws IOException {
    startStorage();
  }

  /**
   * 服务停止入口，调用存储关闭逻辑
   * @throws IOException 停止异常
   */
  @Override
  public void serviceStop() throws IOException {
    closeStorage();
  }

  /**
   * 存储实现类特定的初始化逻辑，由具体实现类扩展
   * @param conf 配置对象
   * @throws IOException 初始化异常
   */
  protected abstract void initStorage(Configuration conf) throws IOException;

  /**
   * 存储实现类特定的启动逻辑，由具体实现类扩展
   * @throws IOException 启动异常
   */
  protected abstract void startStorage() throws IOException;

  /**
   * 存储实现类特定的关闭逻辑，由具体实现类扩展
   * @throws IOException 关闭异常
   */
  protected abstract void closeStorage() throws IOException;

  /**
   * 从存储中加载所有历史服务器状态数据
   * @return 加载完成的历史服务器状态对象
   * @throws IOException 加载异常
   */
  public abstract HistoryServerState loadState() throws IOException;

  /**
   * 持久化存储MR代理令牌及其过期时间，方法必须阻塞到数据持久化完成才返回
   * @param tokenId 要存储的代理令牌标识
   * @param renewDate 令牌更新截止时间
   * @throws IOException 存储异常
   */
  public abstract void storeToken(MRDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException;

  /**
   * 更新存储中MR代理令牌的过期时间，方法必须阻塞到数据更新完成才返回
   * @param tokenId 要更新的代理令牌标识
   * @param renewDate 新的令牌更新截止时间
   * @throws IOException 更新异常
   */
  public abstract void updateToken(MRDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException;

  /**
   * 从存储中删除指定MR代理令牌，方法必须阻塞到数据删除完成才返回
   * @param tokenId 要删除的代理令牌标识
   * @throws IOException 删除异常
   */
  public abstract void removeToken(MRDelegationTokenIdentifier tokenId)
      throws IOException;

  /**
   * 持久化存储MR代理令牌主密钥，方法必须阻塞到数据持久化完成才返回
   * @param key 要存储的代理主密钥
   * @throws IOException 存储异常
   */
  public abstract void storeTokenMasterKey(
      DelegationKey key) throws IOException;

  /**
   * 从存储中删除指定MR代理令牌主密钥，方法必须阻塞到数据删除完成才返回
   * @param key 要删除的代理主密钥
   * @throws IOException 删除异常
   */
  public abstract void removeTokenMasterKey(DelegationKey key)
      throws IOException;
}