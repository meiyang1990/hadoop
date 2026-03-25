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

package org.apache.hadoop.yarn.server.timeline.security;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.timeline.recovery.LeveldbTimelineStateStore;
import org.apache.hadoop.yarn.server.timeline.recovery.TimelineStateStore;
import org.apache.hadoop.yarn.server.timeline.recovery.TimelineStateStore.TimelineServiceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：Timeline V1版本授权令牌密钥管理器服务，是{@link TimelineV1DelegationTokenSecretManager的服务包装类
 * 负责管理应用历史时间线服务的授权令牌与密钥，支持重启恢复
 */
@Private
@Unstable
public class TimelineV1DelegationTokenSecretManagerService extends
    TimelineDelgationTokenSecretManagerService {
  // 时间线服务状态存储，用于持久化令牌和密钥信息，支持恢复
  private TimelineStateStore stateStore = null;

  /**
   * 构造函数，初始化服务
   */
  public TimelineV1DelegationTokenSecretManagerService() {
    super(TimelineV1DelegationTokenSecretManagerService.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 如果开启了恢复功能，初始化状态存储
    if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_RECOVERY_ENABLED)) {
      stateStore = createStateStore(conf);
      stateStore.init(conf);
    }
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // 如果状态存储存在，启动并从存储中恢复令牌状态
    if (stateStore != null) {
      stateStore.start();
      TimelineServiceState state = stateStore.loadState();
      ((TimelineV1DelegationTokenSecretManager)
          getTimelineDelegationTokenSecretManager()).recover(state);
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    // 停止状态存储
    if (stateStore != null) {
      stateStore.stop();
    }
    super.serviceStop();
  }

  @Override
  protected AbstractDelegationTokenSecretManager
      <TimelineDelegationTokenIdentifier>
      createTimelineDelegationTokenSecretManager(long secretKeyInterval,
          long tokenMaxLifetime, long tokenRenewInterval,
          long tokenRemovalScanInterval) {
    // 创建V1版本的授权令牌密钥管理器
    return new TimelineV1DelegationTokenSecretManager(secretKeyInterval,
        tokenMaxLifetime, tokenRenewInterval, tokenRemovalScanInterval,
        stateStore);
  }

  /**
   * 根据配置创建状态存储实例
   * @param conf 配置对象
   * @return 创建好的状态存储实例
   */
  protected TimelineStateStore createStateStore(
      Configuration conf) {
    // 通过反射创建配置指定的存储类，默认使用Leveldb实现
    return ReflectionUtils.newInstance(
        conf.getClass(YarnConfiguration.TIMELINE_SERVICE_STATE_STORE_CLASS,
            LeveldbTimelineStateStore.class,
            TimelineStateStore.class), conf);
  }

  /**
   * 适用于ATSv1和ATSv1.5版本的授权令牌密钥管理器
   * 负责生成、存储、更新、回收授权令牌和主密钥，支持持久化和恢复
   */
  @Private
  @Unstable
  public static class TimelineV1DelegationTokenSecretManager extends
      AbstractDelegationTokenSecretManager<TimelineDelegationTokenIdentifier> {

    public static final Logger LOG =
        LoggerFactory.getLogger(TimelineV1DelegationTokenSecretManager.class);

    // 时间线服务状态存储，用于持久化令牌和密钥
    private TimelineStateStore stateStore;

    /**
     * 构造Timeline V1版本的授权令牌密钥管理器
     * @param delegationKeyUpdateInterval 密钥更新间隔（毫秒）
     * @param delegationTokenMaxLifetime 授权令牌最大生命周期（毫秒）
     * @param delegationTokenRenewInterval 令牌必须更新的间隔（毫秒）
     * @param delegationTokenRemoverScanInterval 扫描过期令牌的间隔（毫秒）
     * @param stateStore 时间线服务状态存储
     */
    public TimelineV1DelegationTokenSecretManager(
        long delegationKeyUpdateInterval,
        long delegationTokenMaxLifetime,
        long delegationTokenRenewInterval,
        long delegationTokenRemoverScanInterval,
        TimelineStateStore stateStore) {
      super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
          delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
      this.stateStore = stateStore;
    }

    @Override
    public TimelineDelegationTokenIdentifier createIdentifier() {
      // 创建Timeline授权令牌标识符实例
      return new TimelineDelegationTokenIdentifier();
    }

    @Override
    protected void storeNewMasterKey(DelegationKey key) throws IOException {
      LOG.debug("Storing master key {}", key.getKeyId());
      try {
        if (stateStore != null) {
          // 持久化新主密钥到状态存储
          stateStore.storeTokenMasterKey(key);
        }
      } catch (IOException e) {
        LOG.error("Unable to store master key " + key.getKeyId(), e);
      }
    }

    @Override
    protected void removeStoredMasterKey(DelegationKey key) {
      LOG.debug("Removing master key {}", key.getKeyId());
      try {
        if (stateStore != null) {
          // 从状态存储删除过期主密钥
          stateStore.removeTokenMasterKey(key);
        }
      } catch (IOException e) {
        LOG.error("Unable to remove master key " + key.getKeyId(), e);
      }
    }

    @Override
    protected void storeNewToken(TimelineDelegationTokenIdentifier tokenId,
        long renewDate) {
      LOG.debug("Storing token {}", tokenId.getSequenceNumber());
      try {
        if (stateStore != null) {
          // 持久化新授权令牌到状态存储
          stateStore.storeToken(tokenId, renewDate);
        }
      } catch (IOException e) {
        LOG.error("Unable to store token " + tokenId.getSequenceNumber(), e);
      }
    }

    @Override
    protected void removeStoredToken(TimelineDelegationTokenIdentifier tokenId)
        throws IOException {
      LOG.debug("Storing token {}", tokenId.getSequenceNumber());
      try {
        if (stateStore != null) {
          // 从状态存储删除已取消的授权令牌
          stateStore.removeToken(tokenId);
        }
      } catch (IOException e) {
        LOG.error("Unable to remove token " + tokenId.getSequenceNumber(), e);
      }
    }

    @Override
    protected void updateStoredToken(TimelineDelegationTokenIdentifier tokenId,
        long renewDate) {
      LOG.debug("Updating token {}", tokenId.getSequenceNumber());
      try {
        if (stateStore != null) {
          // 更新状态存储中令牌的续期时间
          stateStore.updateToken(tokenId, renewDate);
        }
      } catch (IOException e) {
        LOG.error("Unable to update token " + tokenId.getSequenceNumber(), e);
      }
    }

    /**
     * 从持久化状态恢复所有令牌和密钥
     * @param state 从存储加载的时间线服务状态
     * @throws IOException IO异常
     */
    public void recover(TimelineServiceState state) throws IOException {
      LOG.info("Recovering " + getClass().getSimpleName());
      // 恢复所有主密钥
      for (DelegationKey key : state.getTokenMasterKeyState()) {
        addKey(key);
      }
      // 恢复令牌序列号
      this.delegationTokenSequenceNumber = state.getLatestSequenceNumber();
      // 恢复所有已颁发的授权令牌
      for (Entry<TimelineDelegationTokenIdentifier, Long> entry :
          state.getTokenState().entrySet()) {
        addPersistedDelegationToken(entry.getKey(), entry.getValue());
      }
    }
  }
}