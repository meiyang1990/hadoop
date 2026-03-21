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

package org.apache.hadoop.yarn.server.nodemanager.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.yarn.server.nodemanager.recovery.RecoveryIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerTokensState;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.MasterKeyData;

/**
 * NodeManager端容器令牌密钥管理器，负责容器令牌的生成、验证和轮转管理。
 * NM只维护两个主密钥：RM当前使用的密钥，以及上一个轮转周期的旧密钥。
 * 同时负责防止同一容器令牌被重复用于启动容器，保障安全。
 */
public class NMContainerTokenSecretManager extends
    BaseContainerTokenSecretManager {

  private static final Logger LOG =
       LoggerFactory.getLogger(NMContainerTokenSecretManager.class);
  
  // 上一个轮转周期的主密钥
  private MasterKeyData previousMasterKey;
  // 已成功启动容器的跟踪器，按过期时间分组存储容器ID，用于防重复启动
  private final TreeMap<Long, List<ContainerId>> recentlyStartedContainerTracker;
  // NM状态存储服务，用于持久化密钥和已启动容器信息，支持NM重启恢复
  private final NMStateStoreService stateStore;
  
  // 当前Node的节点地址，用于验证容器令牌是否属于本节点
  private String nodeHostAddr;
  
  /**
   * 使用空状态存储构造NM容器令牌密钥管理器（用于不需要持久化恢复的场景）。
   * @param conf 配置对象
   */
  public NMContainerTokenSecretManager(Configuration conf) {
    this(conf, new NMNullStateStoreService());
  }

  /**
   * 使用指定状态存储构造NM容器令牌密钥管理器。
   * @param conf 配置对象
   * @param stateStore NM状态存储服务
   */
  public NMContainerTokenSecretManager(Configuration conf,
      NMStateStoreService stateStore) {
    super(conf);
    recentlyStartedContainerTracker =
        new TreeMap<Long, List<ContainerId>>();
    this.stateStore = stateStore;
  }

  /**
   * 从状态存储恢复容器令牌密钥和已启动容器信息，用于NM重启后恢复状态。
   * @throws IOException 恢复失败时抛出IO异常
   */
  public synchronized void recover()
      throws IOException {
    RecoveredContainerTokensState state =
        stateStore.loadContainerTokensState();
    // 恢复当前主密钥
    MasterKey key = state.getCurrentMasterKey();
    if (key != null) {
      super.currentMasterKey =
          new MasterKeyData(key, createSecretKey(key.getBytes().array()));
    }

    // 恢复上一个主密钥
    key = state.getPreviousMasterKey();
    if (key != null) {
      previousMasterKey =
          new MasterKeyData(key, createSecretKey(key.getBytes().array()));
    }

    // 从当前主密钥密钥ID恢复下一个密钥序列号
    if (super.currentMasterKey != null) {
      super.serialNo = super.currentMasterKey.getMasterKey().getKeyId() + 1;
    }

    // 恢复所有已启动容器信息
    try (RecoveryIterator<Entry<ContainerId, Long>> it = state.getIterator()) {
      while (it.hasNext()) {
        Entry<ContainerId, Long> entry = it.next();
        ContainerId containerId = entry.getKey();
        Long expTime = entry.getValue();
        List<ContainerId> containerList =
            recentlyStartedContainerTracker.get(expTime);
        if (containerList == null) {
          containerList = new ArrayList<ContainerId>();
          recentlyStartedContainerTracker.put(expTime, containerList);
        }
        if (!containerList.contains(containerId)) {
          containerList.add(containerId);
        }
      }
    }
  }

  /**
   * 更新当前主密钥，并持久化到状态存储。
   * @param key 新的当前主密钥数据
   */
  private void updateCurrentMasterKey(MasterKeyData key) {
    super.currentMasterKey = key;
    try {
      stateStore.storeContainerTokenCurrentMasterKey(key.getMasterKey());
    } catch (IOException e) {
      LOG.error("Unable to update current master key in state store", e);
    }
  }

  /**
   * 更新上一个主密钥，并持久化到状态存储。
   * @param key 新的上一个主密钥数据
   */
  private void updatePreviousMasterKey(MasterKeyData key) {
    previousMasterKey = key;
    try {
      stateStore.storeContainerTokenPreviousMasterKey(key.getMasterKey());
    } catch (IOException e) {
      LOG.error("Unable to update previous master key in state store", e);
    }
  }

  /**
   * 设置从RM获取的新主密钥，处理主密钥轮转。
   * 注册阶段或RM轮转主密钥通知NM时调用此方法。
   * @param masterKeyRecord RM下发的新主密钥记录
   */
  @Private
  public synchronized void setMasterKey(MasterKey masterKeyRecord) {
    // 仅当密钥ID变更时才更新
    if (super.currentMasterKey == null || super.currentMasterKey.getMasterKey()
          .getKeyId() != masterKeyRecord.getKeyId()) {
      LOG.info("Rolling master-key for container-tokens, got key with id "
          + masterKeyRecord.getKeyId());
      // 原当前密钥降级为旧密钥
      if (super.currentMasterKey != null) {
        updatePreviousMasterKey(super.currentMasterKey);
      }
      // 新密钥成为当前密钥
      updateCurrentMasterKey(new MasterKeyData(masterKeyRecord,
          createSecretKey(masterKeyRecord.getBytes().array())));
    }
  }

  /**
   * 验证容器令牌，提取对应密码，支持当前和上一个主密钥验证。
   * 同时验证容器令牌是否属于本NM节点。
   */
  @Override
  public synchronized byte[] retrievePassword(
      ContainerTokenIdentifier identifier) throws SecretManager.InvalidToken {
    int keyId = identifier.getMasterKeyId();

    MasterKeyData masterKeyToUse = null;
    // 匹配旧主密钥
    if (this.previousMasterKey != null
        && keyId == this.previousMasterKey.getMasterKey().getKeyId()) {
      // 容器启动使用了前一个主密钥生成的令牌
      masterKeyToUse = this.previousMasterKey;
    } 
    // 匹配当前主密钥
    else if (keyId == super.currentMasterKey.getMasterKey().getKeyId()) {
      // 容器启动使用了当前主密钥生成的令牌
      masterKeyToUse = super.currentMasterKey;
    }

    // 验证容器令牌的节点地址是否与当前NM匹配
    if (nodeHostAddr != null
        && !identifier.getNmHostAddress().equals(nodeHostAddr)) {
      // 令牌不属于本节点，验证失败
      throw new SecretManager.InvalidToken("Given Container "
          + identifier.getContainerID().toString()
          + " identifier is not valid for current Node manager. Expected : "
          + nodeHostAddr + " Found : " + identifier.getNmHostAddress());
    }
    
    // 使用匹配到的密钥计算令牌密码返回
    if (masterKeyToUse != null) {
      return retrievePasswordInternal(identifier, masterKeyToUse);
    }

    // 没有匹配到有效密钥，令牌非法
    throw new SecretManager.InvalidToken("Given Container "
        + identifier.getContainerID().toString()
        + " seems to have an illegally generated token.");
  }

  /**
   * 容器启动成功后，记录容器令牌，防止同一令牌重复启动容器。
   * 持久化记录到状态存储，支持重启恢复。
   * @param tokenId 容器令牌标识符
   */
  public synchronized void startContainerSuccessful(
      ContainerTokenIdentifier tokenId) {

    // 先清理已过期的容器令牌记录
    removeAnyContainerTokenIfExpired();
    
    ContainerId containerId = tokenId.getContainerID();
    Long expTime = tokenId.getExpiryTimeStamp();
    // 按过期时间分组存储，相同过期时间的容器放同一列表
    if (!recentlyStartedContainerTracker.containsKey(expTime)) {
      recentlyStartedContainerTracker
        .put(expTime, new ArrayList<ContainerId>());
    }
    recentlyStartedContainerTracker.get(expTime).add(containerId);
    try {
      // 持久化存储记录
      stateStore.storeContainerToken(containerId, expTime);
    } catch (IOException e) {
      LOG.error("Unable to store token for container " + containerId, e);
    }
  }

  /**
   * 清理已过期的容器令牌记录，从内存和状态存储中删除。
   */
  protected synchronized void removeAnyContainerTokenIfExpired() {
    // 遍历按过期时间排序的容器记录
    Iterator<Entry<Long, List<ContainerId>>> containersI =
        this.recentlyStartedContainerTracker.entrySet().iterator();
    Long currTime = System.currentTimeMillis();
    while (containersI.hasNext()) {
      Entry<Long, List<ContainerId>> containerEntry = containersI.next();
      // 已过期，删除所有该过期时间下的容器记录
      if (containerEntry.getKey() < currTime) {
        for (ContainerId container : containerEntry.getValue()) {
          try {
            stateStore.removeContainerToken(container);
          } catch (IOException e) {
            LOG.error("Unable to remove token for container " + container, e);
          }
        }
        containersI.remove();
      } else {
        // TreeMap有序，遇到第一个未过期的即可停止遍历
        break;
      }
    }
  }

  /**
   * 验证容器启动请求是否合法，防止同一容器令牌重复启动。
   * @param containerTokenIdentifier 待验证的容器令牌标识符
   * @return 如果令牌未使用过返回true，已使用过返回false
   */
  public synchronized boolean isValidStartContainerRequest(
      ContainerTokenIdentifier containerTokenIdentifier) {

    // 先清理已过期记录
    removeAnyContainerTokenIfExpired();

    Long expTime = containerTokenIdentifier.getExpiryTimeStamp();
    List<ContainerId> containers =
        this.recentlyStartedContainerTracker.get(expTime);
    // 容器ID不在已启动列表中，请求合法
    if (containers == null
        || !containers.contains(containerTokenIdentifier.getContainerID())) {
      return true;
    } else {
      // 容器ID已启动过，请求非法，拒绝重复启动
      return false;
    }
  }

  /**
   * 设置当前NM的节点ID，更新节点地址用于令牌验证。
   * @param nodeId 当前NodeManager的节点ID
   */
  public synchronized void setNodeId(NodeId nodeId) {
    nodeHostAddr = nodeId.toString();
    LOG.info("Updating node address : " + nodeHostAddr);
  }
}