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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.io.IOException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 基于内存实现的ResourceManager状态存储，主要用于测试和无持久化需求的场景
 * 将RM所有恢复所需状态存储在进程内存中，RM重启后状态会全部丢失
 */
@Private
@Unstable
public class MemoryRMStateStore extends RMStateStore {
  
  RMState state = new RMState();
  private long epoch = 0L;
  
  @VisibleForTesting
  /** 获取当前内存中的RM状态，仅用于测试 */
  public RMState getState() {
    return state;
  }

  @Override
  public void checkVersion() throws Exception {
  }

  @Override
  /** 获取当前epoch并自增，用于版本控制 */
  public synchronized long getAndIncrementEpoch() throws Exception {
    long currentEpoch = epoch;
    epoch = nextEpoch(epoch);
    return currentEpoch;
  }

  @Override
  /** 从内存加载RM状态，返回当前状态的深拷贝 */
  public synchronized RMState loadState() throws Exception {
    // return a copy of the state to allow for modification of the real state
    RMState returnState = new RMState();
    // 拷贝应用状态
    returnState.appState.putAll(state.appState);
    // 拷贝委派令牌主密钥状态
    returnState.rmSecretManagerState.getMasterKeyState()
      .addAll(state.rmSecretManagerState.getMasterKeyState());
    // 拷贝委派令牌状态
    returnState.rmSecretManagerState.getTokenState().putAll(
      state.rmSecretManagerState.getTokenState());
    // 拷贝委派令牌序列号
    returnState.rmSecretManagerState.dtSequenceNumber =
        state.rmSecretManagerState.dtSequenceNumber;
    // 拷贝AMRM令牌密钥管理器状态
    returnState.amrmTokenSecretManagerState =
        state.amrmTokenSecretManagerState == null ? null
            : AMRMTokenSecretManagerState
              .newInstance(state.amrmTokenSecretManagerState);
    // 拷贝代理CA证书
    if (state.proxyCAState.getCaCert() != null) {
      byte[] caCertData = state.proxyCAState.getCaCert().getEncoded();
      returnState.proxyCAState.setCaCert(caCertData);
    }
    // 拷贝代理CA私钥
    if (state.proxyCAState.getCaPrivateKey() != null) {
      byte[] caPrivateKeyData
          = state.proxyCAState.getCaPrivateKey().getEncoded();
      returnState.proxyCAState.setCaPrivateKey(caPrivateKeyData);
    }
    return returnState;
  }
  
  @Override
  /** 初始化存储，设置epoch起始值 */
  public synchronized void initInternal(Configuration conf) {
    epoch = baseEpoch;
  }

  @Override
  protected synchronized void startInternal() throws Exception {
  }

  @Override
  protected synchronized void closeInternal() throws Exception {
  }

  @Override
  /** 存储新应用状态到内存 */
  public synchronized void storeApplicationStateInternal(
      ApplicationId appId, ApplicationStateData appState)
      throws Exception {
    state.appState.put(appId, appState);
  }

  @Override
  /** 更新应用最终状态，保留原有尝试记录 */
  public synchronized void updateApplicationStateInternal(
      ApplicationId appId, ApplicationStateData appState) 
      throws Exception {
    LOG.info("Updating final state " + appState.getState() + " for app: "
        + appId);
    if (state.appState.get(appId) != null) {
      // add the earlier attempts back
      appState.attempts.putAll(state.appState.get(appId).attempts);
    }
    state.appState.put(appId, appState);
  }

  @Override
  /** 存储新应用尝试状态到内存 */
  public synchronized void storeApplicationAttemptStateInternal(
      ApplicationAttemptId appAttemptId,
      ApplicationAttemptStateData attemptState)
      throws Exception {
    ApplicationStateData appState = state.getApplicationState().get(
        attemptState.getAttemptId().getApplicationId());
    if (appState == null) {
      throw new YarnRuntimeException("Application doesn't exist");
    }
    appState.attempts.put(attemptState.getAttemptId(), attemptState);
  }

  @Override
  /** 更新应用尝试最终状态 */
  public synchronized void updateApplicationAttemptStateInternal(
      ApplicationAttemptId appAttemptId,
      ApplicationAttemptStateData attemptState)
      throws Exception {
    ApplicationStateData appState =
        state.getApplicationState().get(appAttemptId.getApplicationId());
    if (appState == null) {
      throw new YarnRuntimeException("Application doesn't exist");
    }
    LOG.info("Updating final state " + attemptState.getState()
        + " for attempt: " + attemptState.getAttemptId());
    appState.attempts.put(attemptState.getAttemptId(), attemptState);
  }

  @Override
  /** 从内存移除应用尝试状态 */
  public synchronized void removeApplicationAttemptInternal(
      ApplicationAttemptId appAttemptId) throws Exception {
    ApplicationStateData appState =
        state.getApplicationState().get(appAttemptId.getApplicationId());
    ApplicationAttemptStateData attemptState =
        appState.attempts.remove(appAttemptId);
    LOG.info("Removing state for attempt: " + appAttemptId);
    if (attemptState == null) {
      throw new YarnRuntimeException("Application doesn't exist");
    }
  }

  @Override
  /** 从内存移除整个应用状态 */
  public synchronized void removeApplicationStateInternal(
      ApplicationStateData appState) throws Exception {
    ApplicationId appId =
        appState.getApplicationSubmissionContext().getApplicationId();
    ApplicationStateData removed = state.appState.remove(appId);

    if (removed == null) {
      throw new YarnRuntimeException("Removing non-existing application state");
    }
  }

  /**
   * 存储或更新RM委派令牌状态到内存
   * @param rmDTIdentifier RM委派令牌标识符
   * @param renewDate 更新时间
   * @param isUpdate 是否是更新操作
   */
  private void storeOrUpdateRMDT(RMDelegationTokenIdentifier rmDTIdentifier,
      Long renewDate, boolean isUpdate) throws Exception {
    Map<RMDelegationTokenIdentifier, Long> rmDTState =
        state.rmSecretManagerState.getTokenState();
    if (rmDTState.containsKey(rmDTIdentifier)) {
      IOException e = new IOException("RMDelegationToken: " + rmDTIdentifier
          + "is already stored.");
      LOG.info("Error storing info for RMDelegationToken: " + rmDTIdentifier, e);
      throw e;
    }
    rmDTState.put(rmDTIdentifier, renewDate);
    if(!isUpdate) {
      state.rmSecretManagerState.dtSequenceNumber = 
          rmDTIdentifier.getSequenceNumber();
    }
    LOG.info("Store RMDT with sequence number "
             + rmDTIdentifier.getSequenceNumber());
  }

  @Override
  /** 存储新RM委派令牌状态 */
  public synchronized void storeRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception {
    storeOrUpdateRMDT(rmDTIdentifier, renewDate, false);
  }

  @Override
  /** 从内存移除RM委派令牌状态 */
  public synchronized void removeRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier) throws Exception{
    Map<RMDelegationTokenIdentifier, Long> rmDTState =
        state.rmSecretManagerState.getTokenState();
    rmDTState.remove(rmDTIdentifier);
    LOG.info("Remove RMDT with sequence number "
        + rmDTIdentifier.getSequenceNumber());
  }

  @Override
  /** 更新RM委派令牌状态，先删后加 */
  protected synchronized void updateRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception {
    removeRMDelegationTokenState(rmDTIdentifier);
    storeOrUpdateRMDT(rmDTIdentifier, renewDate, true);
    LOG.info("Update RMDT with sequence number "
        + rmDTIdentifier.getSequenceNumber());
  }

  @Override
  /** 存储RM委派令牌主密钥到内存 */
  public synchronized void storeRMDTMasterKeyState(DelegationKey delegationKey)
      throws Exception {
    Set<DelegationKey> rmDTMasterKeyState =
        state.rmSecretManagerState.getMasterKeyState();

    if (rmDTMasterKeyState.contains(delegationKey)) {
      IOException e = new IOException("RMDTMasterKey with keyID: "
              + delegationKey.getKeyId() + " is already stored");
      LOG.info("Error storing info for RMDTMasterKey with keyID: "
          + delegationKey.getKeyId(), e);
      throw e;
    }
    state.getRMDTSecretManagerState().getMasterKeyState().add(delegationKey);
    LOG.info("Store RMDT master key with key id: " + delegationKey.getKeyId()
        + ". Currently rmDTMasterKeyState size: " + rmDTMasterKeyState.size());
  }

  @Override
  /** 从内存移除RM委派令牌主密钥 */
  public synchronized void removeRMDTMasterKeyState(DelegationKey delegationKey)
      throws Exception {
    LOG.info("Remove RMDT master key with key id: " + delegationKey.getKeyId());
    Set<DelegationKey> rmDTMasterKeyState =
        state.rmSecretManagerState.getMasterKeyState();
    rmDTMasterKeyState.remove(delegationKey);
  }

  @Override
  /** 存储资源预约分配状态到内存 */
  protected synchronized void storeReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) throws Exception {
    LOG.info("Storing reservationallocation for " + reservationIdName + " " +
            "for plan " + planName);
    Map<ReservationId, ReservationAllocationStateProto> planState =
        state.getReservationState().get(planName);
    if (planState == null) {
      planState = new HashMap<>();
      state.getReservationState().put(planName, planState);
    }
    ReservationId reservationId =
        ReservationId.parseReservationId(reservationIdName);
    planState.put(reservationId, reservationAllocation);
  }

  @Override
  /** 从内存移除资源预约状态 */
  protected synchronized void removeReservationState(
      String planName, String reservationIdName) throws Exception {
    LOG.info("Removing reservationallocation " + reservationIdName
              + " for plan " + planName);

    Map<ReservationId, ReservationAllocationStateProto> planState =
        state.getReservationState().get(planName);
    if (planState == null) {
      throw new YarnRuntimeException("State for plan " + planName + " does " +
          "not exist");
    }
    ReservationId reservationId =
        ReservationId.parseReservationId(reservationIdName);
    planState.remove(reservationId);
    if (planState.isEmpty()) {
      state.getReservationState().remove(planName);
    }
  }

  @Override
  /** 存储代理CA证书和私钥到内存 */
  protected void storeProxyCACertState(
      X509Certificate caCert, PrivateKey caPrivateKey) throws Exception {
    state.getProxyCAState().setCaCert(caCert);
    state.getProxyCAState().setCaPrivateKey(caPrivateKey);
  }

  @Override
  protected Version loadVersion() throws Exception {
    return null;
  }

  @Override
  protected void storeVersion() throws Exception {
  }

  @Override
  protected Version getCurrentVersion() {
    return null;
  }

  @Override
  /** 存储或更新AMRM令牌密钥管理器状态到内存 */
  public synchronized void storeOrUpdateAMRMTokenSecretManagerState(
      AMRMTokenSecretManagerState amrmTokenSecretManagerState,
      boolean isUpdate) {
    if (amrmTokenSecretManagerState != null) {
      state.amrmTokenSecretManagerState = AMRMTokenSecretManagerState
          .newInstance(amrmTokenSecretManagerState);
    }
  }

  @Override
  public void deleteStore() throws Exception {
  }

  @Override
  public void removeApplication(ApplicationId removeAppId) throws Exception {
  }

}