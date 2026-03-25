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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.io.ByteArrayInputStream;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.crypto.SecretKey;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.SettableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.RMFatalEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMFatalEventType;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AggregateAppResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

@Private
@Unstable
/**
 * ResourceManager状态存储抽象基类，为RM HA场景下的状态恢复提供统一存储接口
 * 封装异步事件处理逻辑，子类只需要实现阻塞式的存储/加载方法即可完成持久化功能
 */
public abstract class RMStateStore extends AbstractService {

  // 各类状态存储根节点路径常量定义
  @VisibleForTesting
  public static final String RM_APP_ROOT = "RMAppRoot";
  protected static final String RM_DT_SECRET_MANAGER_ROOT = "RMDTSecretManagerRoot";
  protected static final String RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME =
      "RMDelegationTokensRoot";
  protected static final String DELEGATION_KEY_PREFIX = "DelegationKey_";
  protected static final String DELEGATION_TOKEN_PREFIX = "RMDelegationToken_";
  protected static final String DELEGATION_TOKEN_SEQUENCE_NUMBER_PREFIX =
      "RMDTSequenceNumber_";
  protected static final String AMRMTOKEN_SECRET_MANAGER_ROOT =
      "AMRMTokenSecretManagerRoot";
  protected static final String RESERVATION_SYSTEM_ROOT =
      "ReservationSystemRoot";
  protected static final String PROXY_CA_ROOT = "ProxyCARoot";
  protected static final String PROXY_CA_CERT_NODE = "caCert";
  protected static final String PROXY_CA_PRIVATE_KEY_NODE = "caPrivateKey";
  protected static final String VERSION_NODE = "RMVersionNode";
  protected static final String EPOCH_NODE = "EpochNode";
  protected long baseEpoch;
  private long epochRange;
  protected ResourceManager resourceManager;
  private final ReadLock readLock;
  private final WriteLock writeLock;

  public static final Logger LOG =
      LoggerFactory.getLogger(RMStateStore.class);

  /**
   * RM状态存储自身状态枚举
   */
  public enum RMStateStoreState {
    ACTIVE,  // 活跃状态，可处理存储请求
    FENCED   // 隔离状态，HA场景下当前RM备节点切换为主节点前旧主节点被隔离
  };

  // RM状态存储状态机工厂定义所有事件与状态迁移规则
  private static final StateMachineFactory<RMStateStore,
                                           RMStateStoreState,
                                           RMStateStoreEventType, 
                                           RMStateStoreEvent>
      stateMachineFactory = new StateMachineFactory<RMStateStore,
                                                    RMStateStoreState,
                                                    RMStateStoreEventType,
                                                    RMStateStoreEvent>(
      RMStateStoreState.ACTIVE)
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.STORE_APP, new StoreAppTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.UPDATE_APP, new UpdateAppTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.REMOVE_APP, new RemoveAppTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.STORE_APP_ATTEMPT,
          new StoreAppAttemptTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.UPDATE_APP_ATTEMPT,
          new UpdateAppAttemptTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.REMOVE_APP_ATTEMPT,
          new RemoveAppAttemptTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.STORE_MASTERKEY,
          new StoreRMDTMasterKeyTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.REMOVE_MASTERKEY,
          new RemoveRMDTMasterKeyTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.STORE_DELEGATION_TOKEN,
          new StoreRMDTTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.REMOVE_DELEGATION_TOKEN,
          new RemoveRMDTTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.UPDATE_DELEGATION_TOKEN,
          new UpdateRMDTTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.UPDATE_AMRM_TOKEN,
          new StoreOrUpdateAMRMTokenTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.STORE_RESERVATION,
          new StoreReservationAllocationTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.REMOVE_RESERVATION,
          new RemoveReservationAllocationTransition())
      .addTransition(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED,
          RMStateStoreEventType.FENCED)
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.STORE_PROXY_CA_CERT,
          new StoreProxyCACertTransition())
      .addTransition(RMStateStoreState.FENCED, RMStateStoreState.FENCED,
          EnumSet.of(
          RMStateStoreEventType.STORE_APP,
          RMStateStoreEventType.UPDATE_APP,
          RMStateStoreEventType.REMOVE_APP,
          RMStateStoreEventType.STORE_APP_ATTEMPT,
          RMStateStoreEventType.UPDATE_APP_ATTEMPT,
          RMStateStoreEventType.FENCED,
          RMStateStoreEventType.STORE_MASTERKEY,
          RMStateStoreEventType.REMOVE_MASTERKEY,
          RMStateStoreEventType.STORE_DELEGATION_TOKEN,
          RMStateStoreEventType.REMOVE_DELEGATION_TOKEN,
          RMStateStoreEventType.UPDATE_DELEGATION_TOKEN,
          RMStateStoreEventType.UPDATE_AMRM_TOKEN,
          RMStateStoreEventType.STORE_RESERVATION,
          RMStateStoreEventType.REMOVE_RESERVATION,
          RMStateStoreEventType.STORE_PROXY_CA_CERT));

  private final StateMachine<RMStateStoreState,
                             RMStateStoreEventType,
                             RMStateStoreEvent> stateMachine;

  /**
   * 存储新应用状态的状态迁移处理
   */
  private static class StoreAppTransition
      implements MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreAppEvent)) {
        // 不应该发生
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      ApplicationStateData appState =
          ((RMStateStoreAppEvent) event).getAppState();
      ApplicationId appId =
          appState.getApplicationSubmissionContext().getApplicationId();
      LOG.info("Storing info for app: " + appId);
      try {
        // 调用子类实现的存储方法
        store.storeApplicationStateInternal(appId, appState);
        // 通知应用存储完成
        store.notifyApplication(
            new RMAppEvent(appId, RMAppEventType.APP_NEW_SAVED));
      } catch (Exception e) {
        LOG.error("Error storing app: " + appId, e);
        if (e instanceof StoreLimitException) {
          // 存储容量超限，通知应用保存失败
          store.notifyApplication(
              new RMAppEvent(appId, RMAppEventType.APP_SAVE_FAILED,
                  e.getMessage()));
        } else {
          // 其他错误，处理存储失败，可能切换到隔离状态
          isFenced = store.notifyStoreOperationFailedInternal(e);
        }
      }
      return finalState(isFenced);
    };

  }

  /**
   * 更新应用状态的状态迁移处理
   */
  private static class UpdateAppTransition implements
      MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateUpdateAppEvent)) {
        // 不应该发生
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      ApplicationStateData appState =
          ((RMStateUpdateAppEvent) event).getAppState();
      SettableFuture<Object> result =
          ((RMStateUpdateAppEvent) event).getResult();
      ApplicationId appId =
          appState.getApplicationSubmissionContext().getApplicationId();
      LOG.info("Updating info for app: " + appId);
      try {
        // 如果应用已经进入终态，修剪冗余信息节省存储空间
        if (isAppStateFinal(appState)) {
          pruneAppState(appState);
        }
        // 调用子类实现更新方法
        store.updateApplicationStateInternal(appId, appState);
        // 如果需要，通知应用更新完成
        if (((RMStateUpdateAppEvent) event).isNotifyApplication()) {
          store.notifyApplication(new RMAppEvent(appId,
              RMAppEventType.APP_UPDATE_SAVED));
        }

        // 设置异步结果
        if (result != null) {
          result.set(null);
        }

      } catch (Exception e) {
        String msg = "Error updating app: " + appId;
        LOG.error(msg, e);
        // 处理存储失败
        isFenced = store.notifyStoreOperationFailedInternal(e);
        // 设置异步结果异常
        if (result != null) {
          result.setException(new YarnException(msg, e));
        }
      }
      return finalState(isFenced);
    }

    /** 判断应用状态是否为终态 */
    private boolean isAppStateFinal(ApplicationStateData appState) {
      RMAppState state = appState.getState();
      return state == RMAppState.FINISHED || state == RMAppState.FAILED ||
          state == RMAppState.KILLED;
    }

    /** 修剪终态应用的冗余信息，仅保留恢复所需的必要字段 */
    private void pruneAppState(ApplicationStateData appState) {
      ApplicationSubmissionContext srcCtx =
          appState.getApplicationSubmissionContext();
      ApplicationSubmissionContextPBImpl context =
          new ApplicationSubmissionContextPBImpl();
      // 仅保留恢复所需字段，其他字段清除
      context.setApplicationId(srcCtx.getApplicationId());
      context.setResource(srcCtx.getResource());
      context.setQueue(srcCtx.getQueue());
      context.setAMContainerResourceRequests(
          srcCtx.getAMContainerResourceRequests());
      context.setApplicationName(srcCtx.getApplicationName());
      context.setPriority(srcCtx.getPriority());
      context.setApplicationTags(srcCtx.getApplicationTags());
      context.setApplicationType(srcCtx.getApplicationType());
      context.setUnmanagedAM(srcCtx.getUnmanagedAM());
      context.setNodeLabelExpression(srcCtx.getNodeLabelExpression());
      ContainerLaunchContextPBImpl amContainerSpec =
              new ContainerLaunchContextPBImpl();
      amContainerSpec.setApplicationACLs(
              srcCtx.getAMContainerSpec().getApplicationACLs());
      context.setAMContainerSpec(amContainerSpec);
      appState.setApplicationSubmissionContext(context);
    }
  }

  /**
   * 删除应用状态的状态迁移处理
   */
  private static class RemoveAppTransition implements
      MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreRemoveAppEvent)) {
        // 不应该发生
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      ApplicationStateData appState =
          ((RMStateStoreRemoveAppEvent) event).getApp