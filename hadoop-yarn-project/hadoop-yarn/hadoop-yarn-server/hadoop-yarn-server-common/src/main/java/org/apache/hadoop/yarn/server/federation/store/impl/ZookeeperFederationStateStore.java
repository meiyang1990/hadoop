// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.impl;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.Comparator;
import java.util.stream.Collectors;

import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterInfoProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterPolicyConfigurationProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ApplicationHomeSubClusterProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeletePoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeletePoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterIdPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterInfoPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.SubClusterPolicyConfigurationPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.impl.pb.ApplicationHomeSubClusterPBImpl;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationApplicationHomeSubClusterStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationMembershipStateStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationPolicyStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationReservationHomeSubClusterStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationRouterRMTokenInputValidator;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;

import static org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils.filterHomeSubCluster;
import static org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.ZK_DTSM_TOKEN_SEQNUM_BATCH_SIZE;
import static org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.ZK_DTSM_TOKEN_SEQNUM_BATCH_SIZE_DEFAULT;
import static org.apache.hadoop.util.curator.ZKCuratorManager.getNodePath;

/**
 * 基于ZooKeeper实现的YARN联邦状态存储，持久化存储联邦集群的成员信息、应用映射、调度策略、预约信息和代理令牌
 * ZooKeeper implementation of {@link FederationStateStore}.
 * The znode structure is as follows:
 *
 * ROOT_DIR_PATH
 * |--- MEMBERSHIP
 * |     |----- SC1
 * |     |----- SC2
 * |--- APPLICATION
 * |     |----- HIERARCHIES
 * |     |        |----- 1
 * |     |        |      |----- (#ApplicationId barring last character)
 * |     |        |      |       |       |----- APP Data
 * |     |        |      ....
 * |     |        |
 * |     |        |----- 2
 * |     |        |      |----- (#ApplicationId barring last 2 characters)
 * |     |        |      |       |----- (#Last 2 characters of ApplicationId)
 * |     |        |      |       |       |----- APP Data
 * |--- POLICY
 * |     |----- QUEUE1
 * |     |----- QUEUE1
 * |--- RESERVATION
 * |     |----- RESERVATION1
 * |     |----- RESERVATION2
 * |--- ROUTER_RM_DT_SECRET_MANAGER_ROOT
 * |     |----- ROUTER_RM_DELEGATION_TOKENS_ROOT
 * |     |       |----- RM_DELEGATION_TOKEN_1
 * |     |       |----- RM_DELEGATION_TOKEN_2
 * |     |       |----- RM_DELEGATION_TOKEN_3
 * |     |----- ROUTER_RM_DT_MASTER_KEYS_ROOT
 * |     |       |----- DELEGATION_KEY_1
 * |     |----- ROUTER_RM_DT_SEQUENTIAL_NUMBER
 */
public class ZookeeperFederationStateStore implements FederationStateStore {

  private static final Logger LOG =
      LoggerFactory.getLogger(ZookeeperFederationStateStore.class);

  private final static String ROOT_ZNODE_NAME_MEMBERSHIP = "memberships";
  private final static String ROOT_ZNODE_NAME_APPLICATION = "applications";
  private final static String ROOT_ZNODE_NAME_POLICY = "policies";
  private final static String ROOT_ZNODE_NAME_RESERVATION = "reservation";

  protected static final String ROOT_ZNODE_NAME_VERSION = "version";

  /** 代理RM委派令牌根节点名称. */
  private final static String ROUTER_RM_DT_SECRET_MANAGER_ROOT = "router_rm_dt_secret_manager_root";
  private static final String ROUTER_RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME =
      "router_rm_dt_master_keys_root";
  private static final String ROUTER_RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME =
      "router_rm_delegation_tokens_root";
  private static final String ROUTER_RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME =
      "router_rm_dt_sequential_number";
  private static final String ROUTER_RM_DT_MASTER_KEY_ID_ZNODE_NAME =
      "router_rm_dt_master_key_id";
  private static final String ROUTER_RM_DELEGATION_KEY_PREFIX = "delegation_key_";
  private static final String ROUTER_RM_DELEGATION_TOKEN_PREFIX = "rm_delegation_token_";

  /** ZooKeeper连接管理器. */
  private ZKCuratorManager zkManager;

  /** 序列号批次分配相关变量. **/
  private int seqNumBatchSize;
  private int currentSeqNum;
  private int currentMaxSeqNum;
  private SharedCount delTokSeqCounter;
  private SharedCount keyIdSeqCounter;

  /** 状态存储根ZNode路径. */
  private String baseZNode;

  private String appsZNode;
  private String membershipZNode;
  private String policiesZNode;
  private String reservationsZNode;
  private String versionNode;
  private int maxAppsInStateStore;

  /** 委派令牌存储路径相关变量. **/
  private Map<Integer, String> routerAppRootHierarchies;
  private String routerRMDTSecretManagerRoot;
  private String routerRMDTMasterKeysRootPath;
  private String routerRMDelegationTokensRootPath;
  private String routerRMSequenceNumberPath;
  private String routerRMMasterKeyIdPath;

  private int appIdNodeSplitIndex = 0;
  private final static int HIERARCHIES_LEVEL = 4;

  @VisibleForTesting
  public static final String ROUTER_APP_ROOT_HIERARCHIES = "HIERARCHIES";

  private volatile Clock clock = SystemClock.getInstance();

  protected static final Version CURRENT_VERSION_INFO = Version.newInstance(1, 1);

  @VisibleForTesting
  private ZKFederationStateStoreOpDurations opDurations =
      ZKFederationStateStoreOpDurations.getInstance();

  private Configuration configuration;

  /*
   * 应用尝试操作类型枚举
   * Indicates different app attempt state store operations.
   */
  private enum AppAttemptOp {
    STORE,
    UPDATE,
    REMOVE
  };

  /**
   * 封装应用节点路径和切分索引信息
   * Encapsulates full app node path and corresponding split index.
   */
  private final static class AppNodeSplitInfo {
    private final String path;
    private final int splitIndex;
    AppNodeSplitInfo(String path, int splitIndex) {
      this.path = path;
      this.splitIndex = splitIndex;
    }
  }

  @Override
  public void init(Configuration conf) throws YarnException {

    LOG.info("Initializing ZooKeeper connection");
    this.configuration = conf;
    // 读取状态存储最大应用数量配置
    maxAppsInStateStore = conf.getInt(
       YarnConfiguration.FEDERATION_STATESTORE_MAX_APPLICATIONS,
       YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_MAX_APPLICATIONS);

    // 读取ZNode根路径和ZooKeeper地址配置
    baseZNode = conf.get(
        YarnConfiguration.FEDERATION_STATESTORE_ZK_PARENT_PATH,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_ZK_PARENT_PATH);
    String zkHostPort = conf.get(YarnConfiguration.FEDERATION_STATESTORE_ZK_ADDRESS);
    try {
      // 初始化ZooKeeper连接
      this.zkManager = new ZKCuratorManager(conf);
      this.zkManager.start(zkHostPort);
    } catch (IOException e) {
      LOG.error("Cannot initialize the ZK connection", e);
    }

    // 初始化各个模块根ZNode路径
    membershipZNode = getNodePath(baseZNode, ROOT_ZNODE_NAME_MEMBERSHIP);
    appsZNode = getNodePath(baseZNode, ROOT_ZNODE_NAME_APPLICATION);
    policiesZNode = getNodePath(baseZNode, ROOT_ZNODE_NAME_POLICY);
    reservationsZNode = getNodePath(baseZNode, ROOT_ZNODE_NAME_RESERVATION);
    versionNode = getNodePath(baseZNode, ROOT_ZNODE_NAME_VERSION);

    // 初始化应用ID分层哈希路径
    initHierarchiesPath();

    // 读取并校验应用ID切分索引配置
    appIdNodeSplitIndex = conf.getInt(YarnConfiguration.ZK_APPID_NODE_SPLIT_INDEX,
         YarnConfiguration.DEFAULT_ZK_APPID_NODE_SPLIT_INDEX);
    if (appIdNodeSplitIndex < 1 || appIdNodeSplitIndex > HIERARCHIES_LEVEL) {
      LOG.info("Invalid value {} for config {} specified. Resetting it to {}",
          appIdNodeSplitIndex, YarnConfiguration.ZK_APPID_NODE_SPLIT_INDEX,
          YarnConfiguration.DEFAULT_ZK_APPID_NODE_SPLIT_INDEX);
      appIdNodeSplitIndex = YarnConfiguration.DEFAULT_ZK_APPID_NODE_SPLIT_INDEX;
    }

    // 初始化委派令牌相关ZNode路径
    routerRMDTSecretManagerRoot = getNodePath(baseZNode, ROUTER_RM_DT_SECRET_MANAGER_ROOT);
    routerRMDTMasterKeysRootPath = getNodePath(routerRMDTSecretManagerRoot,
        ROUTER_RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME);
    routerRMDelegationTokensRootPath = getNodePath(routerRMDTSecretManagerRoot,
        ROUTER_RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME);
    routerRMSequenceNumberPath = getNodePath(routerRMDTSecretManagerRoot,
        ROUTER_RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME);
    routerRMMasterKeyIdPath =