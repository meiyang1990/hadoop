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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.util.curator.ZKCuratorManager.SafeTransaction;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.Epoch;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMDelegationTokenIdentifierData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.AMRMTokenSecretManagerStatePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.EpochPBImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 基于ZooKeeper实现的RM状态存储，用于YARN HA模式下ResourceManager状态恢复
 * 
 * ZK节点结构如下：
 * ROOT_DIR_PATH
 * |--- VERSION_INFO
 * |--- EPOCH_NODE
 * |--- RM_ZK_FENCING_LOCK
 * |--- RM_APP_ROOT
 * |     |----- HIERARCHIES
 * |     |        |----- 1
 * |     |        |      |----- (#ApplicationId 去除最后一个字符)
 * |     |        |      |       |----- (#ApplicationId最后一个字符)
 * |     |        |      |       |       |----- (#ApplicationAttemptIds)
 * |     |        |      ....
 * |     |        |
 * |     |        |----- 2
 * |     |        |      |----- (#ApplicationId 去除最后2个字符)
 * |     |        |      |       |----- (#ApplicationId最后2个字符)
 * |     |        |      |       |       |----- (#ApplicationAttemptIds)
 * |     |        |      ....
 * |     |        |
 * |     |        |----- 3
 * |     |        |      |----- (#ApplicationId 去除最后3个字符)
 * |     |        |      |       |----- (#ApplicationId最后3个字符)
 * |     |        |      |       |       |----- (#ApplicationAttemptIds)
 * |     |        |      ....
 * |     |        |
 * |     |        |----- 4
 * |     |        |      |----- (#ApplicationId 去除最后4个字符)
 * |     |        |      |       |----- (#ApplicationId最后4个字符)
 * |     |        |      |       |       |----- (#ApplicationAttemptIds)
 * |     |        |      ....
 * |     |        |
 * |     |----- (#ApplicationId1)
 * |     |        |----- (#ApplicationAttemptIds)
 * |     |
 * |     |----- (#ApplicationId2)
 * |     |       |----- (#ApplicationAttemptIds)
 * |     ....
 * |
 * |--- RM_DT_SECRET_MANAGER_ROOT
 *        |----- RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME
 *        |----- RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME
 *        |       |----- 1
 *        |       |      |----- (#TokenId 去除最后一个字符)
 *        |       |      |       |----- (#TokenId最后一个字符)
 *        |       |      ....
 *        |       |----- 2
 *        |       |      |----- (#TokenId 去除最后2个字符)
 *        |       |      |       |----- (#TokenId最后2个字符)
 *        |       |      ....
 *        |       |----- 3
 *        |       |      |----- (#TokenId 去除最后3个字符)
 *        |       |      |       |----- (#TokenId最后3个字符)
 *        |       |      ....
 *        |       |----- 4
 *        |       |      |----- (#TokenId 去除最后4个字符)
 *        |       |      |       |----- (#TokenId最后4个字符)
 *        |       |      ....
 *        |       |----- Token_1
 *        |       |----- Token_2
 *        |       ....
 *        |
 *        |----- RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME
 *        |      |----- Key_1
 *        |      |----- Key_2
 *                ....
 * |--- AMRMTOKEN_SECRET_MANAGER_ROOT
 *        |----- currentMasterKey
 *        |----- nextMasterKey
 *
 * |-- RESERVATION_SYSTEM_ROOT
 *        |------PLAN_1
 *        |      |------ RESERVATION_1
 *        |      |------ RESERVATION_2
 *        |      ....
 *        |------PLAN_2
 *        ....
 * |-- PROXY_CA_ROOT
 *        |----- caCert
 *        |----- caPrivateKey
 *
 * 版本变更说明:
 * 1.1 -> 1.2: 新增独立存储AMRMTokenSecretManager状态，保存currentMasterkey和nextMasterkey，从ApplicationAttemptState移除AMRMToken
 * 1.2 -> 1.3: 新增ReservationSystem状态存储
 * 1.3 -> 1.4: 变更应用节点结构，根据可配置分割索引拆分节点分为两级，限制单次加载应用状态时返回的节点数量
 * 1.4 -> 1.5: 变更委托令牌节点结构，根据可配置分割索引拆分节点分为两级，限制单次加载令牌状态时返回的节点数量
 */
@Private
@Unstable
public class ZKRMStateStore extends RMStateStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(ZKRMStateStore.class);

  private static final String RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME =
      "RMDelegationTokensRoot";
  private static final String RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME =
      "RMDTSequentialNumber";
  private static final String RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME =
      "RMDTMasterKeysRoot";
  @VisibleForTesting
  public static final String ROOT_ZNODE_NAME = "ZKRMStateRoot";
  protected static final Version CURRENT_VERSION_INFO = Version
      .newInstance(1, 5);
  @VisibleForTesting
  public static final String RM_APP_ROOT_HIERARCHIES = "HIERARCHIES";

  /* Znode路径 */
  private String zkRootNodePath;
  private String rmAppRoot;
  private Map<Integer, String> rmAppRootHierarchies;
  private Map<Integer, String> rmDelegationTokenHierarchies;
  private String rmDTSecretManagerRoot;
  private String dtMasterKeysRootPath;
  private String delegationTokensRootPath;
  private String dtSequenceNumberPath;
  private String amrmTokenSecretManagerRoot;
  private String reservationRoot;
  private String proxyCARoot;

  @VisibleForTesting
  protected String znodeWorkingPath;
  private int appIdNodeSplitIndex = 0;
  @VisibleForTesting
  protected int delegationTokenNodeSplitIndex = 0;

  /* 隔离相关变量 */
  private static final String FENCING_LOCK = "RM_ZK_FENCING_LOCK";
  private String fencingNodePath;
  private Thread verifyActiveStatusThread;
  private int zkSessionTimeout;
  private int zknodeLimit;

  /* ACL和认证信息 */
  private List<ACL> zkAcl;
  @VisibleForTesting
  List<ACL> zkRootNodeAcl;
  private String zkRootNodeUsername;

  private static final int CREATE_DELETE_PERMS =
      ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE;
  private final String zkRootNodeAuthScheme =
      new DigestAuthenticationProvider().getScheme();

  /** ZooKeeper连接管理器 */
  private ZKCuratorManager zkManager;

  private volatile Clock clock = SystemClock.getInstance();
  @VisibleForTesting
  protected ZKRMStateStoreOpDurations opDurations;

  /*
   * 标识不同的应用尝试状态存储操作类型
   */
  private enum AppAttemptOp {
    STORE,
    UPDATE,
    REMOVE
  };

  /**
   * 封装分层节点布局的节点路径与对应分割索引
   */
  private final static class ZnodeSplitInfo {
    private final String path;
    private final int splitIndex;
    ZnodeSplitInfo(String path, int splitIndex) {
      this.path = path;
      this.splitIndex = splitIndex;
    }
  }

  /**
   * 根据传入的配置和源ACL，构造存储根节点的ACL列表
   * 构造后，源ACL允许的所有用户获得读写管理员权限，当前RM获得独占创建删除权限
   * 仅当HA启用且配置未设置根节点ACL时调用
   * @param conf 配置对象
   * @param sourceACLs 源ACL列表
   * @return 存储根节点的ACL列表
   * @throws java.security.NoSuchAlgorithmException ZK摘要算法找不到抛出
   */
  @VisibleForTesting
  @Private
  @Unstable
  protected List<ACL> constructZkRootNodeACL(Configuration conf,
      List<ACL> sourceACLs) throws NoSuchAlgorithmException {
    List<ACL> zkRootNodeAclList = new ArrayList<>();

    // 遍历源ACL，移除创建删除权限添加到结果列表
    for (ACL acl : sourceACLs) {
      zkRootNodeAclList.add(new ACL(
          ZKUtil.removeSpecificPerms(acl.getPerms(), CREATE_DELETE_PERMS),
          acl.getId()));
    }

    // 获取当前RM实例标识
    zkRootNodeUsername = HAUtil.getConfValueForRMInstance(
        YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS, conf);
    // 生成RM用户ID，使用用户名加密码生成摘要
    Id rmId = new Id(zkRootNodeAuthScheme,
        DigestAuthenticationProvider.generateDigest(zkRootNodeUsername + ":"
            + resourceManager.getZkRootNodePassword()));
    // 添加当前RM的创建删除权限
    zkRootNodeAclList.add(new ACL(CREATE_DELETE_PERMS, rmId));

    return zkRootNodeAclList;
  }

  @Override
  public synchronized void initInternal(Configuration conf)
      throws IOException, NoSuchAlgorithmException {
    /* 初始化隔离相关路径、ACL和操作 */
    // 读取工作父路径配置
    znodeWorkingPath =
        conf.get(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH,
            YarnConfiguration.DEFAULT_ZK_RM_STATE_STORE_PARENT_PATH);
    // 拼接各个核心节点路径
    zkRootNodePath = getNodePath(znodeWorkingPath, ROOT_ZNODE_NAME);
    rmAppRoot = getNodePath(zkRootNodePath, RM_APP_ROOT);
    String hierarchiesPath = getNodePath(rmAppRoot, RM_APP_ROOT_HIERARCHIES);
    rmAppRootHierarchies = new HashMap<>(5);
    rmAppRootHierarchies.put(0, rmAppRoot);
    // 预生成1-4级分层的节点路径
    for (int splitIndex = 1; splitIndex <= 4; splitIndex++) {
      rmAppRootHierarchies.put(splitIndex,
          getNodePath(hierarchiesPath, Integer.toString(splitIndex)));
    }

    // 隔离锁节点路径
    fencingNodePath = getNodePath(zkRootNodePath, FENCING_LOCK);
    // 读取ZK会话超时配置
    zkSessionTimeout = conf.getInt(YarnConfiguration.RM_ZK_TIMEOUT_MS,
        YarnConfiguration.DEFAULT_RM_ZK_TIMEOUT_MS);
    // 读取单个znode最大大小限制
    zknodeLimit = conf.getInt(YarnConfiguration.RM_ZK_ZNODE_SIZE_LIMIT_BYTES,
        YarnConfiguration.DEFAULT_RM_ZK_ZNODE_SIZE_LIMIT_BYTES);

    // 读取应用ID节点分割索引配置，校验并修正非法值
    appIdNodeSplitIndex =
        conf.getInt(YarnConfiguration.ZK_APPID_NODE_SPLIT_INDEX,
            YarnConfiguration.DEFAULT_ZK_APPID_NODE_SPLIT_INDEX);
    if (appIdNodeSplitIndex < 0 || appIdNodeSplitIndex > 4) {
      LOG.info("Invalid value " + appIdNodeSplitIndex + " for config " +
          YarnConfiguration.ZK_APPID_NODE_SPLIT_INDEX + " specified. " +
              "Resetting it to " +
                  YarnConfiguration.DEFAULT_ZK_APPID_NODE_SPLIT_INDEX);
      appIdNodeSplitIndex = YarnConfiguration.DEFAULT_ZK_APPID_NODE_SPLIT_INDEX;
    }

    // 初始化操作耗时统计器
    opDurations = ZKRMStateStoreOpDurations.getInstance();

    // 读取通用ZK ACL配置
    zkAcl = ZKCuratorManager.getZKAcls(conf);

    // HA启用场景下处理根节点ACL
    if (HAUtil.isHAEnabled(conf)) {
      String zkRootNodeAclConf = HAUtil.getConfValueForRMInstance
          (YarnConfiguration.ZK_RM_STATE_STORE_ROOT_NODE_ACL, conf);
      if (zkRootNodeAclConf != null) {
        zkRootNodeAclConf = ZKUtil.resolveConfIndirection(zkRootNodeAclConf);
        // 解析ACL配置
        try {
          zkRootNodeAcl = ZKUtil.parseACLs(zkRootNodeAclConf);
        } catch (ZKUtil.BadAclFormatException bafe) {
          LOG.error("Invalid format for "
              + YarnConfiguration.ZK_RM_STATE_STORE_ROOT_NODE_ACL);
          throw bafe;
        }
      } else {
        // 配置未设置则自动构造根节点ACL
        zkRootNodeAcl = constructZkRootNodeACL(conf, zkAcl);
      }
    }

    // 拼接委托密钥管理器根节点路径
    rmDTSecretManagerRoot =
        getNodePath(zkRootNodePath, RM_DT_SECRET_MANAGER_ROOT);
    dtMasterKeysRootPath = getNodePath(rmDTSecretManagerRoot,
        RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME