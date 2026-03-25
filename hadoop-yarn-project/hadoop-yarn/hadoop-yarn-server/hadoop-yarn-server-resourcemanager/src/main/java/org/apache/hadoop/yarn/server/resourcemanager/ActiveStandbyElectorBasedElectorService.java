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
package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.ActiveStandbyElector;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

<<<<<<< HEAD
  /**
   * 基于 ActiveStandbyElector 实现的 ResourceManager Leader 选举服务。
   * 负责在 HA 模式下通过 ZooKeeper 自动选举 Active RM。
   */
=======
/**
 * 基于 ActiveStandbyElector 实现的YARN ResourceManager 高可用主节点选举服务
 * 提供嵌入式自动故障转移的主备选举能力，基于Zookeeper实现
 */
>>>>>>> a7f26154e2430da367a92d826f772851319cf52d
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ActiveStandbyElectorBasedElectorService extends AbstractService
    implements EmbeddedElector,
    ActiveStandbyElector.ActiveStandbyElectorCallback {
  private static final Logger LOG = LoggerFactory.
      getLogger(ActiveStandbyElectorBasedElectorService.class.getName());
  // 状态切换请求信息，来源为ZK自动选举
  private static final HAServiceProtocol.StateChangeRequestInfo req =
      new HAServiceProtocol.StateChangeRequestInfo(
          HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC);

  // 所属ResourceManager实例
  private ResourceManager rm;

  // 当前RM节点信息序列化字节数组，用于写入Zookeeper
  private byte[] localActiveNodeInfo;
  // ActiveStandbyElector实例，负责具体选举逻辑
  private ActiveStandbyElector elector;
  // Zookeeper会话超时时间，同时也是ZK断开连接后等待切换 standby 的超时时间
  private long zkSessionTimeout;
  // ZK断开连接后等待超时的定时器
  private Timer zkDisconnectTimer;
  @VisibleForTesting
  // ZK断开连接定时器的同步锁对象
  final Object zkDisconnectLock = new Object();

  /**
   * 构造主备选举服务，绑定所属ResourceManager
   * @param rm ResourceManager实例
   */
  ActiveStandbyElectorBasedElectorService(ResourceManager rm) {
    super(ActiveStandbyElectorBasedElectorService.class.getName());
    this.rm = rm;
  }

  @Override
  protected void serviceInit(Configuration conf)
      throws Exception {
    // 确保配置为YarnConfiguration实例
    conf = conf instanceof YarnConfiguration
        ? conf
        : new YarnConfiguration(conf);

    // 获取Zookeeper集群地址配置
    String zkQuorum = conf.get(YarnConfiguration.RM_ZK_ADDRESS);
    if (zkQuorum == null) {
      throw new YarnRuntimeException("Embedded automatic failover " +
          "is enabled, but " + YarnConfiguration.RM_ZK_ADDRESS +
          " is not set");
    }

    // 获取当前RM的HA ID和集群ID
    String rmId = HAUtil.getRMHAId(conf);
    String clusterId = YarnConfiguration.getClusterId(conf);
    // 序列化当前RM节点信息
    localActiveNodeInfo = createActiveNodeInfo(clusterId, rmId);

    // 获取Zookeeper上选举节点的基础路径
    String zkBasePath = conf.get(YarnConfiguration.AUTO_FAILOVER_ZK_BASE_PATH,
        YarnConfiguration.DEFAULT_AUTO_FAILOVER_ZK_BASE_PATH);
    // 拼接当前集群选举节点完整路径
    String electionZNode = zkBasePath + "/" + clusterId;

    // 读取ZK会话超时配置
    zkSessionTimeout = conf.getLong(YarnConfiguration.RM_ZK_TIMEOUT_MS,
        YarnConfiguration.DEFAULT_RM_ZK_TIMEOUT_MS);

    // 获取Zookeeper访问ACL和认证信息
    List<ACL> zkAcls = ZKCuratorManager.getZKAcls(conf);
    List<ZKUtil.ZKAuthInfo> zkAuths = ZKCuratorManager.getZKAuths(conf);

    // 获取Zookeeper操作最大重试次数配置
    int maxRetryNum =
        conf.getInt(YarnConfiguration.RM_HA_FC_ELECTOR_ZK_RETRIES_KEY, conf
          .getInt(CommonConfigurationKeys.HA_FC_ELECTOR_ZK_OP_RETRIES_KEY,
            CommonConfigurationKeys.HA_FC_ELECTOR_ZK_OP_RETRIES_DEFAULT));
    // 读取Zookeeper客户端SSL启用配置
    boolean isSSLEnabled =
        conf.getBoolean(CommonConfigurationKeys.ZK_CLIENT_SSL_ENABLED,
            conf.getBoolean(YarnConfiguration.RM_ZK_CLIENT_SSL_ENABLED,
                YarnConfiguration.DEFAULT_RM_ZK_CLIENT_SSL_ENABLED));
    // 如果启用SSL，初始化SSL信任库和密钥库
    SecurityUtil.TruststoreKeystore truststoreKeystore
            = isSSLEnabled ? new SecurityUtil.TruststoreKeystore(conf) : null;
    // 创建ActiveStandbyElector实例
    elector = new ActiveStandbyElector(zkQuorum, (int) zkSessionTimeout,
        electionZNode, zkAcls, zkAuths, this, maxRetryNum, false, truststoreKeystore);

    // 确保选举父节点存在
    elector.ensureParentZNode();
    // 检查父节点数据是否安全（集群ID匹配、格式正确）
    if (!isParentZnodeSafe(clusterId)) {
      notifyFatalError(String.format("invalid data in znode, %s, " +
          "which may require the state store to be reformatted",
          electionZNode));
    }

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // 加入主节点选举
    elector.joinElection(localActiveNodeInfo);
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    /**
     * When error occurs in serviceInit(), serviceStop() can be called.
     * We need null check for the case.
     */
    // 如果elector已初始化，退出选举并关闭连接
    if (elector != null) {
      elector.quitElection(false);
      elector.terminateConnection();
    }
    super.serviceStop();
  }

  @Override
  public void becomeActive() throws ServiceFailedException {
    // 取消ZK断开连接定时器
    cancelDisconnectTimer();

    try {
      // 通知RM Admin服务切换到Active状态
      rm.getRMContext().getRMAdminService().transitionToActive(req);
    } catch (Exception e) {
      throw new ServiceFailedException("RM could not transition to Active", e);
    }
  }

  @Override
  public void becomeStandby() {
    // 取消ZK断开连接定时器
    cancelDisconnectTimer();

    try {
      // 通知RM Admin服务切换到Standby状态
      rm.getRMContext().getRMAdminService().transitionToStandby(req);
    } catch (Exception e) {
      LOG.error("RM could not transition to Standby", e);
    }
  }

  /**
   * Stop the disconnect timer.  Any running tasks will be allowed to complete.
   */
  private void cancelDisconnectTimer() {
    // 同步加锁取消定时器
    synchronized (zkDisconnectLock) {
      if (zkDisconnectTimer != null) {
        zkDisconnectTimer.cancel();
        zkDisconnectTimer = null;
      }
    }
  }

  /**
   * When the ZK client loses contact with ZK, this method will be called to
   * allow the RM to react. Because the loss of connection can be noticed
   * before the session timeout happens, it is undesirable to transition
   * immediately. Instead the method starts a timer that will wait
   * {@link YarnConfiguration#RM_ZK_TIMEOUT_MS} milliseconds before
   * initiating the transition into standby state.
   */
  @Override
  public void enterNeutralMode() {
    LOG.warn("Lost contact with Zookeeper. Transitioning to standby in "
        + zkSessionTimeout + " ms if connection is not reestablished.");

    // If we've just become disconnected, start a timer.  When the time's up,
    // we'll transition to standby.
    // 同步加锁启动断开连接超时定时器
    synchronized (zkDisconnectLock) {
      if (zkDisconnectTimer == null) {
        zkDisconnectTimer = new Timer("Zookeeper disconnect timer");
        zkDisconnectTimer.schedule(new TimerTask() {
          @Override
          public void run() {
            synchronized (zkDisconnectLock) {
              // Only run if the timer hasn't been cancelled
              if (zkDisconnectTimer != null) {
                // 超时未恢复连接，切换到Standby状态
                becomeStandby();
              }
            }
          }
        }, zkSessionTimeout);
      }
    }
  }

  @SuppressWarnings(value = "unchecked")
  @Override
  public void notifyFatalError(String errorMessage) {
    // 发送致命事件给RM，触发RM关闭退出
    rm.getRMContext().getDispatcher().getEventHandler().handle(
        new RMFatalEvent(RMFatalEventType.EMBEDDED_ELECTOR_FAILED,
            errorMessage));
  }

  @Override
  public void fenceOldActive(byte[] oldActiveData) {
    // 嵌入式选举不支持隔离旧主节点，直接忽略该请求
    LOG.debug("Request to fence old active being ignored, " +
        "as embedded leader election doesn't support fencing");
  }

  /**
   * 序列化集群ID和RM ID生成Active节点信息字节数组
   * @param clusterId 集群ID
   * @param rmId 当前RM HA ID
   * @return 序列化后的proto字节数组
   * @throws IOException 序列化异常
   */
  private static byte[] createActiveNodeInfo(String clusterId, String rmId)
      throws IOException {
    return YarnServerResourceManagerServiceProtos.ActiveRMInfoProto
        .newBuilder()
        .setClusterId(clusterId)
        .setRmId(rmId)
        .build()
        .toByteArray();
  }

  /**
   * 检查Zookeeper上父节点存储的当前活动RM信息是否安全（集群ID匹配、格式正确）
   * @param clusterId 当前集群ID
   * @return 检查是否通过
   * @throws InterruptedException 中断异常
   * @throws IOException IO异常
   * @throws KeeperException ZK异常
   */
  private boolean isParentZnodeSafe(String clusterId)
      throws InterruptedException, IOException, KeeperException {
    byte[] data;
    try {
      // 获取当前活动RM节点数据
      data = elector.getActiveData();
    } catch (ActiveStandbyElector.ActiveNotFoundException e) {
      // 没有找到活动节点，父节点安全
      return true;
    }

    YarnServerResourceManagerServiceProtos.ActiveRMInfoProto proto;
    try {
      // 反序列化解析节点数据
      proto = YarnServerResourceManagerServiceProtos.ActiveRMInfoProto
          .parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Invalid data in ZK: " + StringUtils.byteToHexString(data));
      // 数据格式错误，不安全
      return false;
    }

    // 检查集群ID是否匹配，避免不同集群RM冲突
    if (!proto.getClusterId().equals(clusterId)) {
      LOG.error("Mismatched cluster! The other RM seems " +
          "to be from a different cluster. Current cluster = " + clusterId +
          "Other RM's cluster = " + proto.getClusterId());
      // 集群ID不匹配，不安全
      return false;
    }
    // 检查通过
    return true;
  }

  // EmbeddedElector methods

  @Override
  public void rejoinElection() {
    // 退出当前选举，重新加入选举
    elector.quitElection(false);
    elector.joinElection(localActiveNodeInfo);
  }

  @Override
  public String getZookeeperConnectionState() {
    // 返回Zookeeper连接状态描述
    return elector.getHAZookeeperConnectionState();
  }
}