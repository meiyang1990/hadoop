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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;

/**
 * 文件说明：基于Apache Curator实现的YARN ResourceManager高可用领导者选举服务
 * 核心职责：在RM HA模式下，通过ZooKeeper实现自动选举active RM，支持自动主备切换
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CuratorBasedElectorService extends AbstractService
    implements EmbeddedElector, LeaderLatchListener {
  public static final Logger LOG =
      LoggerFactory.getLogger(CuratorBasedElectorService.class);
  // Curator领导者锁实例，用于领导者选举
  private LeaderLatch leaderLatch;
  // Curator框架客户端，连接ZooKeeper
  private CuratorFramework curator;
  // 领导者锁在ZooKeeper上的节点路径
  private String latchPath;
  // 当前RM的HA ID
  private String rmId;
  // 当前ResourceManager实例引用
  private ResourceManager rm;

  /**
   * 构造函数，创建基于Curator的选举服务
   * @param rm ResourceManager实例
   */
  public CuratorBasedElectorService(ResourceManager rm) {
    super(CuratorBasedElectorService.class.getName());
    this.rm = rm;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置中获取当前RM的HA ID
    rmId = HAUtil.getRMHAId(conf);
    // 从配置中获取集群ID
    String clusterId = YarnConfiguration.getClusterId(conf);
    // 读取配置中ZooKeeper基础路径，使用默认值如果未配置
    String zkBasePath = conf.get(
        YarnConfiguration.AUTO_FAILOVER_ZK_BASE_PATH,
        YarnConfiguration.DEFAULT_AUTO_FAILOVER_ZK_BASE_PATH);
    // 拼接完整领导者锁节点路径
    latchPath = zkBasePath + "/" + clusterId;
    // 从RM获取已初始化的Curator客户端
    curator = rm.getCurator();
    // 初始化并启动领导者锁
    initAndStartLeaderLatch();
    super.serviceInit(conf);
  }

  /**
   * 初始化并启动Curator LeaderLatch，开始参与选举
   * @throws Exception 初始化或启动异常
   */
  private void initAndStartLeaderLatch() throws Exception {
    leaderLatch = new LeaderLatch(curator, latchPath, rmId);
    leaderLatch.addListener(this);
    leaderLatch.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    // 关闭领导者锁，退出选举
    closeLeaderLatch();
    super.serviceStop();
  }

  @Override
  public void rejoinElection() {
    try {
      // 关闭原有领导者锁
      closeLeaderLatch();
      // 等待1秒后重新加入选举
      Thread.sleep(1000);
      // 重新初始化并启动领导者锁，加入选举
      initAndStartLeaderLatch();
    } catch (Exception e) {
      LOG.info("Fail to re-join election.", e);
    }
  }

  @Override
  public String getZookeeperConnectionState() {
    // 返回当前ZooKeeper连接状态
    return "Connected to zookeeper : " +
        curator.getZookeeperClient().isConnected();
  }

  /**
   * 当前节点被选为领导者（active RM）后的回调处理
   */
  @Override
  public void isLeader() {
    LOG.info(rmId + "is elected leader, transitioning to active");
    try {
      // 通知RMAdmin服务将当前RM切换为active状态
      rm.getRMContext().getRMAdminService()
          .transitionToActive(
          new HAServiceProtocol.StateChangeRequestInfo(
              HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC));
    } catch (Exception e) {
      // 切换active失败，放弃领导权，重新加入新一轮选举
      LOG.info(rmId + " failed to transition to active, giving up leadership",
          e);
      notLeader();
      rejoinElection();
    }
  }

  /**
   * 关闭领导者锁，释放领导权
   * @throws IOException 关闭异常
   */
  private void closeLeaderLatch() throws IOException {
    if (leaderLatch != null) {
      leaderLatch.close();
    }
  }

  /**
   * 当前节点失去领导权后的回调处理
   */
  @Override
  public void notLeader() {
    LOG.info(rmId + " relinquish leadership");
    try {
      // 通知RMAdmin服务将当前RM切换为standby状态
      rm.getRMContext().getRMAdminService()
          .transitionToStandby(
          new HAServiceProtocol.StateChangeRequestInfo(
              HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC));
    } catch (Exception e) {
      LOG.info(rmId + " did not transition to standby successfully.");
    }
  }

  /**
   * 获取Curator客户端实例，仅用于单元测试
   * @return CuratorFramework实例
   */
  // only for testing
  @VisibleForTesting
  public CuratorFramework getCuratorClient() {
    return this.curator;
  }
}