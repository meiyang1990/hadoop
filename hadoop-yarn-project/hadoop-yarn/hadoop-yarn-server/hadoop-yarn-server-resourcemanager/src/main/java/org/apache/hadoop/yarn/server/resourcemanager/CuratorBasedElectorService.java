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
 * 基于 Curator 的 Leader 选举服务
 * 使用 Apache Curator 框架的 LeaderLatch 实现 RM 高可用的自动故障转移
 * 当获得领导权时自动转换为 Active 状态，失去领导权时自动转换为 Standby 状态
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CuratorBasedElectorService extends AbstractService
    implements EmbeddedElector, LeaderLatchListener {
  public static final Logger LOG =
      LoggerFactory.getLogger(CuratorBasedElectorService.class);
  private LeaderLatch leaderLatch;  // Curator LeaderLatch 用于领导选举
  private CuratorFramework curator;
  private String latchPath;  // ZooKeeper 上的选举路径
  private String rmId;  // RM 实例 ID（HA 模式下每个 RM 唯一标识）
  private ResourceManager rm;

  public CuratorBasedElectorService(ResourceManager rm) {
    super(CuratorBasedElectorService.class.getName());
    this.rm = rm;
  }

  /**
   * 服务初始化：构建 ZK 选举路径并启动 LeaderLatch
   * 路径格式：{zkBasePath}/{clusterId}
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    rmId = HAUtil.getRMHAId(conf);
    String clusterId = YarnConfiguration.getClusterId(conf);
    String zkBasePath = conf.get(
        YarnConfiguration.AUTO_FAILOVER_ZK_BASE_PATH,
        YarnConfiguration.DEFAULT_AUTO_FAILOVER_ZK_BASE_PATH);
    latchPath = zkBasePath + "/" + clusterId;
    curator = rm.getCurator();
    initAndStartLeaderLatch();
    super.serviceInit(conf);
  }

  /**
   * 初始化并启动 LeaderLatch，注册当前 RM 为监听器
   */
  private void initAndStartLeaderLatch() throws Exception {
    leaderLatch = new LeaderLatch(curator, latchPath, rmId);
    leaderLatch.addListener(this);
    leaderLatch.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    closeLeaderLatch();
    super.serviceStop();
  }

  /**
   * 重新加入选举：关闭当前 Latch，短暂等待后重新创建并启动
   * 常用于失败后重试场景
   */
  @Override
  public void rejoinElection() {
    try {
      closeLeaderLatch();
      Thread.sleep(1000);
      initAndStartLeaderLatch();
    } catch (Exception e) {
      LOG.info("Fail to re-join election.", e);
    }
  }

  @Override
  public String getZookeeperConnectionState() {
    return "Connected to zookeeper : " +
        curator.getZookeeperClient().isConnected();
  }

  /**
   * LeaderLatchListener 回调：当前 RM 当选为 Leader
   * 自动触发到 Active 状态的转换
   */
  @Override
  public void isLeader() {
    LOG.info(rmId + "is elected leader, transitioning to active");
    try {
      rm.getRMContext().getRMAdminService()
          .transitionToActive(
          new HAServiceProtocol.StateChangeRequestInfo(
              HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC));
    } catch (Exception e) {
      LOG.info(rmId + " failed to transition to active, giving up leadership",
          e);
      // 转换失败时放弃领导权并重新加入选举
      notLeader();
      rejoinElection();
    }
  }

  private void closeLeaderLatch() throws IOException {
    if (leaderLatch != null) {
      leaderLatch.close();
    }
  }

  /**
   * LeaderLatchListener 回调：当前 RM 失去 Leader 身份
   * 自动触发到 Standby 状态的转换
   */
  @Override
  public void notLeader() {
    LOG.info(rmId + " relinquish leadership");
    try {
      rm.getRMContext().getRMAdminService()
          .transitionToStandby(
          new HAServiceProtocol.StateChangeRequestInfo(
              HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC));
    } catch (Exception e) {
      LOG.info(rmId + " did not transition to standby successfully.");
    }
  }

  // only for testing
  @VisibleForTesting
  public CuratorFramework getCuratorClient() {
    return this.curator;
  }
}
