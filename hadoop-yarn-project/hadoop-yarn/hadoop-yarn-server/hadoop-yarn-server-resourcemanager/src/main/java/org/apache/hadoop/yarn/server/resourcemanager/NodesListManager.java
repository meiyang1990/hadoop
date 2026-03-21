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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.HostsFileReader.HostDetails;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppNodeUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppNodeUpdateEvent.RMAppNodeUpdateType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeDecommissioningEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * YARN ResourceManager 节点列表管理器，负责管理集群 NodeManager 节点的准入规则、
 * 节点上下线（包含优雅下线/强制下线）、节点配置刷新、非活跃节点清理等核心功能。
 * 基于 include/exclude 配置文件实现对节点的准入管控，支持手动刷新节点配置。
 */
@SuppressWarnings("unchecked")
public class NodesListManager extends CompositeService implements
    EventHandler<NodesListManagerEvent> {

  private static final Logger LOG =
      LoggerFactory.getLogger(NodesListManager.class);

  private HostsFileReader hostsReader;
  private Configuration conf;
  private final RMContext rmContext;

  // 默认优雅下线超时时间（秒），负数表示不超时，0表示立即下线
  private int defaultDecTimeoutSecs =
      YarnConfiguration.DEFAULT_RM_NODE_GRACEFUL_DECOMMISSION_TIMEOUT;

  private String includesFile;
  private String excludesFile;

  private Resolver resolver;
  private Timer removalTimer;
  private int nodeRemovalCheckInterval;
  private Set<RMNode> gracefulDecommissionableNodes;
  private boolean enableNodeUntrackedWithoutIncludePath;

  /**
   * 构造 NodesListManager 实例
   * @param rmContext ResourceManager 上下文对象
   */
  public NodesListManager(RMContext rmContext) {
    super(NodesListManager.class.getName());
    this.rmContext = rmContext;
    this.gracefulDecommissionableNodes = ConcurrentHashMap.newKeySet();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    this.conf = conf;

    // 读取节点IP缓存过期时间配置
    int nodeIpCacheTimeout = conf.getInt(
        YarnConfiguration.RM_NODE_IP_CACHE_EXPIRY_INTERVAL_SECS,
        YarnConfiguration.DEFAULT_RM_NODE_IP_CACHE_EXPIRY_INTERVAL_SECS);
    // 缓存超时不大于0则使用直接解析，不缓存
    if (nodeIpCacheTimeout <= 0) {
      resolver = new DirectResolver();
    } else {
      // 否则使用带缓存的解析器，缓存过期后自动清理
      resolver =
          new CachedResolver(SystemClock.getInstance(), nodeIpCacheTimeout);
      addIfService(resolver);
    }

    // 读取include/exclude文件，配置节点访问限制
    try {
      this.includesFile = conf.get(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
          YarnConfiguration.DEFAULT_RM_NODES_INCLUDE_FILE_PATH);
      this.excludesFile = conf.get(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
          YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH);
      this.hostsReader =
          createHostsFileReader(this.includesFile, this.excludesFile);
      // 将exclude列表中的节点初始化为已下线
      setDecommissionedNMs();
      printConfiguredHosts(false);
    } catch (YarnException ex) {
      disableHostsFileReader(ex);
    } catch (IOException ioe) {
      disableHostsFileReader(ioe);
    }

    // 读取未追踪节点自动清理配置
    enableNodeUntrackedWithoutIncludePath = conf.getBoolean(
        YarnConfiguration.RM_ENABLE_NODE_UNTRACKED_WITHOUT_INCLUDE_PATH,
        YarnConfiguration.DEFAULT_RM_ENABLE_NODE_UNTRACKED_WITHOUT_INCLUDE_PATH);
    final Set<String> untrackedSelectiveStatesToRemove = Arrays.stream(conf.getStrings(
        YarnConfiguration.RM_NODEMANAGER_UNTRACKED_NODE_SELECTIVE_STATES_TO_REMOVE,
        YarnConfiguration.DEFAULT_RM_NODEMANAGER_UNTRACKED_NODE_SELECTIVE_STATES_TO_REMOVE))
            .collect(Collectors.toSet());
    final int nodeRemovalTimeout =
        conf.getInt(
            YarnConfiguration.RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC,
            YarnConfiguration.
                DEFAULT_RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC);
    // 设置检查间隔，最小不超过10分钟
    nodeRemovalCheckInterval = (Math.min(nodeRemovalTimeout/2,
        600000));
    removalTimer = new Timer("Node Removal Timer");

    // 启动定时任务，定期清理过期未追踪节点
    removalTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        long now = Time.monotonicNow();
        // 遍历所有非活跃节点
        for (Map.Entry<NodeId, RMNode> entry :
            rmContext.getInactiveRMNodes().entrySet()) {
          NodeId nodeId = entry.getKey();
          RMNode rmNode = entry.getValue();
          // 判断是否为未追踪节点
          if (isUntrackedNode(rmNode.getHostName())) {
            // 如果配置了选择性移除，且当前节点状态不在移除列表中则跳过
            if(CollectionUtils.isNotEmpty(untrackedSelectiveStatesToRemove) &&
                !untrackedSelectiveStatesToRemove.contains(rmNode.getState().toString())) {
              LOG.warn("Untracked node {}, with node state {} is not part of " +
                  "node-removal-untracked.node-selective-states-to-remove config",
                  rmNode.getHostName(), rmNode.getState().toString());
              continue;
            }
            // 初始化未追踪时间戳
            if (rmNode.getUntrackedTimeStamp() == 0) {
              rmNode.setUntrackedTimeStamp(now);
            } else
              // 超过超时时间则从非活跃列表移除
              if (now - rmNode.getUntrackedTimeStamp() >
                  nodeRemovalTimeout) {
                RMNode result = rmContext.getInactiveRMNodes().remove(nodeId);
                if (result != null) {
                  decrInactiveNMMetrics(rmNode);
                  LOG.info("Removed " +result.getState().toString() + " node "
                      + result.getHostName() + " from inactive nodes list");
                }
              }
          } else {
            // 节点恢复追踪，清空时间戳
            rmNode.setUntrackedTimeStamp(0);
          }
        }
      }
    }, nodeRemovalCheckInterval, nodeRemovalCheckInterval);

    super.serviceInit(conf);
  }

  /**
   * 递减对应状态非活跃节点的集群指标计数
   * @param rmNode 要移除的节点
   */
  private void decrInactiveNMMetrics(RMNode rmNode) {
    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();
    switch (rmNode.getState()) {
    case SHUTDOWN:
      clusterMetrics.decrNumShutdownNMs();
      break;
    case DECOMMISSIONED:
      clusterMetrics.decrDecommisionedNMs();
      break;
    case LOST:
      clusterMetrics.decrNumLostNMs();
      break;
    case REBOOTED:
      clusterMetrics.decrNumRebootedNMs();
      break;
    default:
      LOG.debug("Unexpected node state");
    }
  }

  @Override
  public void serviceStop() {
    removalTimer.cancel();
  }

  /**
   * 调试日志打印当前配置的include/exclude主机列表
   * @param graceful 是否优雅刷新
   */
  private void printConfiguredHosts(boolean graceful) {
    if (!LOG.isDebugEnabled()) {
      return;
    }

    LOG.debug("hostsReader: in=" +
        conf.get(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        YarnConfiguration.DEFAULT_RM_NODES_INCLUDE_FILE_PATH) + " out=" +
        conf.get(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
            YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH));

    HostDetails hostDetails;
    if (graceful) {
      hostDetails = hostsReader.getLazyLoadedHostDetails();
    } else {
      hostDetails = hostsReader.getHostDetails();
    }
    for (String include : hostDetails.getIncludedHosts()) {
      LOG.debug("include: " + include);
    }
    for (String exclude : hostDetails.getExcludedHosts()) {
      LOG.debug("exclude: " + exclude);
    }
  }

  /**
   * 刷新节点配置，重新加载include/exclude文件
   * @param yarnConf 新的YARN配置
   * @throws IOException 读取文件异常
   * @throws YarnException YARN处理异常
   */
  public void refreshNodes(Configuration yarnConf)
      throws IOException, YarnException {
    try {
      refreshNodes(yarnConf, false);
    } catch (YarnException | IOException ex) {
      disableHostsFileReader(ex);
    }
  }

  /**
   * 刷新节点配置，可指定是否优雅下线
   * @param yarnConf 新的YARN配置
   * @param graceful 是否优雅下线
   * @throws IOException 读取文件异常
   * @throws YarnException YARN处理异常
   */
  public void refreshNodes(Configuration yarnConf, boolean graceful)
      throws IOException, YarnException {
    refreshHostsReader(yarnConf, graceful, null);
  }

  /**
   * 实际执行主机文件刷新和节点状态处理
   * @param yarnConf 新的YARN配置
   * @param graceful 是否优雅刷新
   * @param timeout 优雅下线超时时间，null表示使用默认配置
   * @throws IOException 读取文件异常
   * @throws YarnException YARN处理异常
   */
  private void refreshHostsReader(
      Configuration yarnConf, boolean graceful, Integer timeout)
          throws IOException, YarnException {
    // 如果未指定超时，从配置读取默认超时
    if (null == timeout) {
      timeout = readDecommissioningTimeout(yarnConf);
    }
    if (null == yarnConf) {
      yarnConf = new YarnConfiguration();
    }
    includesFile =
        yarnConf.get(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
            YarnConfiguration.DEFAULT_RM_NODES_INCLUDE_FILE_PATH);
    excludesFile =
        yarnConf.get(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
            YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH);
    LOG.info("refreshNodes excludesFile " + excludesFile);

    // 加载新的主机列表
    if (graceful) {
      // 优雅刷新先加载不生效
      hostsReader.lazyRefresh(includesFile, excludesFile);
    } else {
      // 直接刷新立即生效
      hostsReader.refresh(includesFile, excludesFile);
    }

    printConfiguredHosts(graceful);

    LOG.info("hostsReader include:{" +
        StringUtils.join(",", hostsReader.getHosts()) +
        "} exclude:{" +
        StringUtils.join(",", hostsReader.getExcludedHosts()) + "}");

    // 处理排除节点列表，更新节点下线/上线状态
    handleExcludeNodeList(graceful, timeout);
    // 将include列表中未注册的节点标记为丢失
    markUnregisteredNodesAsLost(yarnConf);
  }

  /**
   * 初始化时将exclude文件中已配置的节点添加到非活跃列表，标记为已下线
   */
  private void setDecommissionedNMs() {
    Set<String> excludeList = hostsReader.getExcludedHosts();
    for (final String host : excludeList) {
      NodeId nodeId = createUnknownNodeId(host);
      RMNodeImpl rmNode = new RMNodeImpl(nodeId,
          rmContext, host, -1, -1, new UnknownNode(host),
          Resource.newInstance(0, 0), "unknown");
      rmContext.getInactiveRMNodes().put(nodeId, rmNode);
      rmNode.handle(new RMNodeEvent(nodeId, RMNodeEventType.DECOMMISSION));
    }
  }

  // 根据以下规则处理排除节点列表:
  // 1. 已经不在排除列表中的DECOMMISSIONED/DECOMMISSIONING节点需要重新上线
  // 2. 新增的排除节点（未下线）需要执行优雅下线
  // 3. 已经处于DECOMMISSIONED/DECOMMISSIONING的排除节点不做处理
  private void handleExcludeNodeList(boolean graceful, int timeout) {
    // 需要重新上线的节点列表
    List<RMNode> nodesToRecom = new ArrayList<RMNode>();

    // 需要下线的节点列表
    List<RMNode> nodesToDecom = new ArrayList<RMNode>();

    HostDetails hostDetails;
    gracefulDecommissionableNodes.clear();
    if (graceful) {
      hostDetails = hostsReader.getLazyLoadedHostDetails();
    } else {
      hostDetails = hostsReader.getHostDetails();
    }

    Set<String> includes = hostDetails.getIncludedHosts();
    Map<String, Integer> excludes = hostDetails.getExcludedMap();

    // 遍历所有当前活跃节点，检查准入状态
    for (RMNode n : this.rmContext.getRMNodes().values()) {
      NodeState s = n.getState();
      // 判断节点是否被排除（显式排除或不在include列表）
      boolean isExcluded = !isValidNode(
          n.getHostName(), includes, excludes.keySet());
      String nodeStr = "node " + n.getNodeID() + " with state " + s;
      // 节点不需要排除
      if (!isExcluded) {
        // 处于正在下线状态的节点需要重新上线
        if (s == NodeState.DECOMMISSIONING) {
          LOG.info("Recommission " + nodeStr);
          nodesToRecom.add(n);
        }
        // 其他状态无需操作
      } else {
        // 节点需要排除
        if (graceful) {
          // 使用节点级超时，如果没有配置则使用全局超时
          Integer timeoutToUse = (excludes.get(n.getHostName()) != null)?
              excludes.get(n.getHostName()) : timeout;
          // 未下线的节点需要执行优雅下线
          if (s != NodeState.DECOMMISSIONED &&
              s != NodeState.DECOMMISSIONING) {
            LOG.info("Gracefully decommission " + nodeStr);
            nodesToDecom.add(n);
            gracefulDecommissionableNodes.add(n);
          } else if (s == NodeState.DECOMMISSIONING &&
                     !Objects.equals(n.getDecommissioningTimeout(),
                         timeoutToUse)) {
            // 已经在优雅下线，超时时间变更需要更新超时
            LOG.info("Update " + nodeStr + " timeout to be " + timeoutToUse);
            nodesToDecom.add(n);
            gracefulDecommissionableNodes.add(n);
          } else {
            // 状态和超时都无变化无需操作