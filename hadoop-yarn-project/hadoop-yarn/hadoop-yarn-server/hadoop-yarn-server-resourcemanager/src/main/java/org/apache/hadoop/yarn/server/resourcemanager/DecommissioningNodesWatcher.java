// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.util.MonotonicClock;

/**
 * 退役节点监听器，供ResourceTrackerService使用，跟踪处于DECOMMISSIONING状态的节点，
 * 等待节点上所有容器完成后，将节点状态转换为DECOMMISSIONED，并通知NodeManager关闭。
 * <p>
 * 在MapReduce应用场景下，节点上所有容器完成后，仍可能为Reducer提供Map输出数据。
 * 完全优雅的退服需要等待所有关联应用完成才能下线节点，但这会导致长时间运行的应用场景下，
 * 大量空闲节点长期保持DECOMMISSIONING状态占用资源，因此本工具通过超时策略平衡该问题：
 * 无论是否还有容器或应用运行，DECOMMISSIONING节点都会在超时后强制转为DECOMMISSIONED状态。
 * <p>
 * 当没有节点处于DECOMMISSIONING状态时，本工具基本不会产生额外性能开销。
 */
public class DecommissioningNodesWatcher {
  private static final Logger LOG =
      LoggerFactory.getLogger(DecommissioningNodesWatcher.class);

  private final RMContext rmContext;

  /**
   * 退役节点上下文，保存单个DECOMMISSIONING节点的跟踪信息，
   * 记录该节点所有容器状态更新与退服进度。
   */
  class DecommissioningNodeContext {
    private final NodeId nodeId;

    // 当前节点状态
    private NodeState nodeState;

    // 节点进入DECOMMISSIONING状态的起始时间
    private final long decommissioningStartTime;

    private long lastContainerFinishTime;

    // 当前节点活跃容器数量
    private int numActiveContainers;

    // 该节点上运行过的所有应用ID列表
    private List<ApplicationId> appIds;

    // 节点首次被观测到进入DECOMMISSIONED状态的时间
    private long decommissionedTime;

    // 当前节点退服超时时间（毫秒），可从RMNode动态更新
    private long timeoutMs;

    private long lastUpdateTime;

    public DecommissioningNodeContext(NodeId nodeId, int timeoutSec) {
      this.nodeId = nodeId;
      this.appIds = new ArrayList<>();
      this.decommissioningStartTime = mclock.getTime();
      this.timeoutMs = 1000L * timeoutSec;
    }

    void updateTimeout(int timeoutSec) {
      this.timeoutMs = 1000L * timeoutSec;
    }
  }

  // 所有需要跟踪的DECOMMISSIONING节点上下文映射表
  private HashMap<NodeId, DecommissioningNodeContext> decomNodes =
      new HashMap<NodeId, DecommissioningNodeContext>();

  private Timer pollTimer;
  private MonotonicClock mclock;

  /**
   * 构造退役节点监听器，关联RM上下文。
   * @param rmContext ResourceManager上下文
   */
  public DecommissioningNodesWatcher(RMContext rmContext) {
    this.rmContext = rmContext;
    pollTimer = new Timer(true);
    mclock = new MonotonicClock();
  }

  /**
   * 初始化监听器，按配置的轮询间隔启动定时轮询任务。
   * @param conf YARN配置
   */
  public void init(Configuration conf) {
    int v = conf.getInt(
        YarnConfiguration.RM_DECOMMISSIONING_NODES_WATCHER_POLL_INTERVAL,
        YarnConfiguration
          .DEFAULT_RM_DECOMMISSIONING_NODES_WATCHER_POLL_INTERVAL);
    pollTimer.schedule(new PollTimerTask(rmContext), 0, (1000L * v));
  }

  /**
   * 根据最新节点状态更新退服跟踪信息。
   * @param rmNode 目标节点
   * @param remoteNodeStatus NodeManager上报的最新节点状态
   */
  public synchronized void update(RMNode rmNode, NodeStatus remoteNodeStatus) {
    DecommissioningNodeContext context = decomNodes.get(rmNode.getNodeID());
    long now = mclock.getTime();
    // 节点已进入DECOMMISSIONED状态
    if (rmNode.getState() == NodeState.DECOMMISSIONED) {
      if (context == null) {
        return;
      }
      context.nodeState = rmNode.getState();
      // 保留DECOMMISSIONED节点一段时间用于状态日志，避免节点直接消失无法查看状态
      if (context.decommissionedTime == 0) {
        context.decommissionedTime = now;
      } else if (now - context.decommissionedTime > 60000L) {
        decomNodes.remove(rmNode.getNodeID());
      }
    } else if (rmNode.getState() == NodeState.DECOMMISSIONING) {
      // 节点刚进入DECOMMISSIONING状态，新建上下文
      if (context == null) {
        context = new DecommissioningNodeContext(rmNode.getNodeID(),
            rmNode.getDecommissioningTimeout());
        decomNodes.put(rmNode.getNodeID(), context);
        context.nodeState = rmNode.getState();
        context.decommissionedTime = 0;
      }
      // 更新超时时间、最后更新时间
      context.updateTimeout(rmNode.getDecommissioningTimeout());
      context.lastUpdateTime = now;

      // 更新当前节点运行的应用列表
      context.appIds = rmNode.getRunningApps();

      // 统计当前节点活跃容器数量
      int numActiveContainers = 0;
      for (ContainerStatus cs : remoteNodeStatus.getContainersStatuses()) {
        ContainerState newState = cs.getState();
        if (newState == ContainerState.RUNNING ||
            newState == ContainerState.NEW) {
          numActiveContainers++;
        }
      }
      context.numActiveContainers = numActiveContainers;

      // 如果已经没有活跃容器，记录容器全部完成时间
      if (context.numActiveContainers == 0 &&
          context.lastContainerFinishTime == 0) {
        context.lastContainerFinishTime = now;
      }
    } else {
      // 节点处于其他状态，移除跟踪
      if (context != null) {
        decomNodes.remove(rmNode.getNodeID());
      }
    }
  }

  /**
   * 从跟踪列表中移除指定节点。
   * @param nodeId 目标节点ID
   */
  public synchronized void remove(NodeId nodeId) {
    DecommissioningNodeContext context = decomNodes.get(nodeId);
    if (context != null) {
      LOG.info("remove " + nodeId + " in " + context.nodeState);
      decomNodes.remove(nodeId);
    }
  }

  /**
   * 停止监听器，取消定时轮询。
   */
  public void stop() {
    pollTimer.cancel();
    pollTimer = null;
  }

  /**
   * 退役节点状态枚举，表示节点当前退服阶段。
   */
  public enum DecommissioningNodeStatus {
    // 节点不处于DECOMMISSIONING状态，无需跟踪
    NONE,

    // 等待运行中容器完成
    WAIT_CONTAINER,

    // 所有容器已完成，等待运行中应用完成
    WAIT_APP,

    // 等待容器或应用完成超时
    TIMEOUT,

    // 无需继续等待，可转为DECOMMISSIONED状态
    READY,

    // 节点已完成退服
    DECOMMISSIONED,
  }

  /**
   * 检查指定节点是否已准备好完成退服。
   * @param nodeId 目标节点ID
   * @return true表示节点可转为DECOMMISSIONED状态，false否则
   */
  public boolean checkReadyToBeDecommissioned(NodeId nodeId) {
    DecommissioningNodeStatus s = checkDecommissioningStatus(nodeId);
    return (s == DecommissioningNodeStatus.READY ||
            s == DecommissioningNodeStatus.TIMEOUT);
  }

  /**
   * 获取指定节点当前退服状态。
   * @param nodeId 目标节点ID
   * @return 节点当前退服状态枚举
   */
  public DecommissioningNodeStatus checkDecommissioningStatus(NodeId nodeId) {
    DecommissioningNodeContext context = decomNodes.get(nodeId);
    if (context == null) {
      return DecommissioningNodeStatus.NONE;
    }

    if (context.nodeState == NodeState.DECOMMISSIONED) {
      return DecommissioningNodeStatus.DECOMMISSIONED;
    }

    // 计算节点进入DECOMMISSIONING状态后的等待时长
    long waitTime = mclock.getTime() - context.decommissioningStartTime;
    // 仍有活跃容器
    if (context.numActiveContainers > 0) {
      return (context.timeoutMs < 0 || waitTime < context.timeoutMs)?
          DecommissioningNodeStatus.WAIT_CONTAINER :
          DecommissioningNodeStatus.TIMEOUT;
    }

    // 无活跃容器也无应用，直接准备完成退服
    if (context.appIds.size() == 0) {
      return DecommissioningNodeStatus.READY;
    } else {
      // 还有应用未完成，继续等待或超时
      return (context.timeoutMs < 0 || waitTime < context.timeoutMs)?
          DecommissioningNodeStatus.WAIT_APP :
          DecommissioningNodeStatus.TIMEOUT;
    }
  }

  /**
   * 定时轮询任务，周期性执行以下工作：
   * 1. 日志输出所有DECOMMISSIONING节点的当前状态
   * 2. 识别并清理过期 stale 节点（例如已终止的节点）
   * 3. 触发已超时节点的退服完成流程
   */
  class PollTimerTask extends TimerTask {
    private final RMContext rmContext;

    public PollTimerTask(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    public void run() {
      logDecommissioningNodesStatus();
      long now = mclock.getTime();
      Set<NodeId> staleNodes = new HashSet<NodeId>();

      // 遍历所有跟踪节点检查状态
      for (Iterator<Map.Entry<NodeId, DecommissioningNodeContext>> it =
          decomNodes.entrySet().iterator(); it.hasNext();) {
        Map.Entry<NodeId, DecommissioningNodeContext> e = it.next();
        DecommissioningNodeContext d = e.getValue();
        // 跳过最近更新过的节点，NM通常每秒更新一次状态
        if (now - d.lastUpdateTime < 5000L) {
          continue;
        }
        // 移除非DECOMMISSIONING状态的过期节点
        if (d.nodeState != NodeState.DECOMMISSIONING) {
          LOG.debug("remove {} {}", d.nodeState, d.nodeId);
          it.remove();
          continue;
        } else if (now - d.lastUpdateTime > 60000L) {
          // 长时间未收到节点更新，检查节点状态，过期则移除
          RMNode rmNode = getRmNode(d.nodeId);
          if (rmNode != null &&
              rmNode.getState() == NodeState.DECOMMISSIONED) {
            LOG.debug("remove {} {}", rmNode.getState(), d.nodeId);
            it.remove();
            continue;
          }
        }
        // 收集已超时的节点
        if (d.timeoutMs >= 0 &&
            d.decommissioningStartTime + d.timeoutMs < now) {
          staleNodes.add(d.nodeId);
          LOG.debug("Identified stale and timeout node {}", d.nodeId);
        }
      }

      // 处理所有超时节点，触发退服完成事件
      for (NodeId nodeId : staleNodes) {
        RMNode rmNode = this.rmContext.getRMNodes().get(nodeId);
        if (rmNode == null || rmNode.getState() != NodeState.DECOMMISSIONING) {
          remove(nodeId);
          continue;
        }
        if (rmNode.getState() == NodeState.DECOMMISSIONING &&
            checkReadyToBeDecommissioned(rmNode.getNodeID())) {
          LOG.info("DECOMMISSIONING " + nodeId + " timeout");
          this.rmContext.getDispatcher().getEventHandler().handle(
              new RMNodeEvent(nodeId, RMNodeEventType.DECOMMISSION));
        }
      }
    }
  }

  /**
   * 从活跃/非活跃节点列表获取RMNode实例。
   * @param nodeId 目标节点ID
   * @return RMNode实例，不存在则返回null
   */
  private RMNode getRmNode(NodeId nodeId) {
    RMNode rmNode = this.rmContext.getRMNodes().get(nodeId);
    if (rmNode == null) {
      rmNode = this.rmContext.getInactiveRMNodes().get(nodeId);
    }
    return rmNode;
  }

  /**
   * 计算指定节点剩余退服超时时间（秒）。
   * @param context 节点退服上下文
   * @return 剩余秒数，负数表示无超时，0表示已超时
   */
  private int getTimeoutInSec(DecommissioningNodeContext context) {
    if (context.nodeState == NodeState.DECOMMISSIONED) {
      return 0;
    } else if (context.nodeState != NodeState.DECOMMISSIONING) {
      return -1;
    }
    if (context.appIds.size() == 0 && context.numActiveContainers == 0) {
      return 0;
    }
    // 负超时表示无限等待，无超时
    if (context.timeoutMs < 0) {
      return -1;
    }

    long now = mclock.getTime();
    long timeout = context.decommissioningStartTime + context.timeoutMs - now;
    return Math.max(0, (int)(timeout / 1000));
  }

  /**
   * 日志输出所有DECOMMISSIONING节点的当前状态，仅DEBUG级别开启。
   */
  private void logDecommissioningNodesStatus() {
    if (!LOG.isDebugEnabled() || decomNodes.size() == 0) {
      return;
    }
    long now = mclock.getTime();
    for (DecommissioningNodeContext d : decomNodes.values()) {
      StringBuilder sb = new StringBuilder();
      DecommissioningNodeStatus s = checkDecommissioningStatus(d.nodeId);
      sb.append(String.format(
          "%n  %-34s %4ds fresh:%3ds containers:%2d %14s",
          d.nodeId.getHost(),
          (now - d.decommissioningStartTime) / 1000,
          (now - d.lastUpdateTime) / 1000,
          d.numActiveContainers,
          s));
      if (s == DecommissioningNodeStatus.WAIT_APP ||
          s == DecommissioningNodeStatus.WAIT_CONTAINER) {
        sb.append(String.format(" timeout:%4ds", getTimeoutInSec(d)));
      }
      for (ApplicationId aid : d.appIds) {
        sb.append("\n    " + aid);
        RMApp rmApp = rmContext.getRMApps().get(aid);
        if (rmApp != null) {
          sb.append(String.format(
              " %s %9s %5.2f%% %5ds",
              rmApp.getState(),
              (rmApp.getApplicationType() == null)?
                  "" : rmApp.getApplicationType(),
              100.0 * rmApp.getProgress(),
              (mclock.getTime() - rmApp.getStartTime()) / 1000));
        }
      }
      LOG.debug("Decommissioning node: " + sb.toString());
    }
  }
}