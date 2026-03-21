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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;

/**
 * 公平调度器抢占处理线程，负责为资源不足的应用抢占其他过量使用资源的容器
 */
class FSPreemptionThread extends SubjectInheritingThread {
  private static final Logger LOG = LoggerFactory.
      getLogger(FSPreemptionThread.class);
  protected final FSContext context;
  private final FairScheduler scheduler;
  // 预抢占警告到实际杀死容器的等待时间
  private final long warnTimeBeforeKill;
  // 两次饥饿检查之间的间隔时间
  private final long delayBeforeNextStarvationCheck;
  // 预抢占定时器，用于延迟执行容器杀死操作
  private final Timer preemptionTimer;
  // 调度器读锁，保证和更新线程并发安全
  private final Lock schedulerReadLock;

  @SuppressWarnings("deprecation")
  FSPreemptionThread(FairScheduler scheduler) {
    setDaemon(true);
    setName("FSPreemptionThread");
    this.scheduler = scheduler;
    this.context = scheduler.getContext();
    FairSchedulerConfiguration fsConf = scheduler.getConf();
    context.setPreemptionEnabled();
    context.setPreemptionUtilizationThreshold(
        fsConf.getPreemptionUtilizationThreshold());
    preemptionTimer = new Timer("Preemption Timer", true);

    warnTimeBeforeKill = fsConf.getWaitTimeBeforeKill();
    // 计算分配延迟：连续调度模式下取10次调度间隔，否则取4次节点心跳间隔
    long allocDelay = (fsConf.isContinuousSchedulingEnabled()
        ? 10 * fsConf.getContinuousSchedulingSleepMs() // 10 runs
        : 4 * scheduler.getNMHeartbeatInterval()); // 4 heartbeats
    delayBeforeNextStarvationCheck = warnTimeBeforeKill + allocDelay +
        fsConf.getWaitTimeBeforeNextStarvationCheck();
    schedulerReadLock = scheduler.getSchedulerReadLock();
  }

  @Override
  public void work() {
    while (!Thread.interrupted()) {
      try {
        // 从阻塞队列取出饥饿应用，没有饥饿应用时阻塞等待
        FSAppAttempt starvedApp = context.getStarvedApps().take();
        // 获取调度器读锁，避免和调度更新线程并发冲突
        schedulerReadLock.lock();
        try {
          // 识别需要抢占的容器并执行抢占
          preemptContainers(identifyContainersToPreempt(starvedApp));
        } finally {
          schedulerReadLock.unlock();
        }
        // 触发抢占后，标记该应用下次饥饿检查时间
        starvedApp.preemptionTriggered(delayBeforeNextStarvationCheck);
      } catch (InterruptedException e) {
        LOG.info("Preemption thread interrupted! Exiting.");
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * 针对饥饿应用，识别出需要抢占的容器集合，满足其资源需求
   *
   * @param starvedApp 资源不足的饥饿应用
   * @return 需要被抢占的容器列表
   */
  private List<RMContainer> identifyContainersToPreempt(
      FSAppAttempt starvedApp) {
    List<RMContainer> containersToPreempt = new ArrayList<>();

    // 遍历所有饥饿资源请求，收集足够满足需求的容器
    for (ResourceRequest rr : starvedApp.getStarvedResourceRequests()) {
      // 根据资源请求的位置获取符合位置要求的节点列表
      List<FSSchedulerNode> potentialNodes = scheduler.getNodeTracker()
              .getNodesByResourceName(rr.getResourceName());
      for (int i = 0; i < rr.getNumContainers(); i++) {
        // 找到本容器请求最优的可抢占容器集合
        PreemptableContainers bestContainers =
            getBestPreemptableContainers(rr, potentialNodes);
        if (bestContainers != null) {
          List<RMContainer> containers = bestContainers.getAllContainers();
          if (containers.size() > 0) {
            containersToPreempt.addAll(containers);
            // 在节点上标记这些容器即将被抢占
            trackPreemptionsAgainstNode(containers, starvedApp);
            // 通知容器所属应用即将被抢占
            for (RMContainer container : containers) {
              FSAppAttempt app = scheduler.getSchedulerApp(
                      container.getApplicationAttemptId());
              LOG.info("Preempting container " + container + " from queue: "
                  + (app != null ? app.getQueueName() : "unknown"));
              // 应用已注销的情况下跳过通知
              if (app != null) {
                app.trackContainerForPreemption(container);
              }
            }
          }
        }
      }
    } // End of iteration over RRs
    return containersToPreempt;
  }

  /**
   * 为单个容器请求，在候选节点中找出包含最少AM容器的可抢占容器集合
   * @param potentialNodes 候选节点列表
   * @param rr 资源请求
   * @return 最优可抢占容器集合
   */
  private PreemptableContainers identifyContainersToPreemptForOneContainer(
          List<FSSchedulerNode> potentialNodes, ResourceRequest rr) {
    PreemptableContainers bestContainers = null;
    int maxAMContainers = Integer.MAX_VALUE;

    for (FSSchedulerNode node : potentialNodes) {
      PreemptableContainers preemptableContainers =
              identifyContainersToPreemptOnNode(
                      rr.getCapability(), node, maxAMContainers);

      if (preemptableContainers != null) {
        // 当前节点找到的集合比之前的更好（AM容器更少），更新最优解
        bestContainers = preemptableContainers;
        maxAMContainers = bestContainers.numAMContainers;

        // 找到不含AM容器的最优解，直接结束搜索
        if (maxAMContainers == 0) {
          break;
        }
      }
    }
    return bestContainers;
  }

  /**
   * 在指定节点上识别可抢占容器，尽量避免抢占AM容器，只有当收集到的AM数量小于阈值时才返回结果
   *
   * @param request  请求的资源量
   * @param node  待检查的节点
   * @param maxAMContainers 集合中允许的最大AM容器数量
   * @return 可抢占容器集合，如果满足资源需求且AM数量小于阈值则返回，否则返回null
   */
  private PreemptableContainers identifyContainersToPreemptOnNode(
      Resource request, FSSchedulerNode node, int maxAMContainers) {
    PreemptableContainers preemptableContainers =
        new PreemptableContainers(maxAMContainers);

    // 获取节点上运行中的容器列表，AM容器排在末尾
    List<RMContainer> containersToCheck =
        node.getRunningContainersWithAMsAtTheEnd();
    // 移除已经标记为待抢占的容器
    containersToCheck.removeAll(node.getContainersForPreemption());

    // 初始可使用资源 = 节点未分配资源 - 节点已预留资源
    Resource potential = Resources.subtractFromNonNegative(
        Resources.clone(node.getUnallocatedResource()),
        node.getTotalReserved());

    for (RMContainer container : containersToCheck) {
      FSAppAttempt app =
          scheduler.getSchedulerApp(container.getApplicationAttemptId());
      // 应用已注销的情况下跳过该容器，会很快被清理
      if (app == null) {
        LOG.info("Found container " + container + " on node "
            + node.getNodeName() + "without app, skipping preemption");
        continue;
      }
      ApplicationId appId = app.getApplicationId();

      // 检查该容器是否满足抢占条件（应用当前资源使用超过配额）
      if (app.canContainerBePreempted(container,
              preemptableContainers.getResourcesToPreemptForApp(appId))) {
        // 将容器加入待抢占集合
        if (!preemptableContainers.addContainer(container, appId)) {
          return null;
        }

        // 将容器资源加入总可抢占资源量
        Resources.addTo(potential, container.getAllocatedResource());
      }

      // 检查当前可抢占资源已经满足请求需求，直接返回结果
      if (Resources.fitsIn(request, potential)) {
        return preemptableContainers;
      }
    }

    // 所有可抢占容器总资源仍不满足请求，返回null
    return null;
  }

  /**
   * 在对应的节点上标记这些容器即将被抢占，预留给当前饥饿应用
   * @param containers 待抢占容器列表
   * @param app  抢占资源的饥饿应用
   */
  private void trackPreemptionsAgainstNode(List<RMContainer> containers,
                                           FSAppAttempt app) {
    FSSchedulerNode node = scheduler.getNodeTracker()
        .getNode(containers.get(0).getNodeId());
    node.addContainersForPreemption(containers, app);
  }

  /**
   * 调度延迟抢占任务，等待警告期过后杀死容器
   * @param containers 待杀死的容器列表
   */
  private void preemptContainers(List<RMContainer> containers) {
    // 启动定时器，等待warnTimeBeforeKill后执行杀死操作
    preemptionTimer.schedule(
        new PreemptContainersTask(containers), warnTimeBeforeKill);
  }

  /**
   * 根据资源请求的位置和局部性放松规则，找出包含最少AM容器的最优可抢占容器集合
   * 首先在符合位置要求的节点中查找，如果结果包含AM且允许放松局部性，则扩展到所有节点重新查找
   * 如果其他节点能找到AM更少的集合，则替换结果
   *
   * @param rr 资源请求
   * @param potentialNodes 符合位置要求的候选节点列表
   * @return 最优可抢占容器集合
   */
  private PreemptableContainers getBestPreemptableContainers(ResourceRequest rr,
      List<FSSchedulerNode> potentialNodes) {
    // 首先在符合位置要求的节点中查找
    PreemptableContainers bestContainers =
        identifyContainersToPreemptForOneContainer(potentialNodes, rr);

    // 如果允许放松局部性，且原位置找到的结果包含AM，则尝试在其他节点找更优结果
    if (rr.getRelaxLocality()
        && !ResourceRequest.isAnyLocation(rr.getResourceName())
        && bestContainers != null
        && bestContainers.numAMContainers > 0) {
      // 获取所有不在原候选列表中的节点
      List<FSSchedulerNode> remainingNodes =
          scheduler.getNodeTracker().getAllNodes();
      remainingNodes.removeAll(potentialNodes);
      // 在剩余节点中查找
      PreemptableContainers spareContainers =
          identifyContainersToPreemptForOneContainer(remainingNodes, rr);
      // 如果找到AM更少的结果，替换最优解
      if (spareContainers != null && spareContainers.numAMContainers
          < bestContainers.numAMContainers) {
        bestContainers = spareContainers;
      }
    }

    return bestContainers;
  }

  /**
   * 延迟抢占任务，等待警告期结束后真正杀死容器
   */
  private class PreemptContainersTask extends TimerTask {
    private final List<RMContainer> containers;

    PreemptContainersTask(List<RMContainer> containers) {
      this.containers = containers;
    }

    @Override
    public void run() {
      for (RMContainer container : containers) {
        // 创建抢占状态的容器状态
        ContainerStatus status = SchedulerUtils.createPreemptedContainerStatus(
            container.getContainerId(), SchedulerUtils.PREEMPTED_CONTAINER);

        LOG.info("Killing container " + container);
        // 通知调度器完成容器清理，标记为抢占杀死
        scheduler.completedContainer(
            container, status, RMContainerEventType.KILL);
      }
    }
  }

  /**
   * 可抢占容器集合跟踪类，按应用分组管理，统计AM容器数量
   */
  private static class PreemptableContainers {
    // 按应用分组存储待抢占容器
    Map<ApplicationId, List<RMContainer>> containersByApp;
    // 当前集合中AM容器数量
    int numAMContainers;
    // 允许的最大AM容器数量，超过则返回无效
    int maxAMContainers;

    PreemptableContainers(int maxAMContainers) {
      numAMContainers = 0;
      this.maxAMContainers = maxAMContainers;
      this.containersByApp = new HashMap<>();
    }

    /**
     * 添加容器到集合，如果AM容器数量超过上限则添加失败
     *
     * @param container 待添加容器
     * @param appId 所属应用ID
     * @return 添加成功返回true，否则返回false
     */
    private boolean addContainer(RMContainer container, ApplicationId appId) {
      if (container.isAMContainer()) {
        numAMContainers++;
        // AM数量超过上限，添加失败
        if (numAMContainers >= maxAMContainers) {
          return false;
        }
      }

      if (!containersByApp.containsKey(appId)) {
        containersByApp.put(appId, new ArrayList<>());
      }

      containersByApp.get(appId).add(container);
      return true;
    }

    /**
     * 获取集合中所有待抢占容器
     * @return 全量容器列表
     */
    private List<RMContainer> getAllContainers() {
      List<RMContainer> allContainers = new ArrayList<>();
      for (List<RMContainer> containersForApp : containersByApp.values()) {
        allContainers.addAll(containersForApp);
      }
      return allContainers;
    }

    /**
     * 获取当前集合中指定应用已选中容器的总资源量
     * @param appId 应用ID
     * @return 总资源量
     */
    private Resource getResourcesToPreemptForApp(ApplicationId appId) {
      Resource resourcesToPreempt = Resources.createResource(0, 0);
      if (containersByApp.containsKey(appId)) {
        for (RMContainer container : containersByApp.get(appId)) {
          Resources.addTo(resourcesToPreempt, container.getAllocatedResource());
        }
      }
      return resourcesToPreempt;
    }
  }
}