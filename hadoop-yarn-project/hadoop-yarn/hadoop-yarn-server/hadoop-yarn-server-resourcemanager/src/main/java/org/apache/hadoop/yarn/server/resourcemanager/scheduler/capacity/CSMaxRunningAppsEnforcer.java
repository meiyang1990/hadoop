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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ListMultimap;

/**
 * CS最大运行应用限制执行器，负责跟踪和实施用户与队列的最大运行应用数量约束。
 * 当应用超过限制时会被置为不可运行，等待有应用退出后再恢复运行。
 */
public class CSMaxRunningAppsEnforcer {
  private static final Logger LOG = LoggerFactory.getLogger(
      CSMaxRunningAppsEnforcer.class);

  // 关联的容量调度器实例
  private final CapacityScheduler scheduler;

  // 按用户统计当前可运行应用数量
  private final Map<String, Integer> usersNumRunnableApps;

  // 按用户保存当前不可运行的应用列表（超过限制被限流的应用）
  private final ListMultimap<String, FiCaSchedulerApp> usersNonRunnableApps;

  /**
   * 构造函数，初始化统计容器。
   * @param scheduler 关联的容量调度器实例
   */
  public CSMaxRunningAppsEnforcer(CapacityScheduler scheduler) {
    this.scheduler = scheduler;
    this.usersNumRunnableApps = new HashMap<String, Integer>();
    this.usersNonRunnableApps = ArrayListMultimap.create();
  }

  /**
   * 检查将应用设为可运行是否会超过任何最大运行应用限制，同时更新应用的runnable标记。
   *
   * @param attempt 待检查的应用尝试
   * @return true 应用可运行；false 应用不可运行
   */
  public boolean checkRunnabilityWithUpdate(
      FiCaSchedulerApp attempt) {
    boolean attemptCanRun = !exceedUserMaxParallelApps(attempt.getUser())
        && !exceedQueueMaxParallelApps(attempt.getCSLeafQueue());

    attempt.setRunnable(attemptCanRun);

    return attemptCanRun;
  }

  /**
   * 检查用户当前可运行应用数量是否已超过限制。
   *
   * @param user 用户名
   * @return true 已超过限制；false 未超过限制
   */
  private boolean exceedUserMaxParallelApps(String user) {
    Integer userNumRunnable = usersNumRunnableApps.get(user);
    if (userNumRunnable == null) {
      userNumRunnable = 0;
    }
    if (userNumRunnable >= getUserMaxParallelApps(user)) {
      LOG.info("Maximum runnable apps exceeded for user {}", user);
      return true;
    }

    return false;
  }

  /**
   * 递归检查当前队列及所有父队列的可运行应用数量是否已超过限制。
   *
   * @param queue 当前队列
   * @return true 已超过限制；false 未超过限制
   */
  private boolean exceedQueueMaxParallelApps(AbstractCSQueue queue) {
    // 从当前队列向上检查所有父队列
    while (queue != null) {
      if (queue.getNumRunnableApps() >= queue.getMaxParallelApps()) {
        LOG.info("Maximum runnable apps exceeded for queue {}",
            queue.getQueuePath());
        return true;
      }
      queue = (AbstractCSQueue) queue.getParent();
    }

    return false;
  }

  /**
   * 跟踪新添加的应用，根据应用是否可运行分别统计。
   * @param app 待跟踪的应用
   */
  public void trackApp(FiCaSchedulerApp app) {
    if (app.isRunnable()) {
      trackRunnableApp(app);
    } else {
      trackNonRunnableApp(app);
    }
  }
  
  /**
   * 跟踪新添加的可运行应用，更新用户和各级父队列的可运行应用计数。
   * @param app 待跟踪的可运行应用
   */
  private void trackRunnableApp(FiCaSchedulerApp app) {
    String user = app.getUser();
    AbstractCSQueue queue = (AbstractCSQueue) app.getQueue();
    // 递增所有父队列的可运行应用计数
    AbstractParentQueue parent = (AbstractParentQueue) queue.getParent();
    while (parent != null) {
      parent.incrementRunnableApps();
      parent = (AbstractParentQueue) parent.getParent();
    }

    Integer userNumRunnable = usersNumRunnableApps.get(user);
    usersNumRunnableApps.put(user, (userNumRunnable == null ? 0
        : userNumRunnable) + 1);
  }

  /**
   * 跟踪新添加的不可运行应用，存入用户等待列表。
   * @param app 待跟踪的不可运行应用
   */
  private void trackNonRunnableApp(FiCaSchedulerApp app) {
    String user = app.getUser();
    usersNonRunnableApps.put(user, app);
  }

  /**
   * 调度器重新初始化、配置重新加载后，检查是否有原本不可运行的应用现在可以运行。
   * 当队列最大运行应用数被调大后，原被限流的应用可能恢复运行。
   */

  public void updateRunnabilityOnReload() {
    ParentQueue rootQueue = (ParentQueue) scheduler.getRootQueue();
    List<List<FiCaSchedulerApp>> appsNowMaybeRunnable =
        new ArrayList<List<FiCaSchedulerApp>>();

    // 收集所有可能现在可以运行的应用列表
    gatherPossiblyRunnableAppLists(rootQueue, appsNowMaybeRunnable);

    // 更新应用可运行状态
    updateAppsRunnability(appsNowMaybeRunnable, Integer.MAX_VALUE);
  }

  /**
   * 当某个应用被移除后，检查是否有其他不可运行应用现在可以运行，并恢复其运行状态。
   * 仅在移除应用后原队列达到最大限制减一的情况下，才需要检查，优化性能。
   *
   * @param app 被移除的应用
   */
  public void updateRunnabilityOnAppRemoval(FiCaSchedulerApp app) {
    // childqueueX本身可能没有待处理应用，但如果上层父队列parentqueueY设置了最大运行应用数，
    // childqueueX中一个应用完成，可能允许parentqueueY下其他远距离子节点的应用变为可运行。
    // 只有在移除应用前队列已达到最大限制，移除后才会腾出空间，因此我们找到树中最高的那个
    // 之前达到最大限制，现在腾出空间的祖先队列，只需要检查该队列下的等待应用。
    AbstractLeafQueue queue = app.getCSLeafQueue();
    AbstractCSQueue highestQueueWithAppsNowRunnable =
        (queue.getNumRunnableApps() == queue.getMaxParallelApps() - 1)
        ? queue : null;

    AbstractParentQueue parent = (AbstractParentQueue) queue.getParent();
    while (parent != null) {
      if (parent.getNumRunnableApps() == parent.getMaxParallelApps() - 1) {
        highestQueueWithAppsNowRunnable = parent;
      }
      parent = (AbstractParentQueue) parent.getParent();
    }

    List<List<FiCaSchedulerApp>> appsNowMaybeRunnable =
        new ArrayList<List<FiCaSchedulerApp>>();

    // 编译所有可能现在可运行的应用列表
    // 我们收集列表而不是直接构建所有不可运行应用集合，使得整个操作复杂度是O(队列数)而非O(应用数)
    if (highestQueueWithAppsNowRunnable != null) {
      gatherPossiblyRunnableAppLists(highestQueueWithAppsNowRunnable,
          appsNowMaybeRunnable);
    }
    String user = app.getUser();
    Integer userNumRunning = usersNumRunnableApps.get(user);
    if (userNumRunning == null) {
      userNumRunning = 0;
    }
    // 如果用户移除应用后也腾出了空间，将该用户的等待应用加入检查列表
    if (userNumRunning == getUserMaxParallelApps(user) - 1) {
      List<FiCaSchedulerApp> userWaitingApps = usersNonRunnableApps.get(user);
      if (userWaitingApps != null) {
        appsNowMaybeRunnable.add(userWaitingApps);
      }
    }

    updateAppsRunnability(appsNowMaybeRunnable,
        appsNowMaybeRunnable.size());
  }

  /**
   * 遍历所有可能变为可运行的应用，逐个检查用户和队列是否有剩余空间，
   * 符合条件的应用恢复为可运行状态。如果知道最多能恢复多少个应用，可以提前退出循环。
   * @param appsNowMaybeRunnable 所有可能可运行的应用分组列表
   * @param maxRunnableApps 最多可恢复的应用数量，达到该数量后提前退出
   */
  private void updateAppsRunnability(List<List<FiCaSchedulerApp>>
      appsNowMaybeRunnable, int maxRunnableApps) {
    // 按应用启动时间排序遍历所有候选应用
    Iterator<FiCaSchedulerApp> iter = new MultiListStartTimeIterator(
        appsNowMaybeRunnable);
    FiCaSchedulerApp prev = null;
    List<FiCaSchedulerApp> noLongerPendingApps = new ArrayList<>();
    while (iter.hasNext()) {
      FiCaSchedulerApp next = iter.next();
      if (next == prev) {
        continue;
      }

      // 检查并更新应用可运行状态
      if (checkRunnabilityWithUpdate(next)) {
        AbstractLeafQueue nextQueue = next.getCSLeafQueue();
        LOG.info("{} is now runnable in {}",
            next.getApplicationAttemptId(), nextQueue);
        trackRunnableApp(next);
        FiCaSchedulerApp appSched = next;
        // 将应用重新提交到队列调度
        nextQueue.submitApplicationAttempt(next, next.getUser());
        noLongerPendingApps.add(appSched);

        // 达到最大可恢复数量后提前退出
        if (noLongerPendingApps.size() >= maxRunnableApps) {
          break;
        }
      }

      prev = next;
    }

    // 遍历完成后再从不可运行列表移除已恢复的应用，避免干扰迭代器
    for (FiCaSchedulerApp appSched : noLongerPendingApps) {
      if (!(appSched.getCSLeafQueue().removeNonRunnableApp(appSched))) {
        LOG.error("Can't make app runnable that does not already exist in queue"
            + " as non-runnable: {}. This should never happen.",
            appSched.getApplicationAttemptId());
      }

      if (!usersNonRunnableApps.remove(appSched.getUser(), appSched)) {
        LOG.error("Waiting app {} expected to be in "
            + "usersNonRunnableApps, but was not. This should never happen.",
            appSched.getApplicationAttemptId());
      }
    }
  }

  /**
   * 取消对应用的跟踪，应用完成或被移除时调用。
   * @param app 待取消跟踪的应用
   */
  public void untrackApp(FiCaSchedulerApp app) {
    if (app.isRunnable()) {
      untrackRunnableApp(app);
    } else {
      untrackNonRunnableApp(app);
    }
  }

  /**
   * 取消对可运行应用的跟踪，更新用户和各级父队列的计数。
   * @param app 待取消跟踪的可运行应用
   */
  private void untrackRunnableApp(FiCaSchedulerApp app) {
    // 更新用户可运行应用计数
    String user = app.getUser();
    int newUserNumRunning = usersNumRunnableApps.get(user) - 1;
    if (newUserNumRunning == 0) {
      usersNumRunnableApps.remove(user);
    } else {
      usersNumRunnableApps.put(user, newUserNumRunning);
    }

    // 更新各级队列的可运行应用计数
    AbstractCSQueue queue = (AbstractCSQueue) app.getQueue();
    AbstractParentQueue parent = (AbstractParentQueue) queue.getParent();
    while (parent != null) {
      parent.decrementRunnableApps();
      parent = (AbstractParentQueue) parent.getParent();
    }
  }

  /**
   * 取消对不可运行应用的跟踪，从用户等待列表移除。
   * @param app 待取消跟踪的不可运行应用
   */
  private void untrackNonRunnableApp(FiCaSchedulerApp app) {
    usersNonRunnableApps.remove(app.getUser(), app);
  }

  /**
   * 遍历给定队列下的队列层次结构，收集所有有剩余空间的队列中的不可运行应用列表。
   * @param queue 根队列
   * @param appLists 输出参数，收集得到的不可运行应用列表
   */
  private void gatherPossiblyRunnableAppLists(AbstractCSQueue queue,
      List<List<FiCaSchedulerApp>> appLists) {
    // 如果该队列还有剩余空间，才需要检查它的子队列/自身应用
    if (queue.getNumRunnableApps() < queue.getMaxParallelApps()) {
      if (queue instanceof AbstractLeafQueue) {
        // 叶子队列直接添加自身的不可运行应用副本列表
        appLists.add(
            ((AbstractLeafQueue)queue).getCopyOfNonRunnableAppSchedulables());
      } else {
        // 父队列递归遍历所有子队列
        for (CSQueue child : queue.getChildQueues()) {
          gatherPossiblyRunnableAppLists((AbstractCSQueue) child, appLists);
        }
      }
    }
  }

  /**
   * 从调度配置中获取用户允许的最大并行运行应用数量。
   * @param user 用户名
   * @return 用户最大并行运行应用数，无配置时返回Integer.MAX_VALUE
   */
  private int getUserMaxParallelApps(String user) {
    CapacitySchedulerConfiguration conf = scheduler.getConfiguration();
    if (conf == null) {
      return Integer.MAX_VALUE;
    }

    int userMaxParallelApps = conf.getMaxParallelAppsForUser(user);

    return userMaxParallelApps;
  }

  /**
   * 多列表启动时间迭代器，接收多个按启动时间排序的应用列表，
   * 合并输出一个整体按启动时间排序的应用迭代器。
   * 使用优先队列维护每个列表当前位置的最早应用，实现O(log n)每次获取下一个元素。
   */
  static class MultiListStartTimeIterator implements
      Iterator<FiCaSchedulerApp> {

    // 存储所有输入应用列表
    private List<FiCaSchedulerApp>[] appLists;
    // 每个列表当前遍历到的位置索引
    private int[] curPositionsInAppLists;
    // 优先队列，按当前位置应用的启动时间排序，每次取出最早的
    private PriorityQueue<IndexAndTime> appListsByCurStartTime;

    @SuppressWarnings("unchecked")
    MultiListStartTimeIterator(List<List<FiCaSchedulerApp>> appListList) {
      appLists = appListList.toArray(new List[appListList.size()]);
      curPositionsInAppLists = new int[appLists.length];
      appListsByCurStartTime = new PriorityQueue<IndexAndTime>();
      // 初始化，将每个列表第一个元素加入优先队列
      for (int i = 0; i < appLists.length; i++) {
        long time = appLists[i].isEmpty() ? Long.MAX_VALUE : appLists[i].get(0)
            .getStartTime();
        appListsByCurStartTime.add(new IndexAndTime(i, time));
      }
    }

    @Override
    public boolean hasNext() {
      // 堆不为空，且堆顶元素时间不是Long.MAX_VALUE说明还有未遍历的应用
      return !appListsByCurStartTime.isEmpty()
          && appListsByCurStartTime.peek().time != Long.MAX_VALUE;
    }

    @Override
    public FiCaSchedulerApp next() {
      // 取出当前最早启动的应用
      IndexAndTime indexAndTime = appListsByCurStartTime.remove();
      int nextListIndex = indexAndTime.index;
      FiCaSchedulerApp next = appLists[nextListIndex]
          .get(curPositionsInAppLists[nextListIndex]);
      // 递增该列表当前位置
      curPositionsInAppLists[nextListIndex]++;

      // 更新该列表下一个元素的时间，重新加入堆
      if (curPositionsInAppLists[nextListIndex] <
          appLists[nextListIndex].size()) {
        indexAndTime.time = appLists[nextListIndex]
            .get(curPositionsInAppLists[nextListIndex]).getStartTime();
      } else {
        // 该列表已经遍历完成，设为最大时间
        indexAndTime.time = Long.MAX_VALUE;
      }
      app