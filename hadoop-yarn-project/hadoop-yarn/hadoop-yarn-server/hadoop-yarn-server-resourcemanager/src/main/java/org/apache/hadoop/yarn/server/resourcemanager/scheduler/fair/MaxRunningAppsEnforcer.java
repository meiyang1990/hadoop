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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.AMState;

/**
 * 公平调度器最大运行应用数限制管理器，负责跟踪和强制执行用户和队列的最大运行应用数限制
 */
public class MaxRunningAppsEnforcer {
  private static final Logger LOG = LoggerFactory.getLogger(
      MaxRunningAppsEnforcer.class);
  
  // 所属的公平调度器实例
  private final FairScheduler scheduler;

  // 跟踪每个用户当前可运行应用的数量
  private final Map<String, Integer> usersNumRunnableApps;
  @VisibleForTesting
  // 存储每个用户等待排队的不可运行应用列表
  final ListMultimap<String, FSAppAttempt> usersNonRunnableApps;

  /**
   * 构造方法，初始化最大运行应用数限制管理器
   * @param scheduler 所属公平调度器
   */
  public MaxRunningAppsEnforcer(FairScheduler scheduler) {
    this.scheduler = scheduler;
    this.usersNumRunnableApps = new HashMap<String, Integer>();
    this.usersNonRunnableApps = ArrayListMultimap.create();
  }

  /**
   * 检查将应用置为可运行是否会超出任何最大运行应用数限制
   *
   * @param queue 当前应用所属队列
   * @param attempt 待检查的应用尝试
   * @return true 允许应用运行；false 超出限制不允许运行
   */
  public boolean canAppBeRunnable(FSQueue queue, FSAppAttempt attempt) {
    boolean ret = true;
    // 检查用户级别是否超出最大应用数
    if (exceedUserMaxApps(attempt.getUser())) {
      attempt.updateAMContainerDiagnostics(AMState.INACTIVATED,
          "The user \"" + attempt.getUser() + "\" has reached the maximum limit"
              + " of runnable applications.");
      ret = false;
    } else if (exceedQueueMaxRunningApps(queue)) {
      // 检查队列及其所有祖先队列是否超出最大应用数
      attempt.updateAMContainerDiagnostics(AMState.INACTIVATED,
          "The queue \"" + queue.getName() + "\" has reached the maximum limit"
              + " of runnable applications.");
      ret = false;
    }

    return ret;
  }

  /**
   * 检查用户的可运行应用数是否已超出限制
   *
   * @param user 用户名
   * @return true 超出限制；false 未超出限制
   */
  public boolean exceedUserMaxApps(String user) {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    Integer userNumRunnable = usersNumRunnableApps.get(user);
    if (userNumRunnable == null) {
      userNumRunnable = 0;
    }
    if (userNumRunnable >= allocConf.getUserMaxApps(user)) {
      return true;
    }

    return false;
  }

  /**
   * 递归检查当前队列及其所有祖先队列是否超出最大运行应用数限制
   *
   * @param queue 当前队列
   * @return true 存在队列超出限制；false 所有队列均未超出
   */
  public boolean exceedQueueMaxRunningApps(FSQueue queue) {
    // 检查当前队列及所有父队列
    while (queue != null) {
      if (queue.getNumRunnableApps() >= queue.getMaxRunningApps()) {
        return true;
      }
      queue = queue.getParent();
    }

    return false;
  }

  /**
   * 将应用标记为可运行，更新用户和队列的运行应用计数
   *
   * @param app 待跟踪的应用尝试
   */
  public void trackRunnableApp(FSAppAttempt app) {
    String user = app.getUser();
    FSLeafQueue queue = app.getQueue();
    // 递增所有父队列的可运行应用计数
    FSParentQueue parent = queue.getParent();
    while (parent != null) {
      parent.incrementRunnableApps();
      parent = parent.getParent();
    }

    Integer userNumRunnable = usersNumRunnableApps.get(user);
    usersNumRunnableApps.put(user, (userNumRunnable == null ? 0
        : userNumRunnable) + 1);
  }

  /**
   * 将应用标记为不可运行，加入等待队列
   *
   * @param app 待跟踪的不可运行应用尝试
   */
  public void trackNonRunnableApp(FSAppAttempt app) {
    String user = app.getUser();
    usersNonRunnableApps.put(user, app);
  }

  /**
   * 重新加载分配配置后触发，检查是否有之前不可运行的应用现在满足限制可以变为可运行
   * 
   * 时间复杂度O(n)，n为之前受限现在可能满足条件的不可运行应用数量
   */
  public void updateRunnabilityOnReload() {
    FSParentQueue rootQueue = scheduler.getQueueManager().getRootQueue();
    List<List<FSAppAttempt>> appsNowMaybeRunnable =
        new ArrayList<List<FSAppAttempt>>();

    // 收集所有可能变为可运行的应用列表
    gatherPossiblyRunnableAppLists(rootQueue, appsNowMaybeRunnable);

    // 更新应用可运行状态
    updateAppsRunnability(appsNowMaybeRunnable, Integer.MAX_VALUE);
  }

  /**
   * 应用移除后触发，检查是否有之前不可运行的应用现在满足限制可以变为可运行
   * 
   * 时间复杂度O(n log(n))，n为最高受限 ancestor 队列下的队列数量
   *
   * @param app 被移除的应用
   * @param queue 应用所属叶子队列
   */
  public void updateRunnabilityOnAppRemoval(FSAppAttempt app, FSLeafQueue queue) {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    
    // 找到树中最高位置的、移除应用后刚有空余容量的 ancestor 队列
    FSQueue highestQueueWithAppsNowRunnable = (queue.getNumRunnableApps() ==
        queue.getMaxRunningApps() - 1) ? queue : null;
    FSParentQueue parent = queue.getParent();
    while (parent != null) {
      if (parent.getNumRunnableApps() == parent.getMaxRunningApps() - 1) {
        highestQueueWithAppsNowRunnable = parent;
      }
      parent = parent.getParent();
    }

    List<List<FSAppAttempt>> appsNowMaybeRunnable =
        new ArrayList<List<FSAppAttempt>>();

    // 收集该队列下所有可能可运行的应用
    if (highestQueueWithAppsNowRunnable != null) {
      gatherPossiblyRunnableAppLists(highestQueueWithAppsNowRunnable,
          appsNowMaybeRunnable);
    }
    // 检查用户是否释放出容量，收集该用户等待的应用
    String user = app.getUser();
    Integer userNumRunning = usersNumRunnableApps.get(user);
    if (userNumRunning == null) {
      userNumRunning = 0;
    }
    if (userNumRunning == allocConf.getUserMaxApps(user) - 1) {
      List<FSAppAttempt> userWaitingApps = usersNonRunnableApps.get(user);
      if (userWaitingApps != null) {
        appsNowMaybeRunnable.add(userWaitingApps);
      }
    }

    // 更新应用可运行状态
    updateAppsRunnability(appsNowMaybeRunnable,
        appsNowMaybeRunnable.size());
  }

  /**
   * 遍历所有候选应用，逐个检查用户和队列是否有空闲容量，将满足条件的应用转为可运行
   * 通过maxRunnableApps参数提前终止迭代，避免不必要的遍历
   *
   * @param appsNowMaybeRunnable 所有可能变为可运行的应用列表分组
   * @param maxRunnableApps 最多可激活的应用数量，达到该数量后提前终止
   */
  private void updateAppsRunnability(List<List<FSAppAttempt>>
      appsNowMaybeRunnable, int maxRunnableApps) {
    // 按启动时间排序遍历所有候选应用
    Iterator<FSAppAttempt> iter = new MultiListStartTimeIterator(
        appsNowMaybeRunnable);
    FSAppAttempt prev = null;
    List<FSAppAttempt> noLongerPendingApps = new ArrayList<FSAppAttempt>();
    while (iter.hasNext()) {
      FSAppAttempt next = iter.next();
      if (next == prev) {
        continue;
      }

      if (canAppBeRunnable(next.getQueue(), next)) {
        // 更新运行计数跟踪
        trackRunnableApp(next);
        FSAppAttempt appSched = next;
        // 将应用添加到队列可运行列表
        next.getQueue().addApp(appSched, true);
        noLongerPendingApps.add(appSched);

        // 达到最大激活数量提前退出
        if (noLongerPendingApps.size() >= maxRunnableApps) {
          break;
        }
      }

      prev = next;
    }
    
    // 遍历完成后统一从等待列表移除已激活应用，避免迭代过程中修改列表
    for (FSAppAttempt appSched : noLongerPendingApps) {
      if (!appSched.getQueue().removeNonRunnableApp(appSched)) {
        LOG.error("Can't make app runnable that does not already exist in queue"
            + " as non-runnable: " + appSched + ". This should never happen.");
      }
      
      if (!usersNonRunnableApps.remove(appSched.getUser(), appSched)) {
        LOG.error("Waiting app " + appSched + " expected to be in "
            + "usersNonRunnableApps, but was not. This should never happen.");
      }
    }
  }
  /**
   * 移除可运行应用，更新用户和队列的运行应用计数
   *
   * @param app 被移除的应用
   */
  public void untrackRunnableApp(FSAppAttempt app) {
    // 更新用户可运行应用计数
    String user = app.getUser();
    int newUserNumRunning = usersNumRunnableApps.get(user) - 1;
    if (newUserNumRunning == 0) {
      usersNumRunnableApps.remove(user);
    } else {
      usersNumRunnableApps.put(user, newUserNumRunning);
    }
    
    // 更新所有父队列的可运行应用计数
    FSLeafQueue queue = app.getQueue();
    FSParentQueue parent = queue.getParent();
    while (parent != null) {
      parent.decrementRunnableApps();
      parent = parent.getParent();
    }
  }
  
  /**
   * 移除不可运行应用，停止跟踪该应用
   *
   * @param app 被移除的应用
   */
  public void untrackNonRunnableApp(FSAppAttempt app) {
    usersNonRunnableApps.remove(app.getUser(), app);
  }

  /**
   * 遍历给定队列下的层级结构，收集所有有空闲容量的队列中的不可运行应用列表
   *
   * @param queue 根搜索队列
   * @param appLists 收集结果的输出参数，存储所有可能可运行的应用列表分组
   */
  private void gatherPossiblyRunnableAppLists(FSQueue queue,
      List<List<FSAppAttempt>> appLists) {
    if (queue.getNumRunnableApps() < queue.getMaxRunningApps()) {
      if (queue instanceof FSLeafQueue) {
        appLists.add(
            ((FSLeafQueue)queue).getCopyOfNonRunnableAppSchedulables());
      } else {
        for (FSQueue child : queue.getChildQueues()) {
          gatherPossiblyRunnableAppLists(child, appLists);
        }
      }
    }
  }

  /**
   * 多个按启动时间排序的列表合并迭代器，按全局启动时间升序返回元素
   * 使用优先堆维护每个列表当前位置的启动时间，每次取出最小启动时间元素，
   * 时间复杂度O(log k) per next()调用，k为列表数量
   */
  static class MultiListStartTimeIterator implements
      Iterator<FSAppAttempt> {

    // 存储所有应用列表
    private List<FSAppAttempt>[] appLists;
    // 存储每个列表当前遍历位置索引
    private int[] curPositionsInAppLists;
    // 优先堆，按当前位置应用启动时间排序
    private PriorityQueue<IndexAndTime> appListsByCurStartTime;

    @SuppressWarnings("unchecked")
    /**
     * 构造合并迭代器，初始化优先堆
     * @param appListList 多个按启动时间排序的应用列表
     */
    public MultiListStartTimeIterator(List<List<FSAppAttempt>> appListList) {
      appLists = appListList.toArray(new List[appListList.size()]);
      curPositionsInAppLists = new int[appLists.length];
      appListsByCurStartTime = new PriorityQueue<IndexAndTime>();
      // 初始化每个列表，将第一个元素的启动时间加入堆
      for (int i = 0; i < appLists.length; i++) {
        long time = appLists[i].isEmpty() ? Long.MAX_VALUE : appLists[i].get(0)
            .getStartTime();
        appListsByCurStartTime.add(new IndexAndTime(i, time));
      }
    }

    @Override
    public boolean hasNext() {
      // 堆不为空且堆顶元素不是已遍历完毕标记
      return !appListsByCurStartTime.isEmpty()
          && appListsByCurStartTime.peek().time != Long.MAX_VALUE;
    }

    @Override
    public FSAppAttempt next() {
      // 取出当前启动时间最早的元素
      IndexAndTime indexAndTime = appListsByCurStartTime.remove();
      int nextListIndex = indexAndTime.index;
      FSAppAttempt next = appLists[nextListIndex]
          .get(curPositionsInAppLists[nextListIndex]);
      // 移动当前列表指针
      curPositionsInAppLists[nextListIndex]++;

      // 更新该列表下一个元素到堆中
      if (curPositionsInAppLists[nextListIndex] < appLists[nextListIndex].size()) {
        indexAndTime.time = appLists[nextListIndex]
            .get(curPositionsInAppLists[nextListIndex]).getStartTime();
      } else {
        // 列表遍历完毕，设置标记时间
        indexAndTime.time = Long.MAX_VALUE;
      }
      appListsByCurStartTime.add(indexAndTime);

      return next;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove not supported");
    }

    /**
     * 优先堆存储元素：保存列表索引和当前元素启动时间，按启动时间排序
     */
    private static class IndexAndTime implements Comparable<IndexAndTime> {
      public int index;
      public long time;

      public IndexAndTime(int index, long time) {
        this.index = index;
        this.time = time;
      }

      @Override
      public int compareTo(IndexAndTime o) {
        return time < o.time ? -1 : (time > o.time ? 1 : 0);
      }

      @Override
      public boolean equals(Object o) {
        if (!(o instanceof IndexAndTime)) {
          return false;
        }
        IndexAndTime other = (IndexAndTime)o;
        return other.time == time;
      }

      @Override
      public int hashCode() {
        return (int)time;
      }
    }
  }
}