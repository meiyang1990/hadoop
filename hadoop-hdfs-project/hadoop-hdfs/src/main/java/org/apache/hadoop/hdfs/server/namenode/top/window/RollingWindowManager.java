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
package org.apache.hadoop.hdfs.server.namenode.top.window;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：
 * 滚动窗口管理器，用于管理一组RollingWindow，为NameNode TOP指标系统提供核心能力，
 * 支持按操作类型和用户统计请求量，滑动窗口过期清理，并获取当前TOP N用户指标。
 * 本类是metrics系统获取当前TOP指标的入口接口，通过ConcurrentHashMap和线程安全的RollingWindow保证线程安全。
 * 
 * A class to manage the set of {@link RollingWindow}s. This class is the
 * interface of metrics system to the {@link RollingWindow}s to retrieve the
 * current top metrics.
 * <p>
 * Thread-safety is provided by each {@link RollingWindow} being thread-safe as
 * well as {@link ConcurrentHashMap} for the collection of them.
 */
@InterfaceAudience.Private
public class RollingWindowManager {
  public static final Logger LOG = LoggerFactory.getLogger(
      RollingWindowManager.class);

  // 滑动窗口总长度，单位毫秒
  private final int windowLenMs;
  // 每个窗口包含的桶数量，用于分段滑动过期，例如每分钟10个桶
  private final int bucketsPerWindow; // e.g., 10 buckets per minute
  // 需要返回的TOP用户数量，例如报告TOP 10用户
  private final int topUsersCnt; // e.g., report top 10 metrics

  static private class RollingWindowMap extends
      ConcurrentHashMap<String, RollingWindow> {
    private static final long serialVersionUID = -6785807073237052051L;
  }

  /**
   * 滚动窗口快照类，保存一个窗口内所有操作类型的TOP用户排名结果。
   * Represents a snapshot of the rolling window. It contains one Op per 
   * operation in the window, with ranked users for each Op.
   */
  public static class TopWindow {
    private final int windowMillis;
    private final List<Op> top;

    public TopWindow(int windowMillis) {
      this.windowMillis = windowMillis;
      this.top = new LinkedList<>();
    }

    public void addOp(Op op) {
      if (op.getOpType().equals(TopConf.ALL_CMDS)) {
        top.add(0, op);
      } else {
        top.add(op);
      }
    }

    public int getWindowLenMs() {
      return windowMillis;
    }

    public List<Op> getOps() {
      return top;
    }
  }

  /**
   * 操作统计类，保存一个操作类型下的TOP用户排名和总请求量。
   * Represents an operation within a TopWindow. It contains a ranked 
   * set of the top users for the operation.
   */
  public static class Op implements Comparable<Op> {
    private final String opType;
    private final List<User> users;
    private final long totalCount;
    private final int limit;

    public Op(String opType, UserCounts users, int limit) {
      this.opType = opType;
      this.users = new ArrayList<>(users);
      // 按请求量降序排序
      this.users.sort(Collections.reverseOrder());
      this.totalCount = users.getTotal();
      this.limit = limit;
    }

    public String getOpType() {
      return opType;
    }

    public List<User> getAllUsers() {
      return users;
    }

    public List<User> getTopUsers() {
      return (users.size() > limit) ? users.subList(0, limit) : users;
    }

    public long getTotalCount() {
      return totalCount;
    }

    @Override
    public int compareTo(Op other) {
      return Long.signum(totalCount - other.totalCount);
    }

    @Override
    public boolean equals(Object o) {
      return (o instanceof Op) && totalCount == ((Op)o).totalCount;
    }

    @Override
    public int hashCode() {
      return opType.hashCode();
    }
  }

  /**
   * 用户请求统计类，保存用户名和该用户的请求次数。
   * Represents a user who called an Op within a TopWindow. Specifies the 
   * user and the number of times the user called the operation.
   */
  public static class User implements Comparable<User> {
    private final String user;
    private long count;

    public User(String user, long count) {
      this.user = user;
      this.count = count;
    }

    public String getUser() {
      return user;
    }

    public long getCount() {
      return count;
    }

    public void add(long delta) {
      count += delta;
    }

    @Override
    public int compareTo(User other) {
      return Long.signum(count - other.count);
    }

    @Override
    public boolean equals(Object o) {
      return (o instanceof User) && user.equals(((User)o).user;
    }

    @Override
    public int hashCode() {
      return user.hashCode();
    }
  }

  /**
   * 用户计数集合类，负责累加合并同一用户的请求量，并计算总请求数。
   */
  private static class UserCounts extends ArrayList<User> {
    private long total = 0;

    UserCounts(int capacity) {
      super(capacity);
    }

    @Override
    public boolean add(User user) {
      long count = user.getCount();
      int i = indexOf(user);
      // 用户不存在则新增，存在则累加计数
      if (i == -1) {
        super.add(new User(user.getUser(), count));
      } else {
        get(i).add(count);
      }
      // 累加总请求数
      total += count;
      return true;
    }

    @Override
    public boolean addAll(Collection<? extends User> users) {
      users.forEach(user -> add(user));
      return true;
    }

    public long getTotal() {
      return total;
    }
  }

  /**
   * 指标到滚动窗口映射的顶级存储结构：key为操作类型，value为该操作下所有用户的滚动窗口映射。
   * A mapping from each reported metric to its {@link RollingWindowMap} that
   * maintains the set of {@link RollingWindow}s for the users that have
   * operated on that metric.
   */
  public ConcurrentHashMap<String, RollingWindowMap> metricMap =
      new ConcurrentHashMap<>();

  /**
   * 构造滚动窗口管理器，从配置中初始化窗口参数并做参数校验。
   * @param conf Hadoop配置对象
   * @param reportingPeriodMs 报告周期，即滑动窗口长度，单位毫秒
   */
  public RollingWindowManager(Configuration conf, int reportingPeriodMs) {
    
    windowLenMs = reportingPeriodMs;
    bucketsPerWindow =
        conf.getInt(DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_KEY,
            DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_DEFAULT);
    Preconditions.checkArgument(bucketsPerWindow > 0,
        "a window should have at least one bucket");
    Preconditions.checkArgument(bucketsPerWindow <= windowLenMs,
        "the minimum size of a bucket is 1 ms");
    //same-size buckets
    Preconditions.checkArgument(windowLenMs % bucketsPerWindow == 0,
        "window size must be a multiplication of number of buckets");
    topUsersCnt =
        conf.getInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY,
            DFSConfigKeys.NNTOP_NUM_USERS_DEFAULT);
    Preconditions.checkArgument(topUsersCnt > 0,
        "the number of requested top users must be at least 1");
  }

  /**
   * 记录一次用户操作指标，更新对应用户对应操作的滚动窗口计数。
   *
   * @param time 事件发生时间戳
   * @param command 操作类型，即指标名称
   * @param user 发起操作的用户名
   * @param delta 计数增量，一般为+1
   */
  public void recordMetric(long time, String command,
      String user, long delta) {
    RollingWindow window = getRollingWindow(command, user);
    window.incAt(time, delta);
  }

  /**
   * 对当前所有指标生成TOP用户快照，清理过期无请求的窗口，聚合生成结果。
   *
   * @param time 当前时间戳
   * @return 包含所有操作类型TOP用户统计的窗口快照
   */
  public TopWindow snapshot(long time) {
    TopWindow window = new TopWindow(windowLenMs);
    Set<String> metricNames = metricMap.keySet();
    LOG.debug("iterating in reported metrics, size={} values={}",
        metricNames.size(), metricNames);
    // 用于聚合所有操作的总请求量
    UserCounts totalCounts = new UserCounts(metricMap.size());
    for (Map.Entry<String, RollingWindowMap> entry : metricMap.entrySet()) {
      String metricName = entry.getKey();
      RollingWindowMap rollingWindows = entry.getValue();
      UserCounts topN = getTopUsersForMetric(time, metricName, rollingWindows);
      if (!topN.isEmpty()) {
        window.addOp(new Op(metricName, topN, topUsersCnt));
        totalCounts.addAll(topN);
      }
    }
    // 聚合所有操作的TOP用户，生成全操作总统计
    Set<User> topUsers = new HashSet<>();
    for (Op op : window.getOps()) {
      topUsers.addAll(op.getTopUsers());
    }
    // 只保留各操作TOP用户的总计数
    totalCounts.retainAll(topUsers);
    // 添加全操作汇总，不限制TOP数量，允许超过单操作限制以保留所有TOP用户的总统计
    window.addOp(new Op(TopConf.ALL_CMDS, totalCounts, Integer.MAX_VALUE));
    return window;
  }

  /**
   * 计算单个指标的TOP N用户，同时清理过期无请求的用户窗口。
   * 
   * @param time 当前时间戳
   * @param metricName 指标/操作类型名称
   * @param rollingWindows 该指标下所有用户的滚动窗口映射
   * @return 该指标按请求量排序的用户计数集合
   */
  private UserCounts getTopUsersForMetric(long time, String metricName,
      RollingWindowMap rollingWindows) {
    UserCounts topN = new UserCounts(topUsersCnt);
    Iterator<Map.Entry<String, RollingWindow>> iterator =
        rollingWindows.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, RollingWindow> entry = iterator.next();
      String userName = entry.getKey();
      RollingWindow aWindow = entry.getValue();
      // 获取当前窗口有效时间内的总请求量
      long windowSum = aWindow.getSum(time);
      // 清理请求量为0的过期窗口，释放内存
      if (windowSum == 0) {
        LOG.debug("gc window of metric: {} userName: {}",
            metricName, userName);
        iterator.remove();
        continue;
      }
      LOG.debug("offer window of metric: {} userName: {} sum: {}",
          metricName, userName, windowSum);
      topN.add(new User(userName, windowSum));
    }
    LOG.debug("topN users size for command {} is: {}",
        metricName, topN.size());
    return topN;
  }

  /**
   * 获取指定操作和用户对应的滚动窗口，如果不存在则创建新窗口。
   * 使用putIfAbsent保证并发安全。
   *
   * @param metric 指标/操作类型
   * @param user 用户名
   * @return 对应操作和用户的滚动窗口实例
   */
  private RollingWindow getRollingWindow(String metric, String user) {
    RollingWindowMap rwMap = metricMap.get(metric);
    // 操作不存在则创建新映射，并发安全处理
    if (rwMap == null) {
      rwMap = new RollingWindowMap();
      RollingWindowMap prevRwMap = metricMap.putIfAbsent(metric, rwMap);
      if (prevRwMap != null) {
        rwMap = prevRwMap;
      }
    }
    RollingWindow window = rwMap.get(user);
    if (window != null) {
      return window;
    }
    // 用户不存在则创建新窗口，并发安全处理
    window = new RollingWindow(windowLenMs, bucketsPerWindow);
    RollingWindow prevWindow = rwMap.putIfAbsent(user, window);
    if (prevWindow != null) {
      window = prevWindow;
    }
    return window;
  }
}