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
package org.apache.hadoop.hdfs.server.namenode.top.metrics;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager;
import org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager.Op;
import org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager.User;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager.TopWindow;

/**
 * 文件级注释：NameNode 热点操作用户统计模块核心类，维护滚动时间窗口内的用户操作计数，用于统计占用NameNode操作最多的用户
 *
 * The interface to the top metrics.
 * <p>
 * Metrics are collected by a custom audit logger, {@link org.apache.hadoop
 * .hdfs.server.namenode.top.TopAuditLogger}, which calls TopMetrics to
 * increment per-operation, per-user counts on every audit log call. These
 * counts are used to show the top users by NameNode operation as well as
 * across all operations.
 * <p>
 * TopMetrics maintains these counts for a configurable number of time
 * intervals, e.g. 1min, 5min, 25min. Each interval is tracked by a
 * RollingWindowManager.
 * <p>
 * These metrics are published as a JSON string via {@link org.apache.hadoop
 * .hdfs.server .namenode.metrics.FSNamesystemMBean#getTopWindows}. This is
 * done by calling {@link org.apache.hadoop.hdfs.server.namenode.top.window
 * .RollingWindowManager#snapshot} on each RollingWindowManager.
 * <p>
 * Thread-safe: relies on thread-safety of RollingWindowManager
 */
@InterfaceAudience.Private
public class TopMetrics implements MetricsSource {
  public static final Logger LOG = LoggerFactory.getLogger(TopMetrics.class);
  public static final String TOPMETRICS_METRICS_SOURCE_NAME =
      "NNTopUserOpCounts";
  private final boolean isMetricsSourceEnabled;

  /**
   * 输出NNTop配置信息到日志，用于调试和问题排查
   * @param conf Hadoop配置对象
   */
  private static void logConf(Configuration conf) {
    LOG.info("NNTop conf: " + DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_KEY +
        " = " +  conf.get(DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_KEY));
    LOG.info("NNTop conf: " + DFSConfigKeys.NNTOP_NUM_USERS_KEY +
        " = " +  conf.get(DFSConfigKeys.NNTOP_NUM_USERS_KEY));
    LOG.info("NNTop conf: " + DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY +
        " = " +  conf.get(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY));
  }

  /**
   * 滚动窗口管理器映射，键为窗口时长（分钟），值为对应时长的滚动窗口管理器
   * A map from reporting periods to WindowManager. Thread-safety is provided by
   * the fact that the mapping is not changed after construction.
   */
  final Map<Integer, RollingWindowManager> rollingWindowManagers =
      new HashMap<Integer, RollingWindowManager>();

  /**
   * 构造函数，根据配置初始化所有指定时长的滚动窗口
   * @param conf Hadoop配置对象
   * @param reportingPeriods 需要统计的所有时间窗口时长数组
   */
  public TopMetrics(Configuration conf, int[] reportingPeriods) {
    logConf(conf);
    for (int i = 0; i < reportingPeriods.length; i++) {
      rollingWindowManagers.put(reportingPeriods[i], new RollingWindowManager(
          conf, reportingPeriods[i]));
    }
    isMetricsSourceEnabled = conf.getBoolean(DFSConfigKeys.NNTOP_ENABLED_KEY,
        DFSConfigKeys.NNTOP_ENABLED_DEFAULT);
  }

  /**
   * 获取所有时间窗口的当前统计快照，用于对外暴露统计结果
   * @return 所有时间窗口的统计结果列表
   */
  public List<TopWindow> getTopWindows() {
    long monoTime = Time.monotonicNow();
    List<TopWindow> windows = Lists.newArrayListWithCapacity
        (rollingWindowManagers.size());
    for (Entry<Integer, RollingWindowManager> entry : rollingWindowManagers
        .entrySet()) {
      TopWindow window = entry.getValue().snapshot(monoTime);
      windows.add(window);
    }
    return windows;
  }

  /**
   * 从审计日志提取需要统计的信息并上报，保持和审计日志信息一致
   * Pick the same information that DefaultAuditLogger does before writing to a
   * log file. This is to be consistent when {@link TopMetrics} is charged with
   * data read back from log files instead of being invoked directly by the
   * FsNamesystem
   * @param succeeded 操作是否成功
   * @param userName 操作用户名
   * @param addr 客户端地址
   * @param cmd 操作命令类型
   * @param src 源路径
   * @param dst 目标路径
   * @param status 文件状态
   */
  public void report(boolean succeeded, String userName, InetAddress addr,
      String cmd, String src, String dst, FileStatus status) {
    // currently nntop only makes use of the username and the command
    report(userName, cmd);
  }

  /**
   * 上报当前时间点的用户操作
   * @param userName 操作用户名
   * @param cmd 操作命令类型
   */
  public void report(String userName, String cmd) {
    long currTime = Time.monotonicNow();
    report(currTime, userName, cmd);
  }

  /**
   * 上报指定时间点的用户操作，对所有时间窗口执行计数
   * @param currTime 操作发生时间
   * @param userName 操作用户名
   * @param cmd 操作命令类型
   */
  public void report(long currTime, String userName, String cmd) {
    LOG.debug("a metric is reported: cmd: {} user: {}", cmd, userName);
    userName = UserGroupInformation.trimLoginMethod(userName);
    for (RollingWindowManager rollingWindowManager : rollingWindowManagers
        .values()) {
      rollingWindowManager.recordMetric(currTime, cmd, userName, 1);
    }
  }

  /**
   * 实现MetricsSource接口，将统计数据导出给Hadoop metrics2系统，供外部监控系统采集
   * Flatten out the top window metrics into
   * {@link org.apache.hadoop.metrics2.MetricsRecord}s for consumption by
   * external metrics systems. Each metrics record added corresponds to the
   * reporting period a.k.a window length of the configured rolling windows.
   * @param collector 指标收集器
   * @param all 是否导出所有指标
   */
  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    if (!isMetricsSourceEnabled) {
      return;
    }

    // 遍历所有时间窗口，逐个导出指标
    for (final TopWindow window : getTopWindows()) {
      MetricsRecordBuilder rb = collector.addRecord(buildOpRecordName(window))
          .setContext("dfs");
      // 遍历每个操作类型，导出该操作总计数和TOP用户计数
      for (final Op op: window.getOps()) {
        rb.addCounter(buildOpTotalCountMetricsInfo(op), op.getTotalCount());
        for (User user : op.getTopUsers()) {
          rb.addCounter(buildOpRecordMetricsInfo(op, user), user.getCount());
        }
      }
    }
  }

  /**
   * 构建指定时间窗口的指标记录名称
   * @param window 时间窗口对象
   * @return 指标记录名称
   */
  private String buildOpRecordName(TopWindow window) {
    return TOPMETRICS_METRICS_SOURCE_NAME + ".windowMs="
      + window.getWindowLenMs();
  }

  /**
   * 构建操作总计数的指标信息
   * @param op 操作对象
   * @return 指标信息对象
   */
  private MetricsInfo buildOpTotalCountMetricsInfo(Op op) {
    return Interns.info("op=" + StringUtils.deleteWhitespace(op.getOpType())
      + ".TotalCount", "Total operation count");
  }

  /**
   * 构建用户操作计数的指标信息
   * @param op 操作对象
   * @param user 用户统计对象
   * @return 指标信息对象
   */
  private MetricsInfo buildOpRecordMetricsInfo(Op op, User user) {
    return Interns.info("op=" + StringUtils.deleteWhitespace(op.getOpType())
      + ".user=" + user.getUser()
      + ".count", "Total operations performed by user");
  }
}