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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.task.reduce.MapHost.State;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce Shuffle阶段的调度器实现，负责管理Reduce任务拉取Map输出的调度逻辑
 * 核心职责：维护待拉取的Map输出位置、管理失败主机惩罚、控制并发拉取数量、监控Shuffle进度与健康状态
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ShuffleSchedulerImpl<K,V> implements ShuffleScheduler<K,V> {
  private static final ThreadLocal<Long> SHUFFLE_START =
      new ThreadLocal<Long>() {
    protected Long initialValue() {
      return 0L;
    }
  };

  private static final Logger LOG =
      LoggerFactory.getLogger(ShuffleSchedulerImpl.class);
  private static final int MAX_MAPS_AT_ONCE = 20;
  private static final long INITIAL_PENALTY = 10000;
  private static final float PENALTY_GROWTH_RATE = 1.3f;
  private final static int REPORT_FAILURE_LIMIT = 10;
  private static final float BYTES_PER_MILLIS_TO_MBS = 1000f / 1024 / 1024;
  
  // 标记每个Map任务是否已完成拉取
  private final boolean[] finishedMaps;

  // 总的Map任务数量
  private final int totalMaps;
  // 剩余待拉取的Map任务数量
  private int remainingMaps;
  // 按主机地址存储Map输出位置信息，key为主机名+端口
  private Map<String, MapHost> mapLocations = new HashMap<String, MapHost>();
  // 待分配拉取任务的主机集合
  private Set<MapHost> pendingHosts = new HashSet<MapHost>();
  // 已废弃的Map任务Attempt集合
  private Set<TaskAttemptID> obsoleteMaps = new HashSet<TaskAttemptID>();

  // 当前Reduce任务的AttemptID
  private final TaskAttemptID reduceId;
  private final Random random = new Random();
  // 存储惩罚延迟的延迟队列，超时后自动解禁被惩罚的主机
  private final DelayQueue<Penalty> penalties = new DelayQueue<Penalty>();
  // 处理惩罚超时的后台线程
  private final Referee referee = new Referee();
  // 记录每个Map任务Attempt的拉取失败次数
  private final Map<TaskAttemptID,IntWritable> failureCounts =
    new HashMap<TaskAttemptID,IntWritable>();
  // 记录每个主机的拉取失败次数
  private final Map<String,IntWritable> hostFailures =
    new HashMap<String,IntWritable>();
  private final TaskStatus status;
  // 异常报告器，用于向上层报告异常
  private final ExceptionReporter reporter;
  // 任务终止阈值：单个Map允许的最大失败次数
  private final int abortFailureLimit;
  // Shuffle进度对象
  private final Progress progress;
  // 已完成拉取的Map计数器
  private final Counters.Counter shuffledMapsCounter;
  // 已拉取总字节数计数器
  private final Counters.Counter reduceShuffleBytes;
  // 拉取失败次数计数器
  private final Counters.Counter failedShuffleCounter;

  // Shuffle开始时间
  private final long startTime;
  // 上次进度更新时间
  private long lastProgressTime;

  // 拷贝时间统计器，用于统计并发拷贝的总有效时间
  private final CopyTimeTracker copyTimeTracker;

  // 最大的Map任务运行时间
  private volatile int maxMapRuntime = 0;
  // 允许的最大失败唯一拉取数量
  private final int maxFailedUniqueFetches;
  // 汇报失败前允许的最大拉取失败次数
  private final int maxFetchFailuresBeforeReporting;

  // 截至当前累计拉取的总字节数
  private long totalBytesShuffledTillNow = 0;
  private final DecimalFormat mbpsFormat = new DecimalFormat("0.00");

  // 是否立即汇报读取错误
  private final boolean reportReadErrorImmediately;
  // 最大惩罚延迟时间
  private long maxPenalty = MRJobConfig.DEFAULT_MAX_SHUFFLE_FETCH_RETRY_DELAY;
  // 允许单个主机的最大失败次数
  private int maxHostFailures;

  /**
   * 构造Shuffle调度器，初始化配置与状态
   * @param job 作业配置
   * @param status Reduce任务状态
   * @param reduceId 当前Reduce任务AttemptID
   * @param reporter 异常报告器
   * @param progress 进度对象
   * @param shuffledMapsCounter 已完成拉取Map计数器
   * @param reduceShuffleBytes 总拉取字节计数器
   * @param failedShuffleCounter 拉取失败计数器
   */
  public ShuffleSchedulerImpl(JobConf job, TaskStatus status,
                          TaskAttemptID reduceId,
                          ExceptionReporter reporter,
                          Progress progress,
                          Counters.Counter shuffledMapsCounter,
                          Counters.Counter reduceShuffleBytes,
                          Counters.Counter failedShuffleCounter) {
    totalMaps = job.getNumMapTasks();
    abortFailureLimit = Math.max(30, totalMaps / 10);
    copyTimeTracker = new CopyTimeTracker();
    remainingMaps = totalMaps;
    finishedMaps = new boolean[remainingMaps];
    this.reporter = reporter;
    this.status = status;
    this.reduceId = reduceId;
    this.progress = progress;
    this.shuffledMapsCounter = shuffledMapsCounter;
    this.reduceShuffleBytes = reduceShuffleBytes;
    this.failedShuffleCounter = failedShuffleCounter;
    this.startTime = Time.monotonicNow();
    lastProgressTime = startTime;
    referee.start();
    this.maxFailedUniqueFetches = Math.min(totalMaps, 5);
    this.maxFetchFailuresBeforeReporting = job.getInt(
        MRJobConfig.SHUFFLE_FETCH_FAILURES, REPORT_FAILURE_LIMIT);
    this.reportReadErrorImmediately = job.getBoolean(
        MRJobConfig.SHUFFLE_NOTIFY_READERROR, true);

    this.maxPenalty = job.getLong(MRJobConfig.MAX_SHUFFLE_FETCH_RETRY_DELAY,
        MRJobConfig.DEFAULT_MAX_SHUFFLE_FETCH_RETRY_DELAY);
    this.maxHostFailures = job.getInt(
        MRJobConfig.MAX_SHUFFLE_FETCH_HOST_FAILURES,
        MRJobConfig.DEFAULT_MAX_SHUFFLE_FETCH_HOST_FAILURES);
  }

  @Override
  /**
   * 根据Map任务完成事件，处理Map输出信息更新
   * @param event Map任务完成事件
   */
  public void resolve(TaskCompletionEvent event) {
    switch (event.getTaskStatus()) {
    case SUCCEEDED:
      // Map成功，构造Map输出URI，添加到已知输出列表
      URI u = getBaseURI(reduceId, event.getTaskTrackerHttp());
      addKnownMapOutput(u.getHost() + ":" + u.getPort(),
          u.toString(),
          event.getTaskAttemptId());
      maxMapRuntime = Math.max(maxMapRuntime, event.getTaskRunTime());
      break;
    case FAILED:
    case KILLED:
    case OBSOLETE:
      // Map失败/被杀死/已废弃，标记输出为废弃
      obsoleteMapOutput(event.getTaskAttemptId());
      LOG.info("Ignoring obsolete output of " + event.getTaskStatus() +
          " map-task: '" + event.getTaskAttemptId() + "'");
      break;
    case TIPFAILED:
      // 整个Map任务失败，标记该任务所有输出为完成
      tipFailed(event.getTaskAttemptId().getTaskID());
      LOG.info("Ignoring output of failed map TIP: '" +
          event.getTaskAttemptId() + "'");
      break;
    }
  }

  /**
   * 构造Map输出的HTTP访问基地址URI
   * @param reduceId 当前Reduce任务ID
   * @param url TaskTracker的HTTP地址
   * @return 构造完成的Map输出访问URI
   */
  static URI getBaseURI(TaskAttemptID reduceId, String url) {
    StringBuilder baseUrl = new StringBuilder(url);
    if (!url.endsWith("/")) {
      baseUrl.append("/");
    }
    baseUrl.append("mapOutput?job=");
    baseUrl.append(reduceId.getJobID());
    baseUrl.append("&reduce=");
    baseUrl.append(reduceId.getTaskID().getId());
    baseUrl.append("&map=");
    URI u = URI.create(baseUrl.toString());
    return u;
  }

  /**
   * 处理从指定主机拉取指定Map成功的逻辑，更新进度与状态
   * @param mapId 拉取成功的Map AttemptID
   * @param host 提供输出的主机
   * @param bytes 拉取的字节数
   * @param startMillis 拉取开始时间
   * @param endMillis 拉取结束时间
   * @param output Map输出对象
   * @throws IOException
   */
  public synchronized void copySucceeded(TaskAttemptID mapId,
                                         MapHost host,
                                         long bytes,
                                         long startMillis,
                                         long endMillis,
                                         MapOutput<K,V> output
                                         ) throws IOException {
    // 清除失败计数
    failureCounts.remove(mapId);
    hostFailures.remove(host.getHostName());
    int mapIndex = mapId.getTaskID().getId();

    if (!finishedMaps[mapIndex]) {
      // 提交Map输出，标记完成
      output.commit();
      finishedMaps[mapIndex] = true;
      shuffledMapsCounter.increment(1);
      if (--remainingMaps == 0) {
        // 所有Map拉取完成，通知等待线程
        notifyAll();
      }

      // 计算本次拉取速率
      long copyMillis = (endMillis - startMillis);
      if (copyMillis == 0) copyMillis = 1;
      float bytesPerMillis = (float) bytes / copyMillis;
      float transferRate = bytesPerMillis * BYTES_PER_MILLIS_TO_MBS;
      String individualProgress = "copy task(" + mapId + " succeeded"
          + " at " + mbpsFormat.format(transferRate) + " MB/s)";
      // 更新总拷贝时间统计
      copyTimeTracker.add(startMillis, endMillis);

      totalBytesShuffledTillNow += bytes;
      updateStatus(individualProgress);
      reduceShuffleBytes.increment(bytes);
      lastProgressTime = Time.monotonicNow();
      LOG.debug("map " + mapId + " done " + status.getStateString());
    } else {
      LOG.warn("Aborting already-finished MapOutput for " + mapId);
      output.abort();
    }
  }

  /**
   * 更新Shuffle进度与状态信息，计算整体拉取速率
   * @param individualProgress 本次拉取进度描述
   */
  private synchronized void updateStatus(String individualProgress) {
    int mapsDone = totalMaps - remainingMaps;
    long totalCopyMillis = copyTimeTracker.getCopyMillis();
    if (totalCopyMillis == 0) totalCopyMillis = 1;
    float bytesPerMillis = (float) totalBytesShuffledTillNow / totalCopyMillis;
    float transferRate = bytesPerMillis * BYTES_PER_MILLIS_TO_MBS;
    // 更新进度百分比
    progress.set((float) mapsDone / totalMaps);
    String statusString = mapsDone + " / " + totalMaps + " copied.";
    status.setStateString(statusString);

    // 拼接进度状态字符串，包含聚合速率
    if (individualProgress != null) {
      progress.setStatus(individualProgress + " Aggregated copy rate(" + 
          mapsDone + " of " + totalMaps + " at " + 
      mbpsFormat.format(transferRate) + " MB/s)");
    } else {
      progress.setStatus("copy(" + mapsDone + " of " + totalMaps + " at "
          + mbpsFormat.format(transferRate) + " MB/s)");
    }
  }
  
  private void updateStatus() {
    updateStatus(null);
  }

  /**
   * 记录主机拉取失败次数
   * @param hostname 失败主机名
   */
  public synchronized void hostFailed(String hostname) {
    if (hostFailures.containsKey(hostname)) {
      IntWritable x = hostFailures.get(hostname);
      x.set(x.get() + 1);
    } else {
      hostFailures.put(hostname, new IntWritable(1));
    }
  }

  @VisibleForTesting
  /**
   * 获取指定主机的失败次数，仅用于测试
   * @param hostname 主机名
   * @return 失败次数
   */
  synchronized int hostFailureCount(String hostname) {
    int failures = 0;
    if (hostFailures.containsKey(hostname)) {
      failures = hostFailures.get(hostname).get();
    }
    return failures;
  }

  @VisibleForTesting
  /**
   * 获取指定Map任务Attempt的拉取失败次数，仅用于测试
   * @param mapId Map任务AttemptID
   * @return 失败次数
   */
  synchronized int fetchFailureCount(TaskAttemptID mapId) {
    int failures = 0;
    if (failureCounts.containsKey(mapId)) {
      failures = failureCounts.get(mapId).get();
    }
    return failures;
  }

  /**
   * 处理从指定主机拉取指定Map失败的逻辑，更新失败计数、检查健康状态、施加惩罚
   * @param mapId 拉取失败的Map AttemptID
   * @param host 提供输出的主机
   * @param readError 是否是读取错误
   * @param connectExcpt 是否是连接异常
   */
  public synchronized void copyFailed(TaskAttemptID mapId, MapHost host,
      boolean readError, boolean connectExcpt) {
    int failures = 1;
    // 更新Map拉取失败计数
    if (failureCounts.containsKey(mapId)) {
      IntWritable x = failureCounts.get(mapId);
      x.set(x.get() + 1);
      failures = x.get();
    } else {
      failureCounts.put(mapId, new IntWritable(1));
    }
    String hostname = host.getHostName();
    IntWritable hostFailedNum = hostFailures.get(hostname);
    // 处理并发导致的主机失败计数为空的情况，避免NPE
    if (hostFailedNum == null) {
      hostFailures.put(hostname, new IntWritable(1));
    }
    // 判断主机失败次数是否超过阈值
    boolean hostFail = hostFailures.get(hostname).get() >
        getMaxHostFailures() ? true : false;

    // 单个Map失败超过阈值，抛出异常终止任务
    if (failures >= abortFailureLimit) {
      try {
        throw new IOException(failures + " failures downloading " + mapId);
      } catch (IOException ie) {
        reporter.reportException(ie);
      }
    }

    // 检查是否需要向MRAppMaster汇报失败
    checkAndInformMRAppMaster(failures, mapId, readError, connectExcpt,
        hostFail);

    // 检查Reduce健康状态，失败过多则终止
    checkReducerHealth();

    // 计算惩罚延迟，失败越多延迟越长，不超过最大限制
    long delay = (long) (INITIAL_PENALTY *
        Math.pow(PENALTY_GROWTH_RATE, failures));
    penalize(host, Math.min(delay, maxPenalty));

    failedShuffleCounter