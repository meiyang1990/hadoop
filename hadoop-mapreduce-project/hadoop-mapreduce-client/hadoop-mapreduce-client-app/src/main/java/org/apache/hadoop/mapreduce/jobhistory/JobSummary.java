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

package org.apache.hadoop.mapreduce.jobhistory;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.util.StringUtils;

/**
 * MapReduce作业运行摘要信息存储类，用于记录作业整体运行统计数据和关键时间节点，
 * 为作业历史分析、监控统计提供基础数据结构。
 */
public class JobSummary {
  private JobId jobId;
  private long jobSubmitTime;
  private long jobLaunchTime;
  private long firstMapTaskLaunchTime; // MapAttemptStarted 事件记录的第一个Map任务启动时间
  private long firstReduceTaskLaunchTime; // ReduceAttemptStarted 事件记录的第一个Reduce任务启动时间
  private long jobFinishTime;
  private int numSucceededMaps;
  private int numFailedMaps;
  private int numSucceededReduces;
  private int numFailedReduces;
  private int numKilledMaps;
  private int numKilledReduces;
  private int resourcesPerMap; // 每个Map任务占用的资源量（单位为最小资源单元）
  private int resourcesPerReduce; // 每个Reduce任务占用的资源量（单位为最小资源单元）
  // resource models
  // private int numSlotsPerReduce; | Doesn't make sense with potentially
  // different resource models
  private String user;
  private queue;
  private String jobStatus;
  private long mapSlotSeconds; // TODO Not generated yet in MRV2
  private long reduceSlotSeconds; // TODO Not generated yet MRV2
  // private int clusterSlotCapacity;
  private String jobName;

  JobSummary() {
  }

  /**
   * 获取作业ID
   * @return 作业唯一标识ID
   */
  public JobId getJobId() {
    return jobId;
  }

  /**
   * 设置作业ID
   * @param jobId 作业唯一标识ID
   */
  public void setJobId(JobId jobId) {
    this.jobId = jobId;
  }

  /**
   * 获取作业提交时间
   * @return 作业提交时间戳（毫秒）
   */
  public long getJobSubmitTime() {
    return jobSubmitTime;
  }

  /**
   * 设置作业提交时间
   * @param jobSubmitTime 作业提交时间戳（毫秒）
   */
  public void setJobSubmitTime(long jobSubmitTime) {
    this.jobSubmitTime = jobSubmitTime;
  }

  /**
   * 获取作业启动时间
   * @return 作业启动时间戳（毫秒）
   */
  public long getJobLaunchTime() {
    return jobLaunchTime;
  }

  /**
   * 设置作业启动时间
   * @param jobLaunchTime 作业启动时间戳（毫秒）
   */
  public void setJobLaunchTime(long jobLaunchTime) {
    this.jobLaunchTime = jobLaunchTime;
  }

  /**
   * 获取第一个Map任务启动时间
   * @return 第一个Map任务启动时间戳（毫秒）
   */
  public long getFirstMapTaskLaunchTime() {
    return firstMapTaskLaunchTime;
  }

  /**
   * 设置第一个Map任务启动时间
   * @param firstMapTaskLaunchTime 第一个Map任务启动时间戳（毫秒）
   */
  public void setFirstMapTaskLaunchTime(long firstMapTaskLaunchTime) {
    this.firstMapTaskLaunchTime = firstMapTaskLaunchTime;
  }

  /**
   * 获取第一个Reduce任务启动时间
   * @return 第一个Reduce任务启动时间戳（毫秒）
   */
  public long getFirstReduceTaskLaunchTime() {
    return firstReduceTaskLaunchTime;
  }

  /**
   * 设置第一个Reduce任务启动时间
   * @param firstReduceTaskLaunchTime 第一个Reduce任务启动时间戳（毫秒）
   */
  public void setFirstReduceTaskLaunchTime(long firstReduceTaskLaunchTime) {
    this.firstReduceTaskLaunchTime = firstReduceTaskLaunchTime;
  }

  /**
   * 获取作业完成时间
   * @return 作业完成时间戳（毫秒）
   */
  public long getJobFinishTime() {
    return jobFinishTime;
  }

  /**
   * 设置作业完成时间
   * @param jobFinishTime 作业完成时间戳（毫秒）
   */
  public void setJobFinishTime(long jobFinishTime) {
    this.jobFinishTime = jobFinishTime;
  }

  /**
   * 获取成功完成的Map任务数量
   * @return 成功Map任务数
   */
  public int getNumSucceededMaps() {
    return numSucceededMaps;
  }

  /**
   * 设置成功完成的Map任务数量
   * @param numSucceededMaps 成功Map任务数
   */
  public void setNumSucceededMaps(int numSucceededMaps) {
    this.numSucceededMaps = numSucceededMaps;
  }

  /**
   * 获取失败的Map任务数量
   * @return 失败Map任务数
   */
  public int getNumFailedMaps() {
    return numFailedMaps;
  }

  /**
   * 设置失败的Map任务数量
   * @param numFailedMaps 失败Map任务数
   */
  public void setNumFailedMaps(int numFailedMaps) {
    this.numFailedMaps = numFailedMaps;
  }

  /**
   * 获取被杀死的Map任务数量
   * @return 被杀死Map任务数
   */
  public int getKilledMaps() {
    return numKilledMaps;
  }

  /**
   * 设置被杀死的Map任务数量
   * @param numKilledMaps 被杀死Map任务数
   */
  public void setNumKilledMaps(int numKilledMaps) {
    this.numKilledMaps = numKilledMaps;
  }

  /**
   * 获取被杀死的Reduce任务数量
   * @return 被杀死Reduce任务数
   */
  public int getKilledReduces() {
    return numKilledReduces;
  }

  /**
   * 设置被杀死的Reduce任务数量
   * @param numKilledReduces 被杀死Reduce任务数
   */
  public void setNumKilledReduces(int numKilledReduces) {
    this.numKilledReduces = numKilledReduces;
  }

  /**
   * 获取单个Map任务分配的资源量
   * @return Map任务资源量（单位为最小资源单元）
   */
  public int getResourcesPerMap() {
    return resourcesPerMap;
  }
  
  /**
   * 设置单个Map任务分配的资源量
   * @param resourcesPerMap Map任务资源量（单位为最小资源单元）
   */
  public void setResourcesPerMap(int resourcesPerMap) {
    this.resourcesPerMap = resourcesPerMap;
  }
  
  /**
   * 获取成功完成的Reduce任务数量
   * @return 成功Reduce任务数
   */
  public int getNumSucceededReduces() {
    return numSucceededReduces;
  }

  /**
   * 设置成功完成的Reduce任务数量
   * @param numSucceededReduces 成功Reduce任务数
   */
  public void setNumSucceededReduces(int numSucceededReduces) {
    this.numSucceededReduces = numSucceededReduces;
  }

  /**
   * 获取失败的Reduce任务数量
   * @return 失败Reduce任务数
   */
  public int getNumFailedReduces() {
    return numFailedReduces;
  }

  /**
   * 设置失败的Reduce任务数量
   * @param numFailedReduces 失败Reduce任务数
   */
  public void setNumFailedReduces(int numFailedReduces) {
    this.numFailedReduces = numFailedReduces;
  }

  /**
   * 获取单个Reduce任务分配的资源量
   * @return Reduce任务资源量（单位为最小资源单元）
   */
  public int getResourcesPerReduce() {
    return this.resourcesPerReduce;
  }
  
  /**
   * 设置单个Reduce任务分配的资源量
   * @param resourcesPerReduce Reduce任务资源量（单位为最小资源单元）
   */
  public void setResourcesPerReduce(int resourcesPerReduce) {
    this.resourcesPerReduce = resourcesPerReduce;
  }
  
  /**
   * 获取作业提交用户名
   * @return 提交作业的用户名
   */
  public String getUser() {
    return user;
  }

  /**
   * 设置作业提交用户名
   * @param user 提交作业的用户名
   */
  public void setUser(String user) {
    this.user = user;
  }

  /**
   * 获取作业提交队列名称
   * @return YARN队列名称
   */
  public String getQueue() {
    return queue;
  }

  /**
   * 设置作业提交队列名称
   * @param queue YARN队列名称
   */
  public void setQueue(String queue) {
    this.queue = queue;
  }

  /**
   * 获取作业最终状态
   * @return 作业状态字符串
   */
  public String getJobStatus() {
    return jobStatus;
  }

  /**
   * 设置作业最终状态
   * @param jobStatus 作业状态字符串
   */
  public void setJobStatus(String jobStatus) {
    this.jobStatus = jobStatus;
  }

  /**
   * 获取Map任务总槽位秒数（资源使用量累积）
   * @return Map槽位总秒数
   */
  public long getMapSlotSeconds() {
    return mapSlotSeconds;
  }

  /**
   * 设置Map任务总槽位秒数
   * @param mapSlotSeconds Map槽位总秒数
   */
  public void setMapSlotSeconds(long mapSlotSeconds) {
    this.mapSlotSeconds = mapSlotSeconds;
  }

  /**
   * 获取Reduce任务总槽位秒数（资源使用量累积）
   * @return Reduce槽位总秒数
   */
  public long getReduceSlotSeconds() {
    return reduceSlotSeconds;
  }

  /**
   * 设置Reduce任务总槽位秒数
   * @param reduceSlotSeconds Reduce槽位总秒数
   */
  public void setReduceSlotSeconds(long reduceSlotSeconds) {
    this.reduceSlotSeconds = reduceSlotSeconds;
  }

  /**
   * 获取作业名称
   * @return 作业名称字符串
   */
  public String getJobName() {
    return jobName;
  }

  /**
   * 设置作业名称
   * @param jobName 作业名称字符串
   */
  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  /**
   * 生成格式化的作业摘要字符串，用于存储到作业历史文件
   * @return 键值对格式的作业摘要字符串
   */
  public String getJobSummaryString() {
    SummaryBuilder summary = new SummaryBuilder()
      .add("jobId", jobId)
      .add("submitTime", jobSubmitTime)
      .add("launchTime", jobLaunchTime)
      .add("firstMapTaskLaunchTime", firstMapTaskLaunchTime)
      .add("firstReduceTaskLaunchTime", firstReduceTaskLaunchTime)
      .add("finishTime", jobFinishTime)
      .add("resourcesPerMap", resourcesPerMap)
      .add("resourcesPerReduce", resourcesPerReduce)
      .add("numMaps", numSucceededMaps + numFailedMaps + numKilledMaps)
      .add("numReduces", numSucceededReduces + numFailedReduces
          + numKilledReduces)
      .add("succededMaps", numSucceededMaps)
      .add("succeededReduces", numSucceededReduces)
      .add("failedMaps", numFailedMaps)
      .add("failedReduces", numFailedReduces)
      .add("killedMaps", numKilledMaps)
      .add("killedReduces", numKilledReduces)
      .add("user", user)
      .add("queue", queue)
      .add("status", jobStatus)
      .add("mapSlotSeconds", mapSlotSeconds)
      .add("reduceSlotSeconds", reduceSlotSeconds)
      .add("jobName", jobName);
    return summary.toString();
  }

  static final char EQUALS = '=';
  static final char[] charsToEscape = { StringUtils.COMMA, EQUALS,
      StringUtils.ESCAPE_CHAR };
  
  /**
   * 摘要字符串构建工具类，负责构建转义处理后的键值对格式摘要
   */
  static class SummaryBuilder {
    final StringBuilder buffer = new StringBuilder();

    /**
     * 添加长整型值键值对，优化常见数值类型添加场景
     * @param key 键名
     * @param value 长整型值
     * @return 当前构建器实例
     */
    // A little optimization for a very common case
    SummaryBuilder add(String key, long value) {
      return _add(key, Long.toString(value));
    }

    /**
     * 添加泛型类型键值对，自动对特殊字符进行转义处理
     * @param key 键名
     * @param value 值对象
     * @return 当前构建器实例
     */
    <T> SummaryBuilder add(String key, T value) {
      // 转义特殊字符，替换换行回车为转义序列
      String escapedString = StringUtils.escapeString(String.valueOf(value), 
          StringUtils.ESCAPE_CHAR, charsToEscape).replaceAll("\n", "\\\\n")
                                                 .replaceAll("\r", "\\\\r");
      return _add(key, escapedString);
    }

    /**
     * 合并另一个构建器的内容到当前构建器
     * @param summary 待合并的构建器
     * @return 当前构建器实例
     */
    SummaryBuilder add(SummaryBuilder summary) {
      if (buffer.length() > 0)
        buffer.append(StringUtils.COMMA);
      buffer.append(summary.buffer);
      return this;
    }

    /**
     * 内部添加键值对方法，处理分隔符拼接
     * @param key 键名
     * @param value 已转义的值字符串
     * @return 当前构建器实例
     */
    SummaryBuilder _add(String key, String value) {
      if (buffer.length() > 0)
        buffer.append(StringUtils.COMMA);
      buffer.append(key).append(EQUALS).append(value);
      return this;
    }

    @Override
    public String toString() {
      return buffer.toString();
    }
  }
}