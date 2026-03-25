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

package org.apache.hadoop.mapreduce.v2.api.records;

import java.util.List;

import org.apache.hadoop.yarn.api.records.Priority;

/**
 * 作业运行报告接口，定义MapReduce作业状态与进度信息的获取/设置方法
 * 用于客户端查询作业运行情况，封装作业的全部元信息与执行状态
 */
public interface JobReport {
  /**
   * 获取作业唯一标识
   * @return 作业ID对象
   */
  public abstract JobId getJobId();
  /**
   * 获取作业当前运行状态
   * @return 作业状态枚举
   */
  public abstract JobState getJobState();
  /**
   * 获取Map阶段执行进度
   * @return 0-1之间的进度浮点数
   */
  public abstract float getMapProgress();
  /**
   * 获取Reduce阶段执行进度
   * @return 0-1之间的进度浮点数
   */
  public abstract float getReduceProgress();
  /**
   * 获取清理阶段执行进度
   * @return 0-1之间的进度浮点数
   */
  public abstract float getCleanupProgress();
  /**
   * 获取初始化阶段执行进度
   * @return 0-1之间的进度浮点数
   */
  public abstract float getSetupProgress();
  /**
   * 获取作业提交时间戳
   * @return 提交时间（毫秒，自1970-01-01起）
   */
  public abstract long getSubmitTime();
  /**
   * 获取作业开始运行时间戳
   * @return 开始时间（毫秒，自1970-01-01起）
   */
  public abstract long getStartTime();
  /**
   * 获取作业完成时间戳
   * @return 完成时间（毫秒，自1970-01-01起）
   */
  public abstract long getFinishTime();
  /**
   * 获取提交作业的用户名
   * @return 用户名字符串
   */
  public abstract String getUser();
  /**
   * 获取作业名称
   * @return 作业名称字符串
   */
  public abstract String getJobName();
  /**
   * 获取作业追踪页面URL
   * @return Web追踪页面地址
   */
  public abstract String getTrackingUrl();
  /**
   * 获取作业诊断信息（错误日志、失败原因等）
   * @return 诊断信息字符串
   */
  public abstract String getDiagnostics();
  /**
   * 获取作业配置文件路径
   * @return 作业文件路径
   */
  public abstract String getJobFile();
  /**
   * 获取ApplicationMaster信息列表（包含尝试运行信息）
   * @return AM信息列表
   */
  public abstract List<AMInfo> getAMInfos();
  /**
   * 判断作业是否运行在Uber模式（所有任务在同一个AM容器中运行）
   * @return true表示开启Uber模式，false表示正常模式
   */
  public abstract boolean isUber();
  /**
   * 获取作业调度优先级
   * @return YARN优先级对象
   */
  public abstract Priority getJobPriority();
  /**
   * 获取作业历史文件路径
   * @return 历史文件路径字符串
   */
  public abstract String getHistoryFile();

  /**
   * 设置作业唯一标识
   * @param jobId 作业ID对象
   */
  public abstract void setJobId(JobId jobId);
  /**
   * 设置作业运行状态
   * @param jobState 作业状态枚举
   */
  public abstract void setJobState(JobState jobState);
  /**
   * 设置Map阶段执行进度
   * @param progress 0-1之间的进度浮点数
   */
  public abstract void setMapProgress(float progress);
  /**
   * 设置Reduce阶段执行进度
   * @param progress 0-1之间的进度浮点数
   */
  public abstract void setReduceProgress(float progress);
  /**
   * 设置清理阶段执行进度
   * @param progress 0-1之间的进度浮点数
   */
  public abstract void setCleanupProgress(float progress);
  /**
   * 设置初始化阶段执行进度
   * @param progress 0-1之间的进度浮点数
   */
  public abstract void setSetupProgress(float progress);
  /**
   * 设置作业提交时间戳
   * @param submitTime 提交时间（毫秒，自1970-01-01起）
   */
  public abstract void setSubmitTime(long submitTime);
  /**
   * 设置作业开始运行时间戳
   * @param startTime 开始时间（毫秒，自1970-01-01起）
   */
  public abstract void setStartTime(long startTime);
  /**
   * 设置作业完成时间戳
   * @param finishTime 完成时间（毫秒，自1970-01-01起）
   */
  public abstract void setFinishTime(long finishTime);
  /**
   * 设置提交作业的用户名
   * @param user 用户名字符串
   */
  public abstract void setUser(String user);
  /**
   * 设置作业名称
   * @param jobName 作业名称字符串
   */
  public abstract void setJobName(String jobName);
  /**
   * 设置作业追踪页面URL
   * @param trackingUrl Web追踪页面地址
   */
  public abstract void setTrackingUrl(String trackingUrl);
  /**
   * 设置作业诊断信息
   * @param diagnostics 诊断信息字符串
   */
  public abstract void setDiagnostics(String diagnostics);
  /**
   * 设置作业配置文件路径
   * @param jobFile 作业文件路径
   */
  public abstract void setJobFile(String jobFile);
  /**
   * 设置ApplicationMaster信息列表
   * @param amInfos AM信息列表
   */
  public abstract void setAMInfos(List<AMInfo> amInfos);
  /**
   * 设置是否开启Uber模式
   * @param isUber true表示开启Uber模式
   */
  public abstract void setIsUber(boolean isUber);
  /**
   * 设置作业调度优先级
   * @param priority YARN优先级对象
   */
  public abstract void setJobPriority(Priority priority);
  /**
   * 设置作业历史文件路径
   * @param historyFile 历史文件路径字符串
   */
  public abstract void setHistoryFile(String historyFile);
}