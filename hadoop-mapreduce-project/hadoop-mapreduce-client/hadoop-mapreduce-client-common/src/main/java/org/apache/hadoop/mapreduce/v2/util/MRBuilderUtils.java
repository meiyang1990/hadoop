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

package org.apache.hadoop.mapreduce.v2.util;

import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.util.Records;

/**
 * MapReduce V2 框架对象构建工具类，提供各类核心实体对象的快捷创建方法
 * 统一封装Record对象初始化逻辑，简化MapReduce各类标识、报告信息的构建
 */
public class MRBuilderUtils {

  /**
   * 根据应用ID和作业编号创建新的作业ID对象
   * @param appId YARN应用ID
   * @param id 作业在应用内的编号
   * @return 构建完成的JobId对象
   */
  public static JobId newJobId(ApplicationId appId, int id) {
    JobId jobId = Records.newRecord(JobId.class);
    jobId.setAppId(appId);
    jobId.setId(id);
    return jobId;
  }

  /**
   * 根据集群时间戳、应用编号和作业编号创建新的作业ID对象
   * @param clusterTs 集群时间戳，用于生成应用ID
   * @param appIdInt 应用编号
   * @param id 作业在应用内的编号
   * @return 构建完成的JobId对象
   */
  public static JobId newJobId(long clusterTs, int appIdInt, int id) {
    ApplicationId appId = ApplicationId.newInstance(clusterTs, appIdInt);
    return MRBuilderUtils.newJobId(appId, id);
  }

  /**
   * 根据作业ID、任务编号和任务类型创建新的任务ID对象
   * @param jobId 所属作业ID
   * @param id 任务在作业内的编号
   * @param taskType 任务类型（MAP/REDUCE）
   * @return 构建完成的TaskId对象
   */
  public static TaskId newTaskId(JobId jobId, int id, TaskType taskType) {
    TaskId taskId = Records.newRecord(TaskId.class);
    taskId.setJobId(jobId);
    taskId.setId(id);
    taskId.setTaskType(taskType);
    return taskId;
  }

  /**
   * 根据任务ID和尝试编号创建新的任务尝试ID对象
   * @param taskId 所属任务ID
   * @param attemptId 任务尝试编号
   * @return 构建完成的TaskAttemptId对象
   */
  public static TaskAttemptId newTaskAttemptId(TaskId taskId, int attemptId) {
    TaskAttemptId taskAttemptId =
        Records.newRecord(TaskAttemptId.class);
    taskAttemptId.setTaskId(taskId);
    taskAttemptId.setId(attemptId);
    return taskAttemptId;
  }

  /**
   * 创建新的作业报告对象，使用默认优先级0
   * @param jobId 作业ID
   * @param jobName 作业名称
   * @param userName 提交作业用户名
   * @param state 作业当前状态
   * @param submitTime 作业提交时间
   * @param startTime 作业启动时间
   * @param finishTime 作业完成时间
   * @param setupProgress 作业初始化阶段进度
   * @param mapProgress Map阶段进度
   * @param reduceProgress Reduce阶段进度
   * @param cleanupProgress 作业清理阶段进度
   * @param jobFile 作业配置文件路径
   * @param amInfos 应用Master信息列表
   * @param isUber 是否运行在Uber模式（所有任务都在AM容器中运行）
   * @param diagnostics 诊断信息
   * @return 构建完成的JobReport对象
   */
  public static JobReport newJobReport(JobId jobId, String jobName,
      String userName, JobState state, long submitTime, long startTime,
      long finishTime, float setupProgress, float mapProgress,
      float reduceProgress, float cleanupProgress, String jobFile,
      List<AMInfo> amInfos, boolean isUber, String diagnostics) {
    return newJobReport(jobId, jobName, userName, state, submitTime, startTime,
        finishTime, setupProgress, mapProgress, reduceProgress,
        cleanupProgress, jobFile, amInfos, isUber, diagnostics,
        Priority.newInstance(0));
  }

  /**
   * 创建新的作业报告对象，支持自定义优先级
   * @param jobId 作业ID
   * @param jobName 作业名称
   * @param userName 提交作业用户名
   * @param state 作业当前状态
   * @param submitTime 作业提交时间
   * @param startTime 作业启动时间
   * @param finishTime 作业完成时间
   * @param setupProgress 作业初始化阶段进度
   * @param mapProgress Map阶段进度
   * @param reduceProgress Reduce阶段进度
   * @param cleanupProgress 作业清理阶段进度
   * @param jobFile 作业配置文件路径
   * @param amInfos 应用Master信息列表
   * @param isUber 是否运行在Uber模式（所有任务都在AM容器中运行）
   * @param diagnostics 诊断信息
   * @param priority 作业调度优先级
   * @return 构建完成的JobReport对象
   */
  public static JobReport newJobReport(JobId jobId, String jobName,
      String userName, JobState state, long submitTime, long startTime, long finishTime,
      float setupProgress, float mapProgress, float reduceProgress,
      float cleanupProgress, String jobFile, List<AMInfo> amInfos,
      boolean isUber, String diagnostics, Priority priority) {
    JobReport report = Records.newRecord(JobReport.class);
    report.setJobId(jobId);
    report.setJobName(jobName);
    report.setUser(userName);
    report.setJobState(state);
    report.setSubmitTime(submitTime);
    report.setStartTime(startTime);
    report.setFinishTime(finishTime);
    report.setSetupProgress(setupProgress);
    report.setCleanupProgress(cleanupProgress);
    report.setMapProgress(mapProgress);
    report.setReduceProgress(reduceProgress);
    report.setJobFile(jobFile);
    report.setAMInfos(amInfos);
    report.setIsUber(isUber);
    report.setDiagnostics(diagnostics);
    report.setJobPriority(priority);
    return report;
  }

  /**
   * 创建新的应用Master（ApplicationMaster）信息对象，记录AM的位置和启动信息
   * @param appAttemptId YARN应用尝试ID
   * @param startTime AM启动时间
   * @param containerId AM运行所在容器ID
   * @param nmHost AM所在NodeManager主机地址
   * @param nmPort NodeManager服务端口
   * @param nmHttpPort NodeManager HTTP监控端口
   * @return 构建完成的AMInfo对象
   */
  public static AMInfo newAMInfo(ApplicationAttemptId appAttemptId,
      long startTime, ContainerId containerId, String nmHost, int nmPort,
      int nmHttpPort) {
    AMInfo amInfo = Records.newRecord(AMInfo.class);
    amInfo.setAppAttemptId(appAttemptId);
    amInfo.setStartTime(startTime);
    amInfo.setContainerId(containerId);
    amInfo.setNodeManagerHost(nmHost);
    amInfo.setNodeManagerPort(nmPort);
    amInfo.setNodeManagerHttpPort(nmHttpPort);
    return amInfo;
  }
}