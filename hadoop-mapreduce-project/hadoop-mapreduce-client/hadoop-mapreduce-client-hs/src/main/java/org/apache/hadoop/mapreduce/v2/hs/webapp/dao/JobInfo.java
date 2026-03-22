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
package org.apache.hadoop.mapreduce.v2.hs.webapp.dao;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ConfEntryInfo;
import org.apache.hadoop.mapreduce.v2.hs.CompletedJob;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRApps.TaskAttemptStateUI;
import org.apache.hadoop.security.authorize.AccessControlList;

/**
 * 历史服务器Web界面的Job信息数据访问对象，封装已完成作业的核心统计信息
 * 用于为Web界面提供作业基本信息、任务统计、尝试统计等数据的序列化与传输
 */
@XmlRootElement(name = "job")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobInfo {
  @VisibleForTesting
  static final String NA = "N/A";

  protected long submitTime;
  protected long startTime;
  protected long finishTime;
  protected String id;
  protected String name;
  protected String queue;
  protected String user;
  protected String state;
  protected int mapsTotal;
  protected int mapsCompleted;
  protected int reducesTotal;
  protected int reducesCompleted;
  protected Boolean uberized;
  protected String diagnostics;
  protected Long avgMapTime;
  protected Long avgReduceTime;
  protected Long avgShuffleTime;
  protected Long avgMergeTime;
  protected Integer failedReduceAttempts;
  protected Integer killedReduceAttempts;
  protected Integer successfulReduceAttempts;
  protected Integer failedMapAttempts;
  protected Integer killedMapAttempts;
  protected Integer successfulMapAttempts;
  protected ArrayList<ConfEntryInfo> acls;

  @XmlTransient
  protected int numMaps;
  @XmlTransient
  protected int numReduces;

  /**
   * 无参构造函数，供JAXB序列化/反序列化使用
   */
  public JobInfo() {
  }

  /**
   * 基于Job对象构造JobInfo，提取作业核心统计信息
   * @param job 源作业对象，可为已完成作业实例
   */
  public JobInfo(Job job) {
    this.id = MRApps.toString(job.getID());
    JobReport report = job.getReport();
    
    this.mapsTotal = job.getTotalMaps();
    this.mapsCompleted = job.getCompletedMaps();
    this.reducesTotal = job.getTotalReduces();
    this.reducesCompleted = job.getCompletedReduces();
    this.submitTime = report.getSubmitTime();
    this.startTime = report.getStartTime();
    this.finishTime = report.getFinishTime();
    this.name = job.getName().toString();
    this.queue = job.getQueueName();
    this.user = job.getUserName();
    this.state = job.getState().toString();

    this.acls = new ArrayList<ConfEntryInfo>();
    
    // 仅对已完成作业补充详细统计信息
    if (job instanceof CompletedJob) {
      // 初始化各类统计计数器
      avgMapTime = 0l;
      avgReduceTime = 0l;
      avgShuffleTime = 0l;
      avgMergeTime = 0l;
      failedReduceAttempts = 0;
      killedReduceAttempts = 0;
      successfulReduceAttempts = 0;
      failedMapAttempts = 0;
      killedMapAttempts = 0;
      successfulMapAttempts = 0;
      // 统计任务和尝试的各项指标
      countTasksAndAttempts(job);
      this.uberized = job.isUber();
      this.diagnostics = "";
      List<String> diagnostics = job.getDiagnostics();
      // 拼接诊断信息
      if (diagnostics != null && !diagnostics.isEmpty()) {
        StringBuilder b = new StringBuilder();
        for (String diag : diagnostics) {
          b.append(diag);
        }
        this.diagnostics = b.toString();
      }

      // 提取作业访问控制列表信息
      Map<JobACL, AccessControlList> allacls = job.getJobACLs();
      if (allacls != null) {
        for (Map.Entry<JobACL, AccessControlList> entry : allacls.entrySet()) {
          this.acls.add(new ConfEntryInfo(entry.getKey().getAclName(), entry
              .getValue().getAclString()));
        }
      }
    }
  }

  public long getNumMaps() {
    return numMaps;
  }

  public long getNumReduces() {
    return numReduces;
  }

  public Long getAvgMapTime() {
    return avgMapTime;
  }

  public Long getAvgReduceTime() {
    return avgReduceTime;
  }

  public Long getAvgShuffleTime() {
    return avgShuffleTime;
  }

  public Long getAvgMergeTime() {
    return avgMergeTime;
  }

  public Integer getFailedReduceAttempts() {
    return failedReduceAttempts;
  }

  public Integer getKilledReduceAttempts() {
    return killedReduceAttempts;
  }

  public Integer getSuccessfulReduceAttempts() {
    return successfulReduceAttempts;
  }

  public Integer getFailedMapAttempts() {
    return failedMapAttempts;
  }

  public Integer getKilledMapAttempts() {
    return killedMapAttempts;
  }

  public Integer getSuccessfulMapAttempts() {
    return successfulMapAttempts;
  }

  public ArrayList<ConfEntryInfo> getAcls() {
    return acls;
  }

  public int getReducesCompleted() {
    return this.reducesCompleted;
  }

  public int getReducesTotal() {
    return this.reducesTotal;
  }

  public int getMapsCompleted() {
    return this.mapsCompleted;
  }

  public int getMapsTotal() {
    return this.mapsTotal;
  }

  public String getState() {
    return this.state;
  }

  public String getUserName() {
    return this.user;
  }

  public String getName() {
    return this.name;
  }

  public String getQueueName() {
    return this.queue;
  }

  public String getId() {
    return this.id;
  }

  public long getSubmitTime() {
      return this.submitTime;
  }

  public long getStartTime() {
    return this.startTime;
  }

  /**
   * 使用指定DateFormat格式化作业开始时间
   * @param dateFormat 格式化器
   * @return 格式化后的时间字符串，未设置开始时间返回N/A
   */
  public String getFormattedStartTimeStr(final DateFormat dateFormat) {
    String str = NA;

    if (startTime >= 0) {
      str = dateFormat.format(new Date(startTime));
    }

    return str;
  }

  /**
   * 获取默认格式的作业开始时间字符串
   * @return 时间字符串，未设置开始时间返回N/A
   */
  public String getStartTimeStr() {
    String str = NA;

    if (startTime >= 0) {
      str = new Date(startTime).toString();
    }

    return str;
  }

  public long getFinishTime() {
    return this.finishTime;
  }

  public Boolean isUber() {
    return this.uberized;
  }

  public String getDiagnostics() {
    return this.diagnostics;
  }

  /**
   * 遍历作业所有任务，统计任务尝试数量、计算各阶段平均耗时，更新成员变量
   * 用于为Web页面提供详细的任务统计信息
   * @param job 要统计的作业对象
   */
  private void countTasksAndAttempts(Job job) {
    numReduces = 0;
    numMaps = 0;
    final Map<TaskId, Task> tasks = job.getTasks();
    if (tasks == null) {
      return;
    }
    // 遍历所有任务
    for (Task task : tasks.values()) {
      // 遍历任务的所有尝试
      Map<TaskAttemptId, TaskAttempt> attempts = task.getAttempts();
      int successful, failed, killed;
      for (TaskAttempt attempt : attempts.values()) {

        successful = 0;
        failed = 0;
        killed = 0;
        // 根据尝试状态分类计数
        if (TaskAttemptStateUI.NEW.correspondsTo(attempt.getState())) {
          // 新建状态不统计
        } else if (TaskAttemptStateUI.RUNNING.correspondsTo(attempt.getState())) {
          // 运行中状态不统计
        } else if (TaskAttemptStateUI.SUCCESSFUL.correspondsTo(attempt
            .getState())) {
          ++successful;
        } else if (TaskAttemptStateUI.FAILED.correspondsTo(attempt.getState())) {
          ++failed;
        } else if (TaskAttemptStateUI.KILLED.correspondsTo(attempt.getState())) {
          ++killed;
        }

        // 按任务类型分别累计统计
        switch (task.getType()) {
        case MAP:
          // Map任务尝试计数累加
          successfulMapAttempts += successful;
          failedMapAttempts += failed;
          killedMapAttempts += killed;
          // 成功Map任务累加总耗时，用于计算平均时间
          if (attempt.getState() == TaskAttemptState.SUCCEEDED) {
            numMaps++;
            avgMapTime += (attempt.getFinishTime() - attempt.getLaunchTime());
          }
          break;
        case REDUCE:
          // Reduce任务尝试计数累加
          successfulReduceAttempts += successful;
          failedReduceAttempts += failed;
          killedReduceAttempts += killed;
          // 成功Reduce任务分阶段累加总耗时，用于计算各阶段平均时间
          if (attempt.getState() == TaskAttemptState.SUCCEEDED) {
            numReduces++;
            avgShuffleTime += (attempt.getShuffleFinishTime() - attempt
                .getLaunchTime());
            avgMergeTime += attempt.getSortFinishTime()
                - attempt.getShuffleFinishTime();
            avgReduceTime += (attempt.getFinishTime() - attempt
                .getSortFinishTime());
          }
          break;
        }
      }
    }

    // 计算平均Map耗时
    if (numMaps > 0) {
      avgMapTime = avgMapTime / numMaps;
    }

    // 计算Reduce各阶段平均耗时
    if (numReduces > 0) {
      avgReduceTime = avgReduceTime / numReduces;
      avgShuffleTime = avgShuffleTime / numReduces;
      avgMergeTime = avgMergeTime / numReduces;
    }
  }

}