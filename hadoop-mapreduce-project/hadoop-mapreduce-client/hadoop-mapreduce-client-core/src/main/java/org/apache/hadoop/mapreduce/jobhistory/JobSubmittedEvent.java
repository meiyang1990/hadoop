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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

/**
 * 作业提交事件，用于记录MapReduce作业提交时的相关信息，供作业历史审计使用
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobSubmittedEvent implements HistoryEvent {
  // Avro序列化后的事件数据对象
  private JobSubmitted datum = new JobSubmitted();
  // 作业配置对象
  private JobConf jobConf = null;

  /**
   * 构造作业提交事件，不包含工作流信息
   * @param id 作业ID
   * @param jobName 作业名称
   * @param userName 提交作业的用户名
   * @param submitTime 作业提交时间戳
   * @param jobConfPath 作业配置文件路径
   * @param jobACLs 作业访问控制列表配置
   * @param jobQueueName 作业提交到的队列名称
   */
  public JobSubmittedEvent(JobID id, String jobName, String userName,
      long submitTime, String jobConfPath,
      Map<JobACL, AccessControlList> jobACLs, String jobQueueName) {
    this(id, jobName, userName, submitTime, jobConfPath, jobACLs,
        jobQueueName, "", "", "", "");
  }

  /**
   * 构造作业提交事件，包含工作流基本信息，不包含工作流标签
   * @param id 作业ID
   * @param jobName 作业名称
   * @param userName 提交作业的用户名
   * @param submitTime 作业提交时间戳
   * @param jobConfPath 作业配置文件路径
   * @param jobACLs 作业访问控制列表配置
   * @param jobQueueName 作业提交到的队列名称
   * @param workflowId 工作流ID
   * @param workflowName 工作流名称
   * @param workflowNodeName 工作流节点名称
   * @param workflowAdjacencies 工作流依赖邻接表
   */
  public JobSubmittedEvent(JobID id, String jobName, String userName,
      long submitTime, String jobConfPath,
      Map<JobACL, AccessControlList> jobACLs, String jobQueueName,
      String workflowId, String workflowName, String workflowNodeName,
      String workflowAdjacencies) {
    this(id, jobName, userName, submitTime, jobConfPath, jobACLs,
        jobQueueName, workflowId, workflowName, workflowNodeName,
        workflowAdjacencies, "");
  }

  /**
   * 构造作业提交事件，包含完整工作流信息，不包含作业配置
   * @param id 作业ID
   * @param jobName 作业名称
   * @param userName 提交作业的用户名
   * @param submitTime 作业提交时间戳
   * @param jobConfPath 作业配置文件路径
   * @param jobACLs 作业访问控制列表配置
   * @param jobQueueName 作业提交到的队列名称
   * @param workflowId 工作流ID
   * @param workflowName 工作流名称
   * @param workflowNodeName 工作流节点名称
   * @param workflowAdjacencies 工作流依赖邻接表
   * @param workflowTags 工作流标签，逗号分隔
   */
  public JobSubmittedEvent(JobID id, String jobName, String userName,
      long submitTime, String jobConfPath,
      Map<JobACL, AccessControlList> jobACLs, String jobQueueName,
      String workflowId, String workflowName, String workflowNodeName,
      String workflowAdjacencies, String workflowTags) {
    this(id, jobName, userName, submitTime, jobConfPath, jobACLs,
        jobQueueName, workflowId, workflowName, workflowNodeName,
        workflowAdjacencies, workflowTags, null);
  }

  /**
   * 构造完整的作业提交事件，包含所有信息
   * @param id 作业ID
   * @param jobName 作业名称
   * @param userName 提交作业的用户名
   * @param submitTime 作业提交时间戳
   * @param jobConfPath 作业配置文件路径
   * @param jobACLs 作业访问控制列表配置
   * @param jobQueueName 作业提交到的队列名称
   * @param workflowId 工作流ID
   * @param workflowName 工作流名称
   * @param workflowNodeName 工作流节点名称
   * @param workflowAdjacencies 工作流依赖邻接表
   * @param workflowTags 工作流标签，逗号分隔
   * @param conf 作业配置对象
   */
  public JobSubmittedEvent(JobID id, String jobName, String userName,
      long submitTime, String jobConfPath,
      Map<JobACL, AccessControlList> jobACLs, String jobQueueName,
      String workflowId, String workflowName, String workflowNodeName,
      String workflowAdjacencies, String workflowTags, JobConf conf) {
    // 设置作业ID
    datum.setJobid(new Utf8(id.toString()));
    // 设置作业名称
    datum.setJobName(new Utf8(jobName));
    // 设置提交用户名
    datum.setUserName(new Utf8(userName));
    // 设置提交时间
    datum.setSubmitTime(submitTime);
    // 设置作业配置路径
    datum.setJobConfPath(new Utf8(jobConfPath));
    // 转换ACL格式为Avro可存储格式
    Map<CharSequence, CharSequence> jobAcls = new HashMap<CharSequence, CharSequence>();
    for (Entry<JobACL, AccessControlList> entry : jobACLs.entrySet()) {
      jobAcls.put(new Utf8(entry.getKey().getAclName()), new Utf8(
          entry.getValue().getAclString()));
    }
    // 存储ACL信息
    datum.setAcls(jobAcls);
    // 存储作业队列名称（非空时）
    if (jobQueueName != null) {
      datum.setJobQueueName(new Utf8(jobQueueName));
    }
    // 存储工作流ID（非空时）
    if (workflowId != null) {
      datum.setWorkflowId(new Utf8(workflowId));
    }
    // 存储工作流名称（非空时）
    if (workflowName != null) {
      datum.setWorkflowName(new Utf8(workflowName));
    }
    // 存储工作流节点名称（非空时）
    if (workflowNodeName != null) {
      datum.setWorkflowNodeName(new Utf8(workflowNodeName));
    }
    // 存储工作流依赖邻接表（非空时）
    if (workflowAdjacencies != null) {
      datum.setWorkflowAdjacencies(new Utf8(workflowAdjacencies));
    }
    // 存储工作流标签（非空时）
    if (workflowTags != null) {
      datum.setWorkflowTags(new Utf8(workflowTags));
    }
    // 保存作业配置
    jobConf = conf;
  }

  JobSubmittedEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) {
    this.datum = (JobSubmitted)datum;
  }

  /** 获取作业ID */
  public JobID getJobId() { return JobID.forName(datum.getJobid().toString()); }
  /** 获取作业名称 */
  public String getJobName() { return datum.getJobName().toString(); }
  /** 获取作业队列名称 */
  public String getJobQueueName() {
    if (datum.getJobQueueName() != null) {
      return datum.getJobQueueName().toString();
    }
    return null;
  }
  /** 获取提交用户名 */
  public String getUserName() { return datum.getUserName().toString(); }
  /** 获取提交时间戳 */
  public long getSubmitTime() { return datum.getSubmitTime(); }
  /** 获取作业配置文件路径 */
  public String getJobConfPath() { return datum.getJobConfPath().toString(); }
  /** 获取作业访问控制列表配置 **/
  public Map<JobACL, AccessControlList> getJobAcls() {
    Map<JobACL, AccessControlList> jobAcls =
        new HashMap<JobACL, AccessControlList>();
    // 从Avro存储格式转换回Java对象格式
    for (JobACL jobACL : JobACL.values()) {
      Utf8 jobACLsUtf8 = new Utf8(jobACL.getAclName());
      if (datum.getAcls().containsKey(jobACLsUtf8)) {
        jobAcls.put(jobACL, new AccessControlList(datum.getAcls().get(
            jobACLsUtf8).toString()));
      }
    }
    return jobAcls;
  }
  /** 获取工作流ID */
  public String getWorkflowId() {
    if (datum.getWorkflowId() != null) {
      return datum.getWorkflowId().toString();
    }
    return null;
  }
  /** 获取工作流名称 */
  public String getWorkflowName() {
    if (datum.getWorkflowName() != null) {
      return datum.getWorkflowName().toString();
    }
    return null;
  }
  /** 获取工作流节点名称 */
  public String getWorkflowNodeName() {
    if (datum.getWorkflowNodeName() != null) {
      return datum.getWorkflowNodeName().toString();
    }
    return null;
  }
  /** 获取工作流依赖邻接表 */
  public String getWorkflowAdjacencies() {
    if (datum.getWorkflowAdjacencies() != null) {
      return datum.getWorkflowAdjacencies().toString();
    }
    return null;
  }
  /** 获取工作流标签 */
  public String getWorkflowTags() {
    if (datum.getWorkflowTags() != null) {
      return datum.getWorkflowTags().toString();
    }
    return null;
  }
  /** 获取事件类型 */
  public EventType getEventType() { return EventType.JOB_SUBMITTED; }

  /** 获取作业配置对象 */
  public JobConf getJobConf() {
    return jobConf;
  }

  @Override
  /** 将当前事件转换为YARN时间线服务事件格式 */
  public TimelineEvent toTimelineEvent() {
    // 创建时间线事件对象
    TimelineEvent tEvent = new TimelineEvent();
    // 设置事件ID为大写的事件类型名
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    // 添加提交时间信息
    tEvent.addInfo("SUBMIT_TIME", getSubmitTime());
    // 添加队列名称信息
    tEvent.addInfo("QUEUE_NAME", getJobQueueName());
    // 添加作业名称信息
    tEvent.addInfo("JOB_NAME", getJobName());
    // 添加用户名信息
    tEvent.addInfo("USER_NAME", getUserName());
    // 添加作业配置路径信息
    tEvent.addInfo("JOB_CONF_PATH", getJobConfPath());
    // 添加ACL信息
    tEvent.addInfo("ACLS", getJobAcls());
    // 添加作业队列名称信息
    tEvent.addInfo("JOB_QUEUE_NAME", getJobQueueName());
    // 添加工作流ID信息
    tEvent.addInfo("WORKLFOW_ID", getWorkflowId());
    // 添加工作流名称信息
    tEvent.addInfo("WORKFLOW_NAME", getWorkflowName());
    // 添加工作流节点名称信息
    tEvent.addInfo("WORKFLOW_NODE_NAME", getWorkflowNodeName());
    // 添加工作流依赖信息
    tEvent.addInfo("WORKFLOW_ADJACENCIES",
        getWorkflowAdjacencies());
    // 添加工作流标签信息
    tEvent.addInfo("WORKFLOW_TAGS", getWorkflowTags());

    return tEvent;
  }

  @Override
  /** 获取时间线指标，此事件无指标，返回null */
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }
}