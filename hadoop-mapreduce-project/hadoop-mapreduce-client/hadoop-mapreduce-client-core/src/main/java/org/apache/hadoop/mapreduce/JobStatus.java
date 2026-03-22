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
package org.apache.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringInterner;

/**
 * 描述MapReduce作业的当前运行状态，包含作业进度、状态、时间、权限等核心信息
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class JobStatus implements Writable, Cloneable {

  // 向Hadoop Writable工厂注册当前类的构造方法，支持反序列化实例创建
  static {
    WritableFactories.setFactory
      (JobStatus.class,
       new WritableFactory() {
         public Writable newInstance() { return new JobStatus(); }
       });
  }

  /**
   * 作业运行状态枚举，定义作业所有可能的运行阶段
   */
  public enum State {
    RUNNING(1),
    SUCCEEDED(2),
    FAILED(3),
    PREP(4),
    KILLED(5);
    
    int value;
    
    State(int value) {
      this.value = value;
    }
    
    public int getValue() {
      return value; 
    }
    
  };
  
  private JobID jobid;
  private float mapProgress;
  private float reduceProgress;
  private float cleanupProgress;
  private float setupProgress;
  private State runState;
  private long startTime;
  private String user;
  private String queue;
  private JobPriority priority;
  private String schedulingInfo="NA";
  private String failureInfo = "NA";

  private Map<JobACL, AccessControlList> jobACLs =
      new HashMap<JobACL, AccessControlList>();

  private String jobName;
  private String jobFile;
  private long finishTime;
  private boolean isRetired;
  private String historyFile = "";
  private String trackingUrl ="";
  private int numUsedSlots;
  private int numReservedSlots;
  private int usedMem;
  private int reservedMem;
  private int neededMem;
  private boolean isUber;
    
  /**
   * 空构造方法，用于反序列化
   */
  public JobStatus() {
  }

  /**
   * 构造作业状态对象，包含核心作业基础信息
   * @param jobid 作业ID
   * @param setupProgress 作业setup阶段进度
   * @param mapProgress Map阶段进度
   * @param reduceProgress Reduce阶段进度
   * @param cleanupProgress 作业cleanup阶段进度
   * @param runState 作业当前状态
   * @param jp 作业优先级
   * @param user 提交作业的用户名
   * @param jobName 作业名称
   * @param jobFile 作业配置文件路径
   * @param trackingUrl 作业Web追踪地址
   */
   public JobStatus(JobID jobid, float setupProgress, float mapProgress,
                    float reduceProgress, float cleanupProgress,
                    State runState, JobPriority jp, String user, String jobName, 
                    String jobFile, String trackingUrl) {
     this(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress, 
         runState, jp, user, jobName, "default", jobFile, trackingUrl, false);
   }

   /**
    * 构造作业状态对象，增加队列参数
    * @param jobid 作业ID
    * @param setupProgress 作业setup阶段进度
    * @param mapProgress Map阶段进度
    * @param reduceProgress Reduce阶段进度
    * @param cleanupProgress 作业cleanup阶段进度
    * @param runState 作业当前状态
    * @param jp 作业优先级
    * @param user 提交作业的用户名
    * @param jobName 作业名称
    * @param queue 作业所属队列名称
    * @param jobFile 作业配置文件路径
    * @param trackingUrl 作业Web追踪地址
    */
    public JobStatus(JobID jobid, float setupProgress, float mapProgress,
                     float reduceProgress, float cleanupProgress,
                     State runState, JobPriority jp,
                     String user, String jobName, String queue,
                     String jobFile, String trackingUrl) {
      this(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress,
          runState, jp, user, jobName, queue, jobFile, trackingUrl, false);
    }

   /**
    * 构造作业状态对象，增加Uber模式参数
    * @param jobid 作业ID
    * @param setupProgress 作业setup阶段进度
    * @param mapProgress Map阶段进度
    * @param reduceProgress Reduce阶段进度
    * @param cleanupProgress 作业cleanup阶段进度
    * @param runState 作业当前状态
    * @param jp 作业优先级
    * @param user 提交作业的用户名
    * @param jobName 作业名称
    * @param queue 作业所属队列名称
    * @param jobFile 作业配置文件路径
    * @param trackingUrl 作业Web追踪地址
    * @param isUber 是否运行在Uber模式
    */
  public JobStatus(JobID jobid, float setupProgress, float mapProgress,
                    float reduceProgress, float cleanupProgress,
                    State runState, JobPriority jp,
                    String user, String jobName, String queue,
                    String jobFile, String trackingUrl, boolean isUber) {
     this(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress,
         runState, jp, user, jobName, queue, jobFile, trackingUrl, isUber, "");
  }

 /**
  * 完整构造作业状态对象，包含所有参数
  * @param jobid 作业ID
  * @param setupProgress 作业setup阶段进度
  * @param mapProgress Map阶段进度
  * @param reduceProgress Reduce阶段进度
  * @param cleanupProgress 作业cleanup阶段进度
  * @param runState 作业当前状态
  * @param jp 作业优先级
  * @param user 提交作业的用户名
  * @param jobName 作业名称
  * @param queue 作业所属队列名称
  * @param jobFile 作业配置文件路径
  * @param trackingUrl 作业Web追踪地址
  * @param isUber 是否运行在Uber模式
  * @param historyFile 作业历史文件路径
  */
  public JobStatus(JobID jobid, float setupProgress, float mapProgress,
                   float reduceProgress, float cleanupProgress,
                   State runState, JobPriority jp,
                   String user, String jobName, String queue,
                   String jobFile, String trackingUrl, boolean isUber,
                   String historyFile) {
    this.jobid = jobid;
    this.setupProgress = setupProgress;
    this.mapProgress = mapProgress;
    this.reduceProgress = reduceProgress;
    this.cleanupProgress = cleanupProgress;
    this.runState = runState;
    this.user = user;
    this.queue = queue;
    if (jp == null) {
      throw new IllegalArgumentException("Job Priority cannot be null.");
    }
    priority = jp;
    this.jobName = jobName;
    this.jobFile = jobFile;
    this.trackingUrl = trackingUrl;
    this.isUber = isUber;
    this.historyFile = historyFile;
  }


  /**
   * 设置Map阶段进度，限制进度范围在0.0~1.0之间
   * @param p 进度值
   */
  protected synchronized void setMapProgress(float p) { 
    this.mapProgress = (float) Math.min(1.0, Math.max(0.0, p)); 
  }

  /**
   * 设置cleanup阶段进度，限制进度范围在0.0~1.0之间
   * @param p 进度值
   */
  protected synchronized void setCleanupProgress(float p) { 
    this.cleanupProgress = (float) Math.min(1.0, Math.max(0.0, p)); 
  }

  /**
   * 设置setup阶段进度，限制进度范围在0.0~1.0之间
   * @param p 进度值
   */
  protected synchronized void setSetupProgress(float p) { 
    this.setupProgress = (float) Math.min(1.0, Math.max(0.0, p)); 
  }

  /**
   * 设置Reduce阶段进度，限制进度范围在0.0~1.0之间
   * @param p 进度值
   */
  protected synchronized void setReduceProgress(float p) { 
    this.reduceProgress = (float) Math.min(1.0, Math.max(0.0, p)); 
  }
    
  /**
   * 设置作业优先级
   * @param jp 新的作业优先级
   */
  protected synchronized void setPriority(JobPriority jp) {
    if (jp == null) {
      throw new IllegalArgumentException("Job priority cannot be null.");
    }
    priority = jp;
  }
  
  /**
   * 设置作业完成时间
   * @param finishTime 作业完成时间戳
   */
  protected synchronized void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  /**
   * 设置已完成作业的历史文件路径
   * @param historyFile 历史文件路径
   */
  protected synchronized void setHistoryFile(String historyFile) {
    this.historyFile = historyFile;
  }

  /**
   * 设置作业Web追踪地址
   * @param trackingUrl 追踪地址
   */
  protected synchronized void setTrackingUrl(String trackingUrl) {
    this.trackingUrl = trackingUrl;
  }

  /**
   * 将作业标记为已退役（从活跃内存中移除）
   */
  protected synchronized void setRetired() {
    this.isRetired = true;
  }

  /**
   * 修改作业当前运行状态
   * @param state 新的运行状态
   */
  protected synchronized void setState(State state) {
    this.runState = state;
  }

  /**
   * 设置作业启动时间
   * @param startTime 作业启动时间戳
   */
  protected synchronized void setStartTime(long startTime) { 
    this.startTime = startTime;
  }
    
  /**
   * 设置提交作业的用户名
   * @param userName 用户名
   */
  protected synchronized void setUsername(String userName) { 
    this.user = userName;
  }

  /**
   * 设置作业调度信息，用于记录调度器相关信息
   * @param schedulingInfo 调度信息字符串
   */
  protected synchronized void setSchedulingInfo(String schedulingInfo) {
    this.schedulingInfo = schedulingInfo;
  }

  /**
   * 设置作业访问控制列表
   * @param acls 权限类型到访问控制列表的映射
   */
  protected synchronized void setJobACLs(Map<JobACL, AccessControlList> acls) {
    this.jobACLs = acls;
  }

  /**
   * 设置作业所属队列名称
   * @param queue 队列名称
   */
  protected synchronized void setQueue(String queue) {
    this.queue = queue;
  }

  /**
   * 设置作业失败诊断信息
   * @param failureInfo 失败诊断信息
   */
  protected synchronized void setFailureInfo(String failureInfo) {
    this.failureInfo = failureInfo;
  }
  
  /**
   * 获取作业所属队列名称
   * @return 队列名称
   */
  public synchronized String getQueue() {
    return queue;
  }

  /**
   * 获取Map阶段进度百分比
   * @return 0.0~1.0之间的进度值
   */
  public synchronized float getMapProgress() { return mapProgress; }
    
  /**
   * 获取cleanup阶段进度百分比
   * @return 0.0~1.0之间的进度值
   */
  public synchronized float getCleanupProgress() { return cleanupProgress; }
    
  /**
   * 获取setup阶段进度百分比
   * @return 0.0~1.0之间的进度值
   */
  public synchronized float getSetupProgress() { return setupProgress; }
    
  /**
   * 获取Reduce阶段进度百分比
   * @return 0.0~1.0之间的进度值
   */
  public synchronized float getReduceProgress() { return reduceProgress; }
    
  /**
   * 获取作业当前运行状态
   * @return 作业状态枚举值
   */
  public synchronized State getState() { return runState; }
    
  /**
   * 获取作业启动时间戳
   * @return 启动时间戳（毫秒）
   */
  synchronized public long getStartTime() { return startTime;}

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException cnse) {
      throw new InternalError(cnse.toString());
    }
  }
  
  /**
   * 获取作业ID
   * @return 作业唯一标识
   */
  public JobID getJobID() { return jobid; }
    
  /**
   * 获取提交作业的用户名
   * @return 用户名
   */
  public synchronized String getUsername() { return this.user;}
  
  /**
   * 获取作业调度信息
   * @return 调度信息字符串
   */
  public synchronized String getSchedulingInfo() {
   return schedulingInfo;
  }

  /**
   * 获取作业访问控制列表
   * @return 权限类型到访问控制列表的映射
   */
  public synchronized Map<JobACL, AccessControlList> getJobACLs() {
    return jobACLs;
  }

  /**
   * 获取作业优先级
   * @return 作业优先级枚举值
   */
   public synchronized JobPriority getPriority() { return priority; }
  
   /**
    * 获取作业失败诊断信息
    * @return 失败原因描述
    */
   public synchronized String getFailureInfo() {
     return this.failureInfo;
   }


  /**
   * 判断作业是否已完成（成功/失败/被杀都属于完成状态）
   * @return 作业已完成返回true，否则返回false
   */
  public synchronized boolean isJobComplete() {
    return (runState == JobStatus.State.SUCCEEDED || 
            runState == JobStatus.State.FAILED || 
            runState == JobStatus.State.KILLED);
  }

  ///////////////////////////////////////
  // Writable序列化实现
  ///////////////////////////////////////
  public synchronized void write(DataOutput out) throws IOException {
    jobid.write(out);
    out.writeFloat(setupProgress);
    out.writeFloat(mapProgress);
    out.writeFloat(reduceProgress);
    out.writeFloat(cleanupProgress);
    WritableUtils.writeEnum(out, runState);
    out.writeLong(startTime);
    Text.writeString(out, user);
    WritableUtils.writeEnum(out, priority);
    Text.writeString(out, schedulingInfo);
    out.writeLong(finishTime);
    out.writeBoolean(isRetired);
    Text.writeString(out, historyFile);
    Text.writeString(out, jobName);
    Text.writeString(out, trackingUrl);
    Text.writeString(out, jobFile);
    out.writeBoolean(isUber);

    // 序列化作业访问控制列表
    out.writeInt(jobACLs.size());
    for (Entry<JobACL, AccessControlList> entry : jobACLs.entrySet()) {
      WritableUtils.writeEnum(out, entry.getKey());
      entry.getValue().write(out);
    }
  }

  public synchronized void readFields(DataInput in) throws IOException {
    this.jobid = new JobID();
    this.jobid.readFields(in);
    this.setupProgress = in.readFloat();
    this.mapProgress = in.readFloat();
    this.reduceProgress = in.readFloat();
    this.cleanupProgress = in.readFloat();
    this.runState = WritableUtils.readEnum(in, State.class);
    this.startTime = in.readLong();
    this.user = StringInterner.weakIntern(Text.readString(in));
    this.priority = WritableUtils.readEnum(in, JobPriority.class);
    this.schedulingInfo = StringInterner.weakIntern(Text.readString(in));
    this.finishTime = in.readLong();
    this.isRetired = in.readBoolean();
    this.historyFile = StringInterner.weakIntern(Text.readString(in));
    this.jobName = StringInterner.weakIntern(Text.readString(in));
    this.trackingUrl = StringInterner.weakIntern(Text.readString(in));
    this.jobFile = StringInterner