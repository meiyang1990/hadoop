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
package org.apache.hadoop.mapred;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.security.authorize.AccessControlList;

/**************************************************
 * Describes the current status of a job.  This is
 * not intended to be a comprehensive piece of data.
 * For that, look at JobProfile.
 *************************************************
 **/
/**
 * 描述MapReduce作业当前运行状态的状态类，为兼容旧版MapReduce API提供
 * 继承自新版mapreduce.JobStatus，适配旧API的状态表示方式
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JobStatus extends org.apache.hadoop.mapreduce.JobStatus {

  // 以下为兼容旧API的作业状态常量，从新版API状态枚举取值
  public static final int RUNNING = 
    org.apache.hadoop.mapreduce.JobStatus.State.RUNNING.getValue();
  public static final int SUCCEEDED = 
    org.apache.hadoop.mapreduce.JobStatus.State.SUCCEEDED.getValue();
  public static final int FAILED = 
    org.apache.hadoop.mapreduce.JobStatus.State.FAILED.getValue();
  public static final int PREP = 
    org.apache.hadoop.mapreduce.JobStatus.State.PREP.getValue();
  public static final int KILLED = 
    org.apache.hadoop.mapreduce.JobStatus.State.KILLED.getValue();

  private static final String UNKNOWN = "UNKNOWN";
  
  // 状态码到可读状态名称的映射数组，索引对应状态值
  private static final String[] runStates =
    {UNKNOWN, "RUNNING", "SUCCEEDED", "FAILED", "PREP", "KILLED"};

  /**
   * Helper method to get human-readable state of the job.
   * @param state job state
   * @return human-readable state of the job
   */
  /**
   * 将整数状态码转换为可读的作业状态字符串
   * @param state 整数类型作业状态码
   * @return 人类可读的状态名称
   */
  public static String getJobRunState(int state) {
    if (state < 1 || state >= runStates.length) {
      return UNKNOWN;
    }
    return runStates[state];
  }
  
  /**
   * 将旧版整数状态转换为新版API的状态枚举
   * @param state 旧版整数状态码
   * @return 对应新版状态枚举，非法状态返回null
   */
  static org.apache.hadoop.mapreduce.JobStatus.State getEnum(int state) {
    switch (state) {
      case 1: return org.apache.hadoop.mapreduce.JobStatus.State.RUNNING;
      case 2: return org.apache.hadoop.mapreduce.JobStatus.State.SUCCEEDED;
      case 3: return org.apache.hadoop.mapreduce.JobStatus.State.FAILED;
      case 4: return org.apache.hadoop.mapreduce.JobStatus.State.PREP;
      case 5: return org.apache.hadoop.mapreduce.JobStatus.State.KILLED;
    }
    return null;
  }
  
  /**
   */
  /**
   * 空构造函数
   */
  public JobStatus() {
  }
  
  /**
   * @deprecated 已废弃，兼容旧版本代码使用
   */
  @Deprecated
  public JobStatus(JobID jobid, float mapProgress, float reduceProgress,
      float cleanupProgress, int runState) {
    this(jobid, mapProgress, reduceProgress, cleanupProgress, runState, null,
        null, null, null);
  }

  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param runState The current state of the job
   */
  @Deprecated
  public JobStatus(JobID jobid, float mapProgress, float reduceProgress,
      int runState) {
    this (jobid, mapProgress, reduceProgress, runState, null, null, null, null);
  }

  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param runState The current state of the job
   * @param jp Priority of the job.
   */
  @Deprecated
  public JobStatus(JobID jobid, float mapProgress, float reduceProgress,
      float cleanupProgress, int runState, JobPriority jp) {
    this(jobid, mapProgress, reduceProgress, cleanupProgress, runState, jp,
        null, null, null, null);
  }

  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param setupProgress The progress made on the setup
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param cleanupProgress The progress made on the cleanup
   * @param runState The current state of the job
   * @param jp Priority of the job.
   */
  @Deprecated
  public JobStatus(JobID jobid, float setupProgress, float mapProgress,
      float reduceProgress, float cleanupProgress, 
      int runState, JobPriority jp) {
    this(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress,
        runState, jp, null, null, null, null);
  }

  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param cleanupProgress The progress made on cleanup
   * @param runState The current state of the job
   * @param user userid of the person who submitted the job.
   * @param jobName user-specified job name.
   * @param jobFile job configuration file. 
   * @param trackingUrl link to the web-ui for details of the job.
   */
  /**
   * 构造作业状态对象，使用默认优先级
   * @param jobid 作业ID
   * @param mapProgress Map阶段进度
   * @param reduceProgress Reduce阶段进度
   * @param cleanupProgress 清理阶段进度
   * @param runState 作业当前运行状态
   * @param user 提交作业的用户名
   * @param jobName 作业名称
   * @param jobFile 作业配置文件路径
   * @param trackingUrl 作业Web跟踪地址
   */
  public JobStatus(JobID jobid, float mapProgress, float reduceProgress,
                   float cleanupProgress, int runState, 
                   String user, String jobName, 
                   String jobFile, String trackingUrl) {
    this(jobid, mapProgress, reduceProgress, cleanupProgress, runState,
        JobPriority.DEFAULT, user, jobName, jobFile, trackingUrl);
  }

  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param runState The current state of the job
   * @param user userid of the person who submitted the job.
   * @param jobName user-specified job name.
   * @param jobFile job configuration file. 
   * @param trackingUrl link to the web-ui for details of the job.
   */
  /**
   * 构造作业状态对象，清理进度默认为0，使用默认优先级
   * @param jobid 作业ID
   * @param mapProgress Map阶段进度
   * @param reduceProgress Reduce阶段进度
   * @param runState 作业当前运行状态
   * @param user 提交作业的用户名
   * @param jobName 作业名称
   * @param jobFile 作业配置文件路径
   * @param trackingUrl 作业Web跟踪地址
   */
  public JobStatus(JobID jobid, float mapProgress, float reduceProgress,
                   int runState, String user, String jobName, 
                   String jobFile, String trackingUrl) {
    this(jobid, mapProgress, reduceProgress, 0.0f, runState, user, jobName, 
        jobFile, trackingUrl);
  }

  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param cleanupProgress The progress made on cleanup
   * @param runState The current state of the job
   * @param jp Priority of the job.
   * @param user userid of the person who submitted the job.
   * @param jobName user-specified job name.
   * @param jobFile job configuration file. 
   * @param trackingUrl link to the web-ui for details of the job.
   */
  /**
   * 构造作业状态对象，初始化阶段进度默认为0
   * @param jobid 作业ID
   * @param mapProgress Map阶段进度
   * @param reduceProgress Reduce阶段进度
   * @param cleanupProgress 清理阶段进度
   * @param runState 作业当前运行状态
   * @param jp 作业优先级
   * @param user 提交作业的用户名
   * @param jobName 作业名称
   * @param jobFile 作业配置文件路径
   * @param trackingUrl 作业Web跟踪地址
   */
   public JobStatus(JobID jobid, float mapProgress, float reduceProgress,
                      float cleanupProgress, int runState, JobPriority jp, 
                      String user, String jobName, String jobFile, 
                      String trackingUrl) {
     this(jobid, 0.0f, mapProgress, reduceProgress, 
          cleanupProgress, runState, jp, user, jobName, jobFile,
          trackingUrl);
   }
   
  /**
   * Create a job status object for a given jobid.
   * @param jobid The jobid of the job
   * @param setupProgress The progress made on the setup
   * @param mapProgress The progress made on the maps
   * @param reduceProgress The progress made on the reduces
   * @param cleanupProgress The progress made on the cleanup
   * @param runState The current state of the job
   * @param jp Priority of the job.
   * @param user userid of the person who submitted the job.
   * @param jobName user-specified job name.
   * @param jobFile job configuration file.
   * @param trackingUrl link to the web-ui for details of the job.
   */
   /**
    * 构造作业状态对象，默认队列名为default，关闭uber模式
    * @param jobid 作业ID
    * @param setupProgress 初始化阶段进度
    * @param mapProgress Map阶段进度
    * @param reduceProgress Reduce阶段进度
    * @param cleanupProgress 清理阶段进度
    * @param runState 作业当前运行状态
    * @param jp 作业优先级
    * @param user 提交作业的用户名
    * @param jobName 作业名称
    * @param jobFile 作业配置文件路径
    * @param trackingUrl 作业Web跟踪地址
    */
   public JobStatus(JobID jobid, float setupProgress, float mapProgress,
                    float reduceProgress, float cleanupProgress,
                    int runState, JobPriority jp, String user, String jobName,
                    String jobFile, String trackingUrl) {
     this(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress,
         runState, jp, user, jobName, "default", jobFile, trackingUrl);
   }

   /**
    * Create a job status object for a given jobid.
    * @param jobid The jobid of the job
    * @param setupProgress The progress made on the setup
    * @param mapProgress The progress made on the maps
    * @param reduceProgress The progress made on the reduces
    * @param cleanupProgress The progress made on the cleanup
    * @param runState The current state of the job
    * @param jp Priority of the job.
    * @param user userid of the person who submitted the job.
    * @param jobName user-specified job name.
    * @param jobFile job configuration file.
    * @param trackingUrl link to the web-ui for details of the job.
    * @param isUber Whether job running in uber mode
    */
    /**
     * 构造作业状态对象，指定uber模式，空历史文件路径
     * @param jobid 作业ID
     * @param setupProgress 初始化阶段进度
     * @param mapProgress Map阶段进度
     * @param reduceProgress Reduce阶段进度
     * @param cleanupProgress 清理阶段进度
     * @param runState 作业当前运行状态
     * @param jp 作业优先级
     * @param user 提交作业的用户名
     * @param jobName 作业名称
     * @param jobFile 作业配置文件路径
     * @param trackingUrl 作业Web跟踪地址
     * @param isUber 是否运行在uber模式
     */
    public JobStatus(JobID jobid, float setupProgress, float mapProgress,
                     float reduceProgress, float cleanupProgress,
                     int runState, JobPriority jp, String user, String jobName,
                     String jobFile, String trackingUrl, boolean isUber) {
      this(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress,
          runState, jp, user, jobName, "default", jobFile, trackingUrl, isUber);
  }

   /**
    * Create a job status object for a given jobid.
    * @param jobid The jobid of the job
    * @param setupProgress The progress made on the setup
    * @param mapProgress The progress made on the maps
    * @param reduceProgress The progress made on the reduces
    * @param cleanupProgress The progress made on the cleanup
    * @param runState The current state of the job
    * @param jp Priority of the job.
    * @param user userid of the person who submitted the job.
    * @param jobName user-specified job name.
    * @param jobFile job configuration file.
    * @param trackingUrl link to the web-ui for details of the job.
    * @param isUber Whether job running in uber mode
    * @param historyFile history file
    */
  /**
   * 构造作业状态对象，指定历史文件路径，默认队列名为default
   * @param jobid 作业ID
   * @param setupProgress 初始化阶段进度
   * @param mapProgress Map阶段进度
   * @param reduceProgress Reduce阶段进度
   * @param cleanupProgress 清理阶段进度
   * @param runState 作业当前运行状态
   * @param jp 作业优先级
   * @param user 提交作业的用户名
   * @param jobName 作业名称
   * @param jobFile 作业配置文件路径
   * @param trackingUrl 作业Web跟踪地址
   * @param isUber 是否运行在uber模式
   * @param historyFile 作业历史文件路径
   */
  public JobStatus(JobID jobid, float setupProgress, float mapProgress,
                   float reduceProgress, float cleanupProgress,
                   int runState, JobPriority jp, String user, String jobName,
                   String jobFile, String trackingUrl, boolean isUber,
                   String historyFile) {
      this(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress,
          runState, jp, user, jobName, "default", jobFile, trackingUrl, isUber,
          historyFile);
  }
   
   /**
    * Create a job status object for a given jobid.
    * @param jobid The jobid of the job
    * @param setupProgress The progress made on the setup
    * @param mapProgress The progress made on the maps
    * @param reduceProgress The progress made on the reduces
    * @param cleanupProgress The progress made on the cleanup
    * @param runState The current state of the job
    * @param jp Priority of the job.
    * @param user userid of the person who submitted the job.
    * @param jobName user-specified job name.
    * @param queue job queue name.
    * @param jobFile job configuration file.
    * @param trackingUrl link to the web-ui for details of the job.
    */
   /**
    * 构造作业状态对象，指定队列，关闭uber模式，空历史文件
    * @param jobid 作业ID
    * @param setupProgress 初始化阶段进度
    *