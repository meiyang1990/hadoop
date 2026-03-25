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

package org.apache.hadoop.mapred.jobcontrol;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 兼容旧版mapred API的受控作业类，继承自新版mapreduce包下的ControlledJob，用于作业依赖调度场景
 * 核心职责是封装旧版API的Job配置、ID和状态，支持JobControl对有依赖关系的作业进行顺序调度
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Job extends ControlledJob {
  static final Logger LOG = LoggerFactory.getLogger(Job.class);

  final public static int SUCCESS = 0;
  final public static int WAITING = 1;
  final public static int RUNNING = 2;
  final public static int READY = 3;
  final public static int FAILED = 4;
  final public static int DEPENDENT_FAILED = 5;

  /** 
   * 构造带依赖作业列表的受控作业对象
   * @param jobConf 待执行作业的旧版mapred作业配置
   * @param dependingJobs 当前作业依赖的前置作业列表
   */
  @SuppressWarnings("unchecked")
  public Job(JobConf jobConf, ArrayList<?> dependingJobs) throws IOException {
    super(org.apache.hadoop.mapreduce.Job.getInstance(jobConf),
          (List<ControlledJob>) dependingJobs);
  }

  /**
   * 构造无依赖作业的受控作业对象
   * @param conf 待执行作业的旧版mapred作业配置
   */
  public Job(JobConf conf) throws IOException {
    super(conf);
  }

  /**
   * 获取框架分配给本作业的旧版mapred作业ID
   * @return 旧版JobID对象，如果未分配返回null
   */
  public JobID getAssignedJobID() {
    org.apache.hadoop.mapreduce.JobID temp = super.getMapredJobId();
    if (temp == null) {
      return null;
    }
    return JobID.downgrade(temp);
  }

  /**
   * @deprecated 作业ID由框架分配，不应该手动调用该方法设置
   */
  @Deprecated
  public void setAssignedJobID(JobID mapredJobID) {
    // do nothing
  }

  /**
   * 获取本作业的旧版mapred作业配置
   * @return 拷贝后的JobConf对象
   */
  public synchronized JobConf getJobConf() {
    return new JobConf(super.getJob().getConfiguration());
  }


  /**
   * 设置本作业的旧版mapred作业配置
   * @param jobConf 要设置的旧版作业配置对象
   */
  public synchronized void setJobConf(JobConf jobConf) {
    try {
      super.setJob(org.apache.hadoop.mapreduce.Job.getInstance(jobConf));
    } catch (IOException ioe) { 
      // 捕获并记录配置转换过程中的IO异常
      LOG.info("Exception" + ioe);
    }
  }

  /**
   * 获取当前作业的运行状态，兼容旧版API的状态编码
   * @return 旧版状态编码：0成功/1等待/2运行中/3就绪/4失败/5依赖失败
   */
  public synchronized int getState() {
    State state = super.getJobState();
    if (state == State.SUCCESS) {
      return SUCCESS;
    } 
    if (state == State.WAITING) {
      return WAITING;
    }
    if (state == State.RUNNING) {
      return RUNNING;
    }
    if (state == State.READY) {
      return READY;
    }
    if (state == State.FAILED ) {
      return FAILED;
    }
    if (state == State.DEPENDENT_FAILED ) {
      return DEPENDENT_FAILED;
    }
    return -1;
  }
  
  /**
   * @deprecated 从1.x版本后行为变更，不再允许手动修改作业状态，本方法为空操作
   * @param state 要设置的新状态
   */
  @Deprecated
  protected synchronized void setState(int state) {
    // No-Op, we dont want to change the sate
  }
  
  /**
   * 向当前作业的依赖列表添加前置作业，仅在作业等待运行时可添加
   * @param dependingJob 当前作业依赖的前置作业
   * @return 添加成功返回true
   */
  public synchronized boolean addDependingJob(Job dependingJob) {
    return super.addDependingJob(dependingJob);
  }
  
  /**
   * 获取本作业对应的JobClient客户端实例
   * @return 基于当前作业配置创建的JobClient，创建失败返回null
   */
  public JobClient getJobClient() {
    try {
      return new JobClient(super.getJob().getConfiguration());
    } catch (IOException ioe) {
      return null;
    }
  }

  /**
   * 获取当前作业所有依赖的前置作业列表
   * @return 依赖作业列表
   */
  public ArrayList<Job> getDependingJobs() {
    return JobControl.castToJobList(super.getDependentJobs());
  }

  /**
   * 获取框架分配给本作业的旧版mapred作业ID字符串
   * @return 作业ID字符串，未分配返回null
   */
  public synchronized String getMapredJobID() {
    if (super.getMapredJobId() != null) {
      return super.getMapredJobId().toString();
    }
    return null;
  }

  /**
   * @deprecated 从1.x版本后行为变更，不再允许手动修改作业ID，本方法为空操作
   * @param mapredJobID 要设置的作业ID字符串
   */
  @Deprecated
  public synchronized void setMapredJobID(String mapredJobID) {
    setAssignedJobID(JobID.forName(mapredJobID));
  }
}