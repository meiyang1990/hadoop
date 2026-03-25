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

package org.apache.hadoop.mapreduce.lib.jobcontrol;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * 该类封装了一个带有依赖关系的MapReduce作业，负责根据依赖作业的状态更新自身状态。
 * 作业初始处于WAITING状态：如果没有依赖作业或所有依赖作业都执行成功，作业变为READY状态；
 * 如果任意依赖作业执行失败，当前作业也会标记为DEPENDENT_FAILED失败。
 * 处于READY状态的作业可以被提交到Hadoop执行，状态变为RUNNING，执行完成后根据结果变为SUCCESS或FAILED。
 * 用于构建有依赖关系的DAG作业流，由JobControl统一调度执行。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ControlledJob {
  private static final Logger LOG =
      LoggerFactory.getLogger(ControlledJob.class);

  // 作业状态枚举，定义了作业在生命周期中的可能状态
  public enum State {SUCCESS, WAITING, RUNNING, READY, FAILED,
                            DEPENDENT_FAILED}; 
  public static final String CREATE_DIR = "mapreduce.jobcontrol.createdir.ifnotexist";
  private State state;
  private String controlID;     // 由JobControl分配的管控ID，用于作业调度管理
  private Job job;               // 实际要执行的MapReduce作业对象
  // 供人阅读的状态信息，例如作业失败原因等
  private String message;
  // 当前作业依赖的所有前置作业列表
  private List<ControlledJob> dependingJobs;
	
  /** 
   * 构造带依赖关系的受控作业
   * @param job 待执行的MapReduce作业
   * @param dependingJobs 当前作业依赖的前置作业列表
   */
  public ControlledJob(Job job, List<ControlledJob> dependingJobs) 
      throws IOException {
    this.job = job;
    this.dependingJobs = dependingJobs;
    this.state = State.WAITING;
    this.controlID = "unassigned";
    this.message = "just initialized";
  }
  
  /**
   * 构造一个无依赖的受控作业
   * 
   * @param conf 待执行作业的配置对象
   * @throws IOException
   */
  public ControlledJob(Configuration conf) throws IOException {
    this(Job.getInstance(conf), null);
  }
	
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("job name:\t").append(this.job.getJobName()).append("\n");
    sb.append("job id:\t").append(this.controlID).append("\n");
    sb.append("job state:\t").append(this.state).append("\n");
    sb.append("job mapred id:\t").append(this.job.getJobID()).append("\n");
    sb.append("job message:\t").append(this.message).append("\n");
		
    if (this.dependingJobs == null || this.dependingJobs.size() == 0) {
      sb.append("job has no depending job:\t").append("\n");
    } else {
      sb.append("job has ").append(this.dependingJobs.size()).
         append(" dependeng jobs:\n");
      for (int i = 0; i < this.dependingJobs.size(); i++) {
        sb.append("\t depending job ").append(i).append(":\t");
        sb.append((this.dependingJobs.get(i)).getJobName()).append("\n");
      }
    }
    return sb.toString();
  }
	
  /**
   * 获取当前作业的名称
   * @return 作业名称
   */
  public String getJobName() {
    return job.getJobName();
  }
	
  /**
   * 设置当前作业的名称
   * @param jobName 作业名称
   */
  public void setJobName(String jobName) {
    job.setJobName(jobName);
  }
	
  /**
   * 获取JobControl分配给该作业的管控ID
   * @return 作业管控ID
   */
  public String getJobID() {
    return this.controlID;
  }
	
  /**
   * 设置该作业的管控ID
   * @param id 管控ID
   */
  public void setJobID(String id) {
    this.controlID = id;
  }

  /**
   * 获取MapReduce框架分配给该作业的正式JobID
   * @return MapReduce作业ID
   */
  public synchronized JobID getMapredJobId() {
    return this.job.getJobID();
  }
  
  /**
   * 获取封装的底层MapReduce作业对象
   * @return 底层MapReduce作业对象
   */
  public synchronized Job getJob() {
    return this.job;
  }

  /**
   * 设置封装的底层MapReduce作业对象
   * @param job 底层MapReduce作业对象
   */
  public synchronized void setJob(Job job) {
    this.job = job;
  }

  /**
   * 获取当前作业的状态
   * @return 当前作业状态枚举值
   */
  public synchronized State getJobState() {
    return this.state;
  }
	
  /**
   * 设置当前作业的状态
   * @param state 新的作业状态
   */
  protected synchronized void setJobState(State state) {
    this.state = state;
  }
	
  /**
   * 获取当前作业的状态信息（如失败原因）
   * @return 状态信息字符串
   */
  public synchronized String getMessage() {
    return this.message;
  }

  /**
   * 设置当前作业的状态信息
   * @param message 状态信息字符串
   */
  public synchronized void setMessage(String message) {
    this.message = message;
  }

  /**
   * 获取当前作业所有依赖的前置作业列表
   * @return 依赖前置作业列表
   */
  public List<ControlledJob> getDependentJobs() {
    return this.dependingJobs;
  }
  
  /**
   * 向当前作业的依赖列表添加一个前置作业，仅允许在WAITING状态添加
   * 
   * @param dependingJob 要添加的前置依赖作业
   * @return 添加成功返回true，当前作业不在WAITING状态返回false
   */
  public synchronized boolean addDependingJob(ControlledJob dependingJob) {
    // 仅允许在WAITING状态添加依赖
    if (this.state == State.WAITING) {
      if (this.dependingJobs == null) {
        this.dependingJobs = new ArrayList<ControlledJob>();
      }
      return this.dependingJobs.add(dependingJob);
    } else {
      return false;
    }
  }
	
  /**
   * 判断当前作业是否已完成（包括成功、失败、依赖失败三种完成状态）
   * @return 已完成返回true，否则返回false
   */
  public synchronized boolean isCompleted() {
    return this.state == State.FAILED || 
      this.state == State.DEPENDENT_FAILED ||
      this.state == State.SUCCESS;
  }
	
  /**
   * 判断当前作业是否已就绪可以执行
   * @return 处于READY状态返回true，否则返回false
   */
  public synchronized boolean isReady() {
    return this.state == State.READY;
  }

  /**
   * 杀死当前正在执行的作业
   * @throws IOException
   * @throws InterruptedException
   */
  public void killJob() throws IOException, InterruptedException {
    job.killJob();
  }
  
  /**
   * 将当前作业标记为失败，若作业正在运行则先杀死作业
   * @param message 失败原因信息
   * @throws IOException
   * @throws InterruptedException
   */
  public synchronized void failJob(String message) throws IOException, InterruptedException {
    try {
      if(job != null && this.state == State.RUNNING) {
        job.killJob();
      }
    } finally {
      this.state = State.FAILED;
      this.message = message;
    }
  }
  
  /**
   * 检查处于RUNNING状态作业的执行状态，更新为SUCCESS或FAILED
   */
  private void checkRunningState() throws IOException, InterruptedException {
    try {
      if (job.isComplete()) {
        if (job.isSuccessful()) {
          this.state = State.SUCCESS;
        } else {
          this.state = State.FAILED;
          this.message = "Job failed!";
        }
      }
    } catch (IOException ioe) {
      // 检查过程发生IO异常，将作业标记为失败
      this.state = State.FAILED;
      this.message = StringUtils.stringifyException(ioe);
      try {
        if (job != null) {
          job.killJob();
        }
      } catch (IOException e) {}
    }
  }
	
  /**
   * 检查并更新当前作业的状态，根据当前状态和依赖作业状态更新自身状态
   * @return 更新后的作业状态
   */
   synchronized State checkState() throws IOException, InterruptedException {
    if (this.state == State.RUNNING) {
      // 正在运行的作业检查运行状态
      checkRunningState();
    }
    if (this.state != State.WAITING) {
      // 非WAITING状态直接返回当前状态
      return this.state;
    }
    if (this.dependingJobs == null || this.dependingJobs.size() == 0) {
      // 无依赖作业，直接标记为就绪
      this.state = State.READY;
      return this.state;
    }
    ControlledJob pred = null;
    int n = this.dependingJobs.size();
    // 遍历所有依赖作业检查状态
    for (int i = 0; i < n; i++) {
      pred = this.dependingJobs.get(i);
      // 递归检查依赖作业的状态
      State s = pred.checkState();
      if (s == State.WAITING || s == State.READY || s == State.RUNNING) {
        // 存在未完成的依赖，保持WAITING状态退出循环
        break;
      }
      if (s == State.FAILED || s == State.DEPENDENT_FAILED) {
        // 存在失败的依赖，标记自身为依赖失败
        this.state = State.DEPENDENT_FAILED;
        this.message = "depending job " + i + " with jobID "
          + pred.getJobID() + " failed. " + pred.getMessage();
        break;
      }
      // 所有依赖都执行成功，标记自身为就绪
      if (i == n - 1) {
        this.state = State.READY;
      }
    }

    return this.state;
  }
	
  /**
   * 提交当前作业到MapReduce执行，提交成功状态变为RUNNING，失败变为FAILED
   */
  protected synchronized void submit() {
    try {
      Configuration conf = job.getConfiguration();
      // 如果配置了自动创建输入目录，检查并创建不存在的输入目录
      if (conf.getBoolean(CREATE_DIR, false)) {
        FileSystem fs = FileSystem.get(conf);
        Path inputPaths[] = FileInputFormat.getInputPaths(job);
        for (int i = 0; i < inputPaths.length; i++) {
          if (!fs.exists(inputPaths[i])) {
            try {
              fs.mkdirs(inputPaths[i]);
            } catch (IOException e) {

            }
          }
        }
      }
      // 提交作业
      job.submit();
      this.state = State.RUNNING;
    } catch (Exception ioe) {
      // 提交过程发生异常，标记为失败
      LOG.info(getJobName()+" got an error while submitting ",ioe);
      this.state = State.FAILED;
      this.message = StringUtils.stringifyException(ioe);
    }
  }
	
}