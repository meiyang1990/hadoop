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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

/**
 * 兼容旧版mapred API的作业依赖调度控制器，用于管理一组存在依赖关系的MapReduce作业执行流程
 * 继承自新版mapreduce包的JobControl，提供旧版API风格的接口适配
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JobControl extends 
    org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl {

  /** 
   * 构造指定作业组的作业控制器
   * @param groupName 作业组的标识名称
   */
  public JobControl(String groupName) {
    super(groupName);
  }
  
  /**
   * 将新版ControlledJob列表转换为旧版Job列表，适配旧API调用
   * @param cjobs 新版ControlledJob列表
   * @return 转换后的旧版Job列表
   */
  static ArrayList<Job> castToJobList(List<ControlledJob> cjobs) {
    ArrayList<Job> ret = new ArrayList<Job>();
    for (ControlledJob job : cjobs) {
      ret.add((Job)job);
    }
    return ret;
  }
  
  /**
   * 获取所有处于等待状态的作业列表
   * @return 等待执行的旧版Job列表
   */
  public ArrayList<Job> getWaitingJobs() {
    return castToJobList(super.getWaitingJobList());
  }
	
  /**
   * 获取所有处于运行状态的作业列表
   * @return 正在运行的旧版Job列表
   */
  public ArrayList<Job> getRunningJobs() {
    return castToJobList(super.getRunningJobList());
  }
	
  /**
   * 获取所有处于就绪状态的作业列表（满足依赖条件等待调度）
   * @return 就绪状态的旧版Job列表
   */
  public ArrayList<Job> getReadyJobs() {
    return castToJobList(super.getReadyJobList());
  }
	
  /**
   * 获取所有执行成功的作业列表
   * @return 执行成功的旧版Job列表
   */
  public ArrayList<Job> getSuccessfulJobs() {
    return castToJobList(super.getSuccessfulJobList());
  }
	
  /**
   * 获取所有执行失败的作业列表
   * @return 执行失败的旧版Job列表
   */
  public ArrayList<Job> getFailedJobs() {
    return castToJobList(super.getFailedJobList());
  }

  /**
   * 批量添加一批作业到作业控制组
   * 
   * @param jobs 待添加的旧版Job集合
   */
  public void addJobs(Collection <Job> jobs) {
    for (Job job : jobs) {
      addJob(job);
    }
  }

  /**
   * 获取当前作业控制器的线程状态，兼容旧API的整数状态码返回
   * @return 状态码：0=运行中, 1=挂起, 2=已停止, 3=正在停止, 4=就绪, -1=未知状态
   */
  public int getState() {
    ThreadState state = super.getThreadState();
    if (state == ThreadState.RUNNING) {
      return 0;
    } 
    if (state == ThreadState.SUSPENDED) {
      return 1;
    }
    if (state == ThreadState.STOPPED) {
      return 2;
    }
    if (state == ThreadState.STOPPING) {
      return 3;
    }
    if (state == ThreadState.READY ) {
      return 4;
    }
    return -1;
  }

}