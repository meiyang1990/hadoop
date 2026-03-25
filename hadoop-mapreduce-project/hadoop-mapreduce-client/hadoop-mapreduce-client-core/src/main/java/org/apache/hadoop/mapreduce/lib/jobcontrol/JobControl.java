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
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob.State;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * 该类封装一组有依赖关系的MapReduce作业，负责管理和调度作业执行。
 * 它根据作业状态将其放入不同集合跟踪状态，通过后台线程自动调度就绪作业执行，并根据依赖关系更新后续作业状态。
 * 支持线程的挂起、恢复和停止操作，为客户端提供了不同状态作业的查询接口。
 * 每个添加到组的作业都会分配一个组内唯一ID。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class JobControl implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(JobControl.class);

  /** 调度线程的状态枚举 */
  public enum ThreadState {RUNNING, SUSPENDED,STOPPED, STOPPING, READY};
	
  // 调度线程当前状态
  private ThreadState runnerState;
	
  // 正在处理中的作业列表（包含等待、就绪、运行状态的作业）
  private LinkedList<ControlledJob> jobsInProgress = new LinkedList<ControlledJob>();
  // 执行成功的作业列表
  private LinkedList<ControlledJob> successfulJobs = new LinkedList<ControlledJob>();
  // 执行失败的作业列表
  private LinkedList<ControlledJob> failedJobs = new LinkedList<ControlledJob>();
	
  // 下一个作业ID的生成序号
  private long nextJobID;
  // 作业组名称
  private String groupName;
	
  /**
   * 构造一个作业组的调度控制器
   * @param groupName 作业组标识名称
   */
  public JobControl(String groupName) {
    this.nextJobID = -1;
    this.groupName = groupName;
    this.runnerState = ThreadState.READY;
  }
	
  /**
   * 将LinkedList转换为不可修改的ArrayList返回
   * @param jobs 输入的作业链表
   * @return 转换后的ArrayList
   */
  private static List<ControlledJob> toList(
                   LinkedList<ControlledJob> jobs) {
    ArrayList<ControlledJob> retv = new ArrayList<ControlledJob>();
    for (ControlledJob job : jobs) {
      retv.add(job);
    }
    return retv;
  }
	
  /**
   * 获取处于指定状态的作业列表
   * @param state 需要筛选的作业状态
   * @return 处于该状态的作业列表
   */
  synchronized private List<ControlledJob> getJobsIn(State state) {
    LinkedList<ControlledJob> l = new LinkedList<ControlledJob>();
    for(ControlledJob j: jobsInProgress) {
      if(j.getJobState() == state) {
        l.add(j);
      }
    }
    return l;
  }
  
  /**
   * 获取所有处于等待状态的作业列表
   * @return 等待状态作业列表
   */
  public List<ControlledJob> getWaitingJobList() {
    return getJobsIn(State.WAITING);
  }
	
  /**
   * 获取所有处于运行状态的作业列表
   * @return 运行状态作业列表
   */
  public List<ControlledJob> getRunningJobList() {
    return getJobsIn(State.RUNNING);
  }
	
  /**
   * 获取所有处于就绪状态的作业列表
   * @return 就绪状态作业列表
   */
  public List<ControlledJob> getReadyJobsList() {
    return getJobsIn(State.READY);
  }
	
  /**
   * 获取所有执行成功的作业列表
   * @return 成功作业列表
   */
  synchronized public List<ControlledJob> getSuccessfulJobList() {
    return toList(this.successfulJobs);
  }
	
  /**
   * 获取所有执行失败的作业列表
   * @return 失败作业列表
   */
  synchronized public List<ControlledJob> getFailedJobList() {
    return toList(this.failedJobs);
  }
	
  /**
   * 生成下一个作业唯一ID
   * @return 生成的作业ID
   */
  private String getNextJobID() {
    nextJobID += 1;
    return this.groupName + this.nextJobID;
  }

  /**
   * 添加一个受控作业到作业组
   * @param aJob 要添加的受控作业
   * @return 分配给该作业的组内唯一ID
   */
  synchronized public String addJob(ControlledJob aJob) {
    String id = this.getNextJobID();
    aJob.setJobID(id);
    aJob.setJobState(State.WAITING);
    jobsInProgress.add(aJob);
    return id;	
  }

  /**
   * 添加一个旧API作业到作业组
   * @param aJob 要添加的旧API作业
   * @return 分配给该作业的组内唯一ID
   */
  synchronized public String addJob(Job aJob) {
    return addJob((ControlledJob) aJob);
  }

  /**
   * 添加一批受控作业到作业组
   * @param jobs 要添加的作业集合
   */
  public void addJobCollection(Collection<ControlledJob> jobs) {
    for (ControlledJob job : jobs) {
      addJob(job);
    }
  }
	
  /**
   * 获取当前调度线程的状态
   * @return 调度线程状态
   */
  public ThreadState getThreadState() {
    return this.runnerState;
  }
	
  /**
   * 请求停止调度线程，线程会在下次循环时退出
   */
  public void stop() {
    this.runnerState = ThreadState.STOPPING;
  }
	
  /**
   * 挂起正在运行的调度线程
   */
  public void suspend () {
    if (this.runnerState == ThreadState.RUNNING) {
      this.runnerState = ThreadState.SUSPENDED;
    }
  }
	
  /**
   * 恢复已挂起的调度线程
   */
  public void resume () {
    if (this.runnerState == ThreadState.SUSPENDED) {
      this.runnerState = ThreadState.RUNNING;
    }
  }
	
  /**
   * 检查组内所有作业是否都已完成
   * @return true表示所有作业都已完成，false表示还有未完成作业
   */
  synchronized public boolean allFinished() {
    return jobsInProgress.isEmpty();
  }
	
  /**
   * 调度线程主循环，主要执行三个操作：
   * 1. 检查正在运行的作业状态，更新其状态
   * 2. 根据依赖完成情况更新等待作业的状态
   * 3. 提交所有就绪状态的作业
   */
  public void run() {
    // 启动前先检查作业依赖是否存在环
    if (isCircular(jobsInProgress)) {
      throw new IllegalArgumentException("job control has circular dependency");
    }
    try {
      this.runnerState = ThreadState.RUNNING;
      // 主循环
      while (true) {
        // 挂起状态等待，定期检查状态变化
        while (this.runnerState == ThreadState.SUSPENDED) {
          try {
            Thread.sleep(5000);
          }
          catch (Exception e) {
            //TODO the thread was interrupted, do something!!!
          }
        }
        
        synchronized(this) {
          // 遍历所有处理中的作业，更新状态
          Iterator<ControlledJob> it = jobsInProgress.iterator();
          while(it.hasNext()) {
            ControlledJob j = it.next();
            LOG.debug("Checking state of job "+j);
            // 根据检查后的状态更新作业分组
            switch(j.checkState()) {
            case SUCCESS:
              // 作业成功，移动到成功列表
              successfulJobs.add(j);
              it.remove();
              break;
            case FAILED:
            case DEPENDENT_FAILED:
              // 作业失败，移动到失败列表
              failedJobs.add(j);
              it.remove();
              break;
            case READY:
              // 作业就绪，提交执行
              j.submit();
              break;
            case RUNNING:
            case WAITING:
              // 无操作，保持当前状态
              break;
            }
          }
        }
        
        // 如果状态不是运行或挂起，退出循环
        if (this.runnerState != ThreadState.RUNNING && 
            this.runnerState != ThreadState.SUSPENDED) {
          break;
        }
        try {
          // 休眠5秒后再检查，避免占用过多CPU
          Thread.sleep(5000);
        }
        catch (Exception e) {
          //TODO the thread was interrupted, do something!!!
        }
        // 再次检查状态，确认是否继续循环
        if (this.runnerState != ThreadState.RUNNING && 
            this.runnerState != ThreadState.SUSPENDED) {
          break;
        }
      }
    }catch(Throwable t) {
      // 发生未捕获异常，标记所有未完成作业为失败
      LOG.error("Error while trying to run jobs.",t);
      failAllJobs(t);
    }
    // 标记线程为停止状态
    this.runnerState = ThreadState.STOPPED;
  }

  /**
   * 将所有未完成的作业标记为失败，用于发生系统异常时的清理
   * @param t 导致失败的异常
   */
  synchronized private void failAllJobs(Throwable t) {
    String message = "Unexpected System Error Occurred: "+
    StringUtils.stringifyException(t);
    Iterator<ControlledJob> it = jobsInProgress.iterator();
    while(it.hasNext()) {
      ControlledJob j = it.next();
      try {
        j.failJob(message);
      } catch (IOException e) {
        LOG.error("Error while tyring to clean up "+j.getJobName(), e);
      } catch (InterruptedException e) {
        LOG.error("Error while tyring to clean up "+j.getJobName(), e);
      } finally {
        // 无论失败与否，都移动到失败列表
        failedJobs.add(j);
        it.remove();
      }
    }
  }

 /**
   * 使用拓扑排序算法检测作业依赖是否存在环
   * @param jobList 待检测的作业列表
   * @return true存在循环依赖，false不存在
   */
  private boolean isCircular(final List<ControlledJob> jobList) {
    boolean cyclePresent = false;
    HashSet<ControlledJob> SourceSet = new HashSet<ControlledJob>();
    HashMap<ControlledJob, List<ControlledJob>> processedMap =
	new HashMap<ControlledJob, List<ControlledJob>>();
    // 初始化处理结果映射
    for (ControlledJob n : jobList) {
      processedMap.put(n, new ArrayList<ControlledJob>());
    }
    // 找出所有入度为0的节点（无依赖的作业）
    for (ControlledJob n : jobList) {
      if (!hasInComingEdge(n, jobList, processedMap)) {
	SourceSet.add(n);
      }
    }
    // Kahn拓扑排序过程
    while (!SourceSet.isEmpty()) {
      // 取出一个入度为0的节点
      ControlledJob controlledJob = SourceSet.iterator().next();
      SourceSet.remove(controlledJob);
      // 遍历该节点的所有依赖作业（当前作业完成后依赖它的作业才能执行）
      if (controlledJob.getDependentJobs() != null) {
	for (int i = 0; i < controlledJob.getDependentJobs().size(); i++) {
	  ControlledJob depenControlledJob =
	      controlledJob.getDependentJobs().get(i);
	  // 标记该边已处理
	  processedMap.get(controlledJob).add(depenControlledJob);
	  // 如果依赖作业现在入度变为0，加入源集合
	  if (!hasInComingEdge(depenControlledJob, jobList, processedMap)) {
	    SourceSet.add(depenControlledJob);
	  }
	}
      }
    }

    // 如果还有节点未处理完成，说明存在环
    for (ControlledJob controlledJob : jobList) {
      if (controlledJob.getDependentJobs() != null
	  && controlledJob.getDependentJobs().size() != processedMap.get(
	      controlledJob).size()) {
	cyclePresent = true;
	LOG.error("Job control has circular dependency for the  job "
	    + controlledJob.getJobName());
	break;
      }
    }
    return cyclePresent;
  }

  /**
   * 检查指定作业当前是否还有未处理的入边（依赖）
   * @param controlledJob 待检查的作业
   * @param controlledJobList 所有作业列表
   * @param processedMap 已处理的边映射
   * @return true还有未处理的入边，false没有未处理入边
   */
  private boolean hasInComingEdge(ControlledJob controlledJob,
      List<ControlledJob> controlledJobList,
      HashMap<ControlledJob, List<ControlledJob>> processedMap) {
    boolean hasIncomingEdge = false;
    // 遍历所有作业，检查是否还有未处理的依赖指向当前作业
    for (ControlledJob k : controlledJobList) {
      if (k != controlledJob && k.getDependentJobs() != null
	  && !processedMap.get(k).contains(controlledJob)
	  && k.getDependentJobs().contains(controlledJob)) {
	hasIncomingEdge = true;
	break;
      }
    }
    return hasIncomingEdge;
  }
}