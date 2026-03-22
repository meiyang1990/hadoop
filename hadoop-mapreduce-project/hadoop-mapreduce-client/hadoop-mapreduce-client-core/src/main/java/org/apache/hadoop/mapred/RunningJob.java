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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * 文件描述：Hadoop MapReduce 旧版API中运行中作业的查询接口，为客户端提供作业状态、进度等信息查询能力
 * 
 * <code>RunningJob</code> is the user-interface to query for details on a 
 * running Map-Reduce job.
 * 
 * <p>Clients can get hold of <code>RunningJob</code> via the {@link JobClient}
 * and then query the running-job for details such as name, configuration, 
 * progress etc.</p> 
 * 
 * @see JobClient
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface RunningJob {

  /**
   * 获取当前作业的配置对象
   *
   * @return 当前作业的配置对象
   */
  public Configuration getConfiguration();

  /**
   * 获取当前作业的唯一标识对象
   * 
   * @return 当前作业的JobID标识对象
   */
  public JobID getID();
  
  /** @deprecated This method is deprecated and will be removed. Applications should 
   * rather use {@link #getID()}.
   */
  @Deprecated
  public String getJobID();
  
  /**
   * 获取当前作业的名称
   * 
   * @return 当前作业的名称字符串
   */
  public String getJobName();

  /**
   * 获取提交作业时配置文件的路径
   * 
   * @return 提交作业配置文件的路径字符串
   */
  public String getJobFile();

  /**
   * 获取当前作业进度跟踪页面的URL
   * 
   * @return 作业进度跟踪页面的URL字符串
   */
  public String getTrackingURL();

  /**
   * 获取当前作业Map阶段的进度，范围在0.0到1.0之间，全部完成返回1.0
   * 
   * @return Map阶段进度值（0.0-1.0）
   * @throws IO异常
   */
  public float mapProgress() throws IOException;

  /**
   * 获取当前作业Reduce阶段的进度，范围在0.0到1.0之间，全部完成返回1.0
   * 
   * @return Reduce阶段进度值（0.0-1.0）
   * @throws IO异常
   */
  public float reduceProgress() throws IOException;

  /**
   * 获取当前作业Cleanup阶段的进度，范围在0.0到1.0之间，全部完成返回1.0
   * 
   * @return Cleanup阶段进度值（0.0-1.0）
   * @throws IO异常
   */
  public float cleanupProgress() throws IOException;

  /**
   * 获取当前作业Setup阶段的进度，范围在0.0到1.0之间，全部完成返回1.0
   * 
   * @return Setup阶段进度值（0.0-1.0）
   * @throws IO异常
   */
  public float setupProgress() throws IOException;

  /**
   * 检查作业是否已完成，非阻塞调用
   * 
   * @return <code>true</code> 作业已完成，否则返回<code>false</code>
   * @throws IO异常
   */
  public boolean isComplete() throws IOException;

  /**
   * 检查作业是否成功完成
   * 
   * @return <code>true</code> 作业执行成功，否则返回<code>false</code>
   * @throws IO异常
   */
  public boolean isSuccessful() throws IOException;
  
  /**
   * 阻塞等待直到作业完成
   * 
   * @throws IO异常
   */
  public void waitForCompletion() throws IOException;

  /**
   * 返回当前作业的状态枚举值，对应{@link JobStatus}中的状态定义
   * 
   * @return 当前作业状态编码
   * @throws IO异常
   */
  public int getJobState() throws IOException;
  
  /**
   * 返回当前作业状态的快照对象{@link JobStatus}，需要重新调用获取最新状态
   * 
   * @return 当前作业状态快照对象
   * @throws IO异常
   */
  public JobStatus getJobStatus() throws IOException;

  /**
   * 终止正在运行的作业，阻塞等待所有任务被杀死后返回，如果作业已停止则直接返回
   * 
   * @throws IO异常
   */
  public void killJob() throws IOException;
  
  /**
   * 修改正在运行作业的优先级
   * @param priority 作业新的优先级
   * @throws IO异常
   */
  public void setJobPriority(String priority) throws IOException;
  
  /**
   * 获取任务完成（成功/失败）事件列表，用于分页查询
   *  
   * @param startFrom 起始查询下标
   * @return 任务完成事件数组 {@link TaskCompletionEvent}
   * @throws IO异常
   */
  public TaskCompletionEvent[] getTaskCompletionEvents(int startFrom) 
  throws IOException;
  
  /**
   * 终止指定的任务尝试
   * 
   * @param taskId 待终止任务尝试的ID
   * @param shouldFail 如果为true，将该任务标记为失败并计入失败任务列表；否则仅杀死任务，不影响作业整体失败状态
   * @throws IO异常
   */
  public void killTask(TaskAttemptID taskId, boolean shouldFail) throws IOException;
  
  /** @deprecated Applications should rather use {@link #killTask(TaskAttemptID, boolean)}*/
  @Deprecated
  public void killTask(String taskId, boolean shouldFail) throws IOException;
  
  /**
   * 获取当前作业的所有统计计数器
   * 
   * @return 作业计数器对象，如果作业已退役则返回null
   * @throws IO异常
   */
  public Counters getCounters() throws IOException;
  
  /**
   * 获取指定任务尝试的诊断信息
   * @param taskid 任务尝试ID
   * @return 任务诊断信息字符串数组
   * @throws IO异常
   */
  public String[] getTaskDiagnostics(TaskAttemptID taskid) throws IOException;

  /**
   * 获取作业历史文件归档的URL，历史文件尚未生成则返回空字符串
   * 
   * @return 作业历史文件归档URL
   * @throws IO异常
   */
  public String getHistoryUrl() throws IOException;

  /**
   * 检查作业是否已从JobTracker内存中移除并退役，退役后作业历史文件会被拷贝到getHistoryUrl()指定位置
   * @return <code>true</code> 作业已退役，否则返回<code>false</code>
   * @throws IO异常
   */
  public boolean isRetired() throws IOException;
  
  /**
   * 获取作业失败信息
   * @return 作业失败原因描述，如果未失败则返回空
   * @throws IO异常
   */
  public String getFailureInfo() throws IOException;
}