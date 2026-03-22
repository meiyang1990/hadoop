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
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapred.JvmTask;
import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;
import org.apache.hadoop.mapreduce.security.token.JobTokenSelector;
import org.apache.hadoop.security.token.TokenInfo;

/**
 * 文件说明：MapReduce旧API框架中，Task子进程与父进程TaskTracker之间的通信协议接口
 * 核心职责：定义了子任务执行过程中向父进程汇报状态、获取任务、完成交互等所有通信约定
 * 业务作用：TaskTracker作为父进程负责从ResourceManager获取任务，子进程实际执行任务，通过该协议完成双向通信
 */ 
@TokenInfo(JobTokenSelector.class)
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface TaskUmbilicalProtocol extends VersionedProtocol {

  /** 
   * 版本变更历史记录：
   * Changed the version to 2, since we have a new method getMapOutputs 
   * Changed version to 3 to have progress() return a boolean
   * Changed the version to 4, since we have replaced 
   *         TaskUmbilicalProtocol.progress(String, float, String, 
   *         org.apache.hadoop.mapred.TaskStatus.Phase, Counters) 
   *         with statusUpdate(String, TaskStatus)
   * 
   * Version 5 changed counters representation for HADOOP-2248
   * Version 6 changes the TaskStatus representation for HADOOP-2208
   * Version 7 changes the done api (via HADOOP-3140). It now expects whether
   *           or not the task's output needs to be promoted.
   * Version 8 changes {job|tip|task}id's to use their corresponding 
   * objects rather than strings.
   * Version 9 changes the counter representation for HADOOP-1915
   * Version 10 changed the TaskStatus format and added reportNextRecordRange
   *            for HADOOP-153
   * Version 11 Adds RPCs for task commit as part of HADOOP-3150
   * Version 12 getMapCompletionEvents() now also indicates if the events are 
   *            stale or not. Hence the return type is a class that 
   *            encapsulates the events and whether to reset events index.
   * Version 13 changed the getTask method signature for HADOOP-249
   * Version 14 changed the getTask method signature for HADOOP-4232
   * Version 15 Adds FAILED_UNCLEAN and KILLED_UNCLEAN states for HADOOP-4759
   * Version 16 Change in signature of getTask() for HADOOP-5488
   * Version 17 Modified TaskID to be aware of the new TaskTypes
   * Version 18 Added numRequiredSlots to TaskStatus for MAPREDUCE-516
   * Version 19 Added fatalError for child to communicate fatal errors to TT
   * Version 20 Added methods to manage checkpoints
   * Version 21 Added fastFail parameter to fatalError
   * */

  public static final long versionID = 21L;
  
  /**
   * 子任务进程启动后调用，获取分配给当前JVM的具体执行任务
   * @param context 当前JVM相对于启动它的TaskTracker的上下文信息
   * @return 需要执行的任务对象
   * @throws IOException IO异常
   */
  JvmTask getTask(JvmContext context) throws IOException;
  
  /**
   * 子任务向父进程汇报当前进度与状态，同时保持心跳，支持返回抢占相关反馈
   * @param taskId 当前子任务的尝试ID
   * @param taskStatus 子任务当前状态信息
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   * @return True如果父进程认识该任务，否则返回False
   */
  AMFeedback statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus) 
  throws IOException, InterruptedException;
  
  /**
   * 子任务向父进程上报诊断错误信息
   * @param taskid 出错的任务尝试ID
   * @param trace 需要上报的错误堆栈/描述信息
   * @throws IOException IO异常
   */
  void reportDiagnosticInfo(TaskAttemptID taskid, String trace) throws IOException;
  
  /**
   * 子任务上报即将处理的下一条记录范围
   * @param taskid 当前任务尝试ID
   * @param range 记录序号范围
   * @throws IOException IO异常
   */
  void reportNextRecordRange(TaskAttemptID taskid, SortedRanges.Range range) 
    throws IOException;

  /**
   * 子任务上报任务已成功完成，若子进程退出前未调用则视为任务失败
   * @param taskid 当前任务尝试ID
   * @throws IOException IO异常
   */
  void done(TaskAttemptID taskid) throws IOException;
  
  /** 
   * 子任务上报任务已完成，但提交仍处于挂起等待状态
   * @param taskId 当前任务尝试ID
   * @param taskStatus 子任务当前状态
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  void commitPending(TaskAttemptID taskId, TaskStatus taskStatus) 
  throws IOException, InterruptedException;  

  /**
   * 子任务轮询父进程，询问是否可以继续执行任务提交
   * @param taskid 当前任务尝试ID
   * @return true允许提交，false需要等待
   * @throws IOException IO异常
   */
  boolean canCommit(TaskAttemptID taskid) throws IOException;

  /**
   * Reduce任务上报shuffle阶段拉取map输出失败错误
   * @param taskId 当前任务尝试ID
   * @param message 错误信息
   * @throws IOException IO异常
   */
  void shuffleError(TaskAttemptID taskId, String message) throws IOException;
  
  /**
   * 子任务上报本地文件系统错误
   * @param taskId 当前任务尝试ID
   * @param message 错误信息
   * @throws IOException IO异常
   */
  void fsError(TaskAttemptID taskId, String message) throws IOException;

  /**
   * 子任务上报致命错误，通知父进程任务失败
   * @param taskId 当前任务尝试ID
   * @param message 失败信息
   * @param fastFail 是否开启快速失败标记
   * @throws IOException IO异常
   */
  void fatalError(TaskAttemptID taskId, String message, boolean fastFail) throws IOException;
  
  /**
   * Reduce任务调用，获取已完成Map任务的输出位置信息
   * @param jobId 当前作业ID
   * @param fromIndex 从哪个索引位置开始获取事件
   * @param maxLocs 最多获取多少个位置信息
   * @param id 请求获取信息的Reduce任务尝试ID
   * @return 包含Map任务完成事件的更新对象，附带是否需要重置索引的标记
   * @throws IOException IO异常
   */
  MapTaskCompletionEventsUpdate getMapCompletionEvents(JobID jobId, 
                                                       int fromIndex, 
                                                       int maxLocs,
                                                       TaskAttemptID id) 
  throws IOException;

  /**
   * 子任务向ApplicationMaster上报任务已被成功抢占
   * @param taskId 当前任务尝试ID
   * @param taskStatus 子任务当前状态
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  void preempted(TaskAttemptID taskId, TaskStatus taskStatus)
      throws IOException, InterruptedException;

  /**
   * 获取指定任务的最新检查点ID，用于任务从检查点恢复执行
   * @param taskID 任务ID
   * @return 该任务最新的检查点ID，若无则返回null
   */
  TaskCheckpointID getCheckpointID(TaskID taskID);

  /**
   * 将指定任务的检查点ID发送给ApplicationMaster存储，用于后续任务重启恢复
   * @param tid 任务ID
   * @param cid 检查点ID
   */
  void setCheckpointID(TaskID tid, TaskCheckpointID cid);

}