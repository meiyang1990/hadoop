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

package org.apache.hadoop.mapreduce.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;

/** 
 * MapReduce客户端与JobTracker服务端通信的RPC协议接口。
 * 客户端通过该协议提供的方法提交作业、查询集群和作业状态、管理作业生命周期。
 */ 
@KerberosInfo(
    serverPrincipal = JTConfig.JT_USER_NAME)
@TokenInfo(DelegationTokenSelector.class)
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface ClientProtocol extends VersionedProtocol {
  /* 
   *Changing the versionID to 2L since the getTaskCompletionEvents method has
   *changed.
   *Changed to 4 since killTask(String,boolean) is added
   *Version 4: added jobtracker state to ClusterStatus
   *Version 5: max_tasks in ClusterStatus is replaced by
   * max_map_tasks and max_reduce_tasks for HADOOP-1274
   * Version 6: change the counters representation for HADOOP-2248
   * Version 7: added getAllJobs for HADOOP-2487
   * Version 8: change {job|task}id's to use corresponding objects rather that strings.
   * Version 9: change the counter representation for HADOOP-1915
   * Version 10: added getSystemDir for HADOOP-3135
   * Version 11: changed JobProfile to include the queue name for HADOOP-3698
   * Version 12: Added getCleanupTaskReports and 
   *             cleanupProgress to JobStatus as part of HADOOP-3150
   * Version 13: Added getJobQueueInfos and getJobQueueInfo(queue name)
   *             and getAllJobs(queue) as a part of HADOOP-3930
   * Version 14: Added setPriority for HADOOP-4124
   * Version 15: Added KILLED status to JobStatus as part of HADOOP-3924            
   * Version 16: Added getSetupTaskReports and 
   *             setupProgress to JobStatus as part of HADOOP-4261           
   * Version 17: getClusterStatus returns the amount of memory used by 
   *             the server. HADOOP-4435
   * Version 18: Added blacklisted trackers to the ClusterStatus 
   *             for HADOOP-4305
   * Version 19: Modified TaskReport to have TIP status and modified the
   *             method getClusterStatus() to take a boolean argument
   *             for HADOOP-4807
   * Version 20: Modified ClusterStatus to have the tasktracker expiry
   *             interval for HADOOP-4939
   * Version 21: Modified TaskID to be aware of the new TaskTypes                                 
   * Version 22: Added method getQueueAclsForCurrentUser to get queue acls info
   *             for a user
   * Version 23: Modified the JobQueueInfo class to inlucde queue state.
   *             Part of HADOOP-5913.  
   * Version 24: Modified ClusterStatus to include BlackListInfo class which 
   *             encapsulates reasons and report for blacklisted node.          
   * Version 25: Added fields to JobStatus for HADOOP-817.   
   * Version 26: Added properties to JobQueueInfo as part of MAPREDUCE-861.
   *              added new api's getRootQueues and
   *              getChildQueues(String queueName)
   * Version 27: Changed protocol to use new api objects. And the protocol is 
   *             renamed from JobSubmissionProtocol to ClientProtocol.
   * Version 28: Added getJobHistoryDir() as part of MAPREDUCE-975.
   * Version 29: Added reservedSlots, runningTasks and totalJobSubmissions
   *             to ClusterMetrics as part of MAPREDUCE-1048.
   * Version 30: Job submission files are uploaded to a staging area under
   *             user home dir. JobTracker reads the required files from the
   *             staging area using user credentials passed via the rpc.
   * Version 31: Added TokenStorage to submitJob      
   * Version 32: Added delegation tokens (add, renew, cancel)
   * Version 33: Added JobACLs to JobStatus as part of MAPREDUCE-1307
   * Version 34: Modified submitJob to use Credentials instead of TokenStorage.
   * Version 35: Added the method getQueueAdmins(queueName) as part of
   *             MAPREDUCE-1664.
   * Version 36: Added the method getJobTrackerStatus() as part of
   *             MAPREDUCE-2337.
   * Version 37: More efficient serialization format for framework counters
   *             (MAPREDUCE-901)
   * Version 38: Added getLogFilePath(JobID, TaskAttemptID) as part of 
   *             MAPREDUCE-3146
   */
  /** 协议版本号，用于RPC兼容性检查 */
  public static final long versionID = 37L;

  /**
   * 申请生成一个唯一的作业ID
   * @return 用于提交作业的唯一作业ID
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public JobID getNewJobID() throws IOException, InterruptedException;

  /**
   * 向JobTracker提交作业执行
   * @param jobId 作业ID
   * @param jobSubmitDir 作业提交目录，存储作业配置和资源
   * @param ts 用户凭证信息，用于访问Staging区域
   * @return 提交后作业的最新状态
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public JobStatus submitJob(JobID jobId, String jobSubmitDir, Credentials ts)
      throws IOException, InterruptedException;

  /**
   * 获取当前集群的指标信息
   * 
   * @return 集群状态汇总指标
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public ClusterMetrics getClusterMetrics() 
  throws IOException, InterruptedException;

  /**
   * 获取JobTracker服务的当前状态
   * 
   * @return JobTracker的状态对象
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public JobTrackerStatus getJobTrackerStatus() throws IOException,
    InterruptedException;

  /**
   * 获取TaskTracker超时时间间隔，超过该时间未心跳则判定节点下线
   * @return TaskTracker超时时间间隔（毫秒）
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public long getTaskTrackerExpiryInterval() throws IOException,
                                               InterruptedException;
  
  /**
   * 获取指定作业队列的管理员ACL，仅Hadoop内部使用
   * @param queueName 队列名称
   * @return 队列管理员访问控制列表
   * @throws IOException IO异常
   */
  public AccessControlList getQueueAdmins(String queueName) throws IOException;

  /**
   * 杀死指定ID的作业
   * @param jobid 待杀死的作业ID
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void killJob(JobID jobid) throws IOException, InterruptedException;

  /**
   * 修改指定作业的优先级
   * @param jobid 作业ID
   * @param priority 要设置的优先级
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void setJobPriority(JobID jobid, String priority) 
  throws IOException, InterruptedException;
  
  /**
   * 杀死指定的任务尝试
   * @param taskId 待杀死的任务尝试ID
   * @param shouldFail 如果为true，任务将被标记为失败并计入作业失败统计；否则仅杀死不影响作业状态
   * @return 是否成功杀死任务
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */ 
  public boolean killTask(TaskAttemptID taskId, boolean shouldFail) 
  throws IOException, InterruptedException;
  
  /**
   * 获取指定作业的当前状态
   * @param jobid 作业ID
   * @return 作业状态对象，未找到作业则返回null
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public JobStatus getJobStatus(JobID jobid) 
  throws IOException, InterruptedException;

  /**
   * 获取指定作业的当前计数器
   * @param jobid 作业ID
   * @return 作业所有计数器信息
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public Counters getJobCounters(JobID jobid) 
  throws IOException, InterruptedException;
    
  /**
   * 获取指定作业对应类型的所有任务报告
   * @param jobid 作业ID
   * @param type 任务类型（Map/Reduce）
   * @return 对应类型任务的报告数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public TaskReport[] getTaskReports(JobID jobid, TaskType type)
  throws IOException, InterruptedException;

  /**
   * 获取MapReduce系统使用的文件系统标识
   * 客户端可根据该标识将作业文件复制到正确的存储位置
   * @return 文件系统名称：本地文件系统返回"local"，HDFS返回"addr:port"
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public String getFilesystemName() throws IOException, InterruptedException;

  /** 
   * 获取所有已提交的作业状态
   * @return 所有已提交作业的状态数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public JobStatus[] getAllJobs() throws IOException, InterruptedException;
  
  /**
   * 获取指定作业从某个事件ID开始的任务完成事件
   * @param jobid 作业ID
   * @param fromEventId 起始事件ID
   * @param maxEvents 最多返回的事件数量
   * @return 任务完成事件数组，无可用事件则返回空数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public TaskCompletionEvent[] getTaskCompletionEvents(JobID jobid,
    int fromEventId, int maxEvents) throws IOException, InterruptedException;
    
  /**
   * 获取指定任务尝试的诊断日志信息
   * @param taskId 任务尝试ID
   * @return 诊断消息数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public String[] getTaskDiagnostics(TaskAttemptID taskId) 
  throws IOException, InterruptedException;

  /** 
   * 获取集群中所有活跃的TaskTracker信息
   * @return 活跃TaskTracker信息数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public TaskTrackerInfo[] getActiveTrackers() 
  throws IOException, InterruptedException;

  /** 
   * 获取集群中所有被拉黑的TaskTracker信息
   * @return 被拉黑TaskTracker信息数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public TaskTrackerInfo[] getBlacklistedTrackers() 
  throws IOException, InterruptedException;

  /**
   * 获取JobTracker系统目录路径，用于存放作业相关文件
   * @return 系统目录路径
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public String getSystemDir() throws IOException, InterruptedException;
  
  /**
   * 获取作业Staging区域根目录路径提示，用于存放作业提交文件
   * @return Staging区域根目录路径
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public String getStagingAreaDir() throws IOException, InterruptedException;

  /**
   * 获取已完成作业历史文件存储目录
   * @return 作业历史目录路径
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public String getJobHistoryDir() 
  throws IOException, InterruptedException;

  /**
   * 获取JobTracker上所有队列信息
   * @return 队列信息对象数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public QueueInfo[] getQueues() throws IOException, InterruptedException;
  
  /**
   * 获取指定作业队列的调度信息
   * @param queueName 队列名称
   * @return 指定队列的调度信息
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public QueueInfo getQueue(String queueName) 
  throws IOException, InterruptedException;
  
  /**
   * 获取当前用户有权限访问的所有队列ACL信息
   * @return 当前用户的队列ACL信息数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public QueueAclsInfo[] getQueueAclsForCurrentUser() 
  throws IOException, InterruptedException;
  
  /**
   * 获取所有根级队列
   * @return 根队列信息数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public QueueInfo[] getRootQueues() throws IOException, InterruptedException;
  
  /**
   * 获取指定队列的直接子队列
   * @param queueName 父队列名称
   * @return 子队列信息数组
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public QueueInfo[] getChildQueues(String queueName) 
  throws IOException, InterruptedException;

  /**
   * 获取新的委托令牌，用于身份认证
   * @param renewer 允许更新该令牌的用户
   * @return 新生成的委托令牌
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public 
  Token<DelegationTokenIdentifier> getDelegationToken(Text renewer
                                                      ) throws IOException,
                                                          InterruptedException;
  
  /**
   * 更新现有委托令牌，延长过期时间
   * @param token 待更新的委托令牌
   * @return 更新后的过期时间
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token
                                   ) throws IOException,
                                            InterruptedException;
  
  /**
   * 取消现有委托令牌
   * @param token 待取消的委托令牌
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token
                                    ) throws IOException,
                                             InterruptedException;
  
  /**
   * 获取日志文件路径参数，若指定任务尝试ID则获取对应任务日志，否则获取作业日志
   * @param jobID 作业ID
   * @param taskAttemptID 任务尝试ID，可为null
   * @return 日志路径参数
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public LogParams getLogFileParams(JobID jobID, TaskAttemptID taskAttemptID)
      throws IOException, InterruptedException;
}