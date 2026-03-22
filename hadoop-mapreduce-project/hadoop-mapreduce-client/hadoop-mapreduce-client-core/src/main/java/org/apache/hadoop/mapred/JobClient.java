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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus.BlackListInfo;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.tools.CLI;
import org.apache.hadoop.mapreduce.util.ConfigUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 文件说明：MapReduce v1 API 客户端核心类，是用户作业与MapReduce集群交互的主要入口
 * 核心功能：提供作业提交、进度跟踪、任务日志获取、集群状态查询等能力
 * <code>JobClient</code> is the primary interface for the user-job to interact
 * with the cluster.
 * 
 * <code>JobClient</code> provides facilities to submit jobs, track their 
 * progress, access component-tasks' reports/logs, get the Map-Reduce cluster
 * status information etc.
 * 
 * <p>The job submission process involves:
 * <ol>
 *   <li>
 *   Checking the input and output specifications of the job.
 *   </li>
 *   <li>
 *   Computing the {@link InputSplit}s for the job.
 *   </li>
 *   <li>
 *   Setup the requisite accounting information for the {@link DistributedCache} 
 *   of the job, if necessary.
 *   </li>
 *   <li>
 *   Copying the job's jar and configuration to the map-reduce system directory 
 *   on the distributed file-system. 
 *   </li>
 *   <li>
 *   Submitting the job to the cluster and optionally monitoring
 *   it's status.
 *   </li>
 * </ol>
 *  
 * Normally the user creates the application, describes various facets of the
 * job via {@link JobConf} and then uses the <code>JobClient</code> to submit 
 * the job and monitor its progress.
 * 
 * <p>Here is an example on how to use <code>JobClient</code>:</p>
 * <p><blockquote><pre>
 *     // Create a new JobConf
 *     JobConf job = new JobConf(new Configuration(), MyJob.class);
 *     
 *     // Specify various job-specific parameters     
 *     job.setJobName("myjob");
 *     
 *     job.setInputPath(new Path("in"));
 *     job.setOutputPath(new Path("out"));
 *     
 *     job.setMapperClass(MyJob.MyMapper.class);
 *     job.setReducerClass(MyJob.MyReducer.class);
 *
 *     // Submit the job, then poll for progress until the job is complete
 *     JobClient.runJob(job);
 * </pre></blockquote>
 * 
 * <b id="JobControl">Job Control</b>
 * 
 * <p>At times clients would chain map-reduce jobs to accomplish complex tasks 
 * which cannot be done via a single map-reduce job. This is fairly easy since 
 * the output of the job, typically, goes to distributed file-system and that 
 * can be used as the input for the next job.</p>
 * 
 * <p>However, this also means that the onus on ensuring jobs are complete 
 * (success/failure) lies squarely on the clients. In such situations the 
 * various job-control options are:
 * <ol>
 *   <li>
 *   {@link #runJob(JobConf)} : submits the job and returns only after 
 *   the job has completed.
 *   </li>
 *   <li>
 *   {@link #submitJob(JobConf)} : only submits the job, then poll the 
 *   returned handle to the {@link RunningJob} to query status and make 
 *   scheduling decisions.
 *   </li>
 *   <li>
 *   {@link JobConf#setJobEndNotificationURI(String)} : setup a notification
 *   on job-completion, thus avoiding polling.
 *   </li>
 * </ol>
 * 
 * @see JobConf
 * @see ClusterStatus
 * @see Tool
 * @see DistributedCache
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JobClient extends CLI implements AutoCloseable {

  @InterfaceAudience.Private
  public static final String MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_KEY =
      "mapreduce.jobclient.retry.policy.enabled";
  @InterfaceAudience.Private
  public static final boolean MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_DEFAULT =
      false;
  @InterfaceAudience.Private
  public static final String MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_KEY =
      "mapreduce.jobclient.retry.policy.spec";
  @InterfaceAudience.Private
  public static final String MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_DEFAULT =
      "10000,6,60000,10"; // t1,n1,t2,n2,...

  /** 任务状态输出过滤枚举，用于控制监控时输出哪些状态任务的日志 */
  public enum TaskStatusFilter { NONE, KILLED, FAILED, SUCCEEDED, ALL }
  private TaskStatusFilter taskOutputFilter = TaskStatusFilter.FAILED; 
  
  private int maxRetry = MRJobConfig.DEFAULT_MR_CLIENT_JOB_MAX_RETRIES;
  private long retryInterval =
      MRJobConfig.DEFAULT_MR_CLIENT_JOB_RETRY_INTERVAL;

  static{
    // 加载MapReduce配置资源
    ConfigUtil.loadResources();
  }

  /**
   * 网络作业实现类，包装新API的Job对象，提供旧API的RunningJob接口
   * 作用：适配新旧API，通过远程集群服务获取作业状态信息
   * A NetworkedJob is an implementation of RunningJob.  It holds
   * a JobProfile object to provide some info, and interacts with the
   * remote service to provide certain functionality.
   */
  static class NetworkedJob implements RunningJob {
    Job job;
    /**
     * We store a JobProfile and a timestamp for when we last
     * acquired the job profile.  If the job is null, then we cannot
     * perform any of the tasks.  The job might be null if the cluster
     * has completely forgotten about the job.  (eg, 24 hours after the
     * job completes.)
     */
    public NetworkedJob(JobStatus status, Cluster cluster) throws IOException {
      this(status, cluster, new JobConf(status.getJobFile()));
    }
    
    private NetworkedJob(JobStatus status, Cluster cluster, JobConf conf)
        throws IOException {
      this(Job.getInstance(cluster, status, conf));
    }

    public NetworkedJob(Job job) throws IOException {
      this.job = job;
    }

    public Configuration getConfiguration() {
      return job.getConfiguration();
    }

    /**
     * 获取作业ID，返回旧API格式的JobID
     * An identifier for the job
     */
    public JobID getID() {
      return JobID.downgrade(job.getJobID());
    }
    
    /** @deprecated This method is deprecated and will be removed. Applications should 
     * rather use {@link #getID()}.*/
    @Deprecated
    public String getJobID() {
      return getID().toString();
    }
    
    /**
     * 获取用户指定的作业名称
     * The user-specified job name
     */
    public String getJobName() {
      return job.getJobName();
    }

    /**
     * 获取作业配置文件路径
     * The name of the job file
     */
    public String getJobFile() {
      return job.getJobFile();
    }

    /**
     * 获取作业跟踪页面的URL地址
     * A URL where the job's status can be seen
     */
    public String getTrackingURL() {
      return job.getTrackingURL();
    }

    /**
     * 获取Map阶段进度，0.0~1.0表示完成百分比
     * A float between 0.0 and 1.0, indicating the % of map work
     * completed.
     */
    public float mapProgress() throws IOException {
      return job.mapProgress();
    }

    /**
     * 获取Reduce阶段进度，0.0~1.0表示完成百分比
     * A float between 0.0 and 1.0, indicating the % of reduce work
     * completed.
     */
    public float reduceProgress() throws IOException {
      return job.reduceProgress();
    }

    /**
     * 获取Cleanup阶段进度，0.0~1.0表示完成百分比
     * A float between 0.0 and 1.0, indicating the % of cleanup work
     * completed.
     */
    public float cleanupProgress() throws IOException {
      try {
        return job.cleanupProgress();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    /**
     * 获取Setup阶段进度，0.0~1.0表示完成百分比
     * A float between 0.0 and 1.0, indicating the % of setup work
     * completed.
     */
    public float setupProgress() throws IOException {
      return job.setupProgress();
    }

    /**
     * 立即返回作业是否已完成
     * Returns immediately whether the whole job is done yet or not.
     */
    public synchronized boolean isComplete() throws IOException {
      return job.isComplete();
    }

    /**
     * 返回作业是否成功完成
     * True iff job completed successfully.
     */
    public synchronized boolean isSuccessful() throws IOException {
      return job.isSuccessful();
    }

    /**
     * 阻塞等待直到作业完成
     * Blocks until the job is finished
     */
    public void waitForCompletion() throws IOException {
      try {
        job.waitForCompletion(false);
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      } catch (ClassNotFoundException ce) {
        throw new IOException(ce);
      }
    }

    /**
     * 从集群获取当前作业状态
     * Tells the service to get the state of the current job.
     */
    public synchronized int getJobState() throws IOException {
      try {
        return job.getJobState().getValue();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    
    /**
     * 请求集群终止当前作业
     * Tells the service to terminate the current job.
     */
    public synchronized void killJob() throws IOException {
      job.killJob();
    }
   
    
    /** Set the priority of the job.
    * @param priority new priority of the job. 
    */
    public synchronized void setJobPriority(String priority) 
                                                throws IOException {
      try {
        job.setPriority(
          org.apache.hadoop.mapreduce.JobPriority.valueOf(priority));
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    
    /**
     * Kill indicated task attempt.
     * @param taskId the id of the task to kill.
     * @param shouldFail if true the task is failed and added to failed tasks list, otherwise
     * it is just killed, w/o affecting job failure status.
     */
    public synchronized void killTask(TaskAttemptID taskId,
        boolean shouldFail) throws IOException {
      if (shouldFail) {
        job.failTask(taskId);
      } else {
        job.killTask(taskId);
      }
    }

    /** @deprecated Applications should rather use {@link #killTask(TaskAttemptID, boolean)}*/
    @Deprecated
    public synchronized void killTask(String taskId, boolean shouldFail) throws IOException {
      killTask(TaskAttemptID.forName(taskId), shouldFail);
    }
    
    /**
     * 从集群获取本作业的任务完成事件列表
     * Fetch task completion events from cluster for this job. 
     */
    public synchronized TaskCompletionEvent[] getTaskCompletionEvents(
        int startFrom) throws IOException {
      try {
        org.apache.hadoop.mapreduce.TaskCompletionEvent[] acls = 
          job.getTaskCompletionEvents(startFrom, 10);
        TaskCompletionEvent[] ret = new TaskCompletionEvent[acls.length];
        // 转换新API事件对象为旧API格式
        for (int i = 0 ; i < acls.length; i++ ) {
          ret[i] = TaskCompletionEvent.downgrade(acls[i]);
        }
        return ret;
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    /**
     * Dump stats to screen
     */
    @Override
    public String toString() {
      return job.toString();
    }
        
    /**
     * 获取作业的所有计数器
     * Returns the counters for this job
     */
    public Counters getCounters() throws IOException {
      Counters result = null;
      org.apache.hadoop.mapreduce.Counters temp = job.getCounters();
      if(temp != null) {
        // 转换新API计数器为旧API格式
        result = Counters.downgrade(temp);
      }
      return result;
    }
    
    @Override
    public String[] getTaskDiagnostics(TaskAttemptID id) throws IOException {
      try { 
        return job.getTaskDiagnostics(id);
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    public String getHistoryUrl() throws IOException {
      try {
        return job.getHistoryUrl();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    public boolean isRetired() throws IOException {
      try {
        return job.isRetired();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    
    boolean monitorAndPrintJob() throws IOException, InterruptedException {
      return job.monitorAndPrintJob();
    }
    
    @Override
    public String getFailureInfo() throws IOException {
      try {
        return job.getStatus().getFailureInfo();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    @Override
    public JobStatus getJobStatus() throws IOException {
      try {
        return JobStatus.downgrade(job.getStatus());
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
  }

  /** 客户端当前用户的UGI信息，所有RPC请求都使用该用户身份 */
  UserGroupInformation clientUgi;
  
  /**
   * 创建一个未初始化的JobClient对象
   * Create a job client.
   */
  public JobClient() {
  }
    
  /**
   * 使用给定配置创建JobClient并连接到默认集群
   * 
   * @param conf 作业配置
   * @throws IOException 连接失败时抛出
   */
  public JobClient(JobConf conf) throws IOException {
    init(conf);
  }

  /**
   * 使用给定配置创建JobClient并连接到默认集群
   * 
   * @param conf 配置对象
   * @throws IOException 连接失败时抛出
   */
  public JobClient(Configuration conf) throws IOException {
    init(new JobConf(conf));
  }

  /**
   * 初始化JobClient并连接到默认集群
   * @param conf 作业配置
   * @throws IOException 初始化/连接失败时抛出
   */
  public void init(JobConf conf) throws IOException {
    setConf(conf);
    // 创建集群连接对象
    cluster = new Cluster(conf);
    // 获取当前用户UGI
    clientUgi = UserGroupInformation.getCurrentUser();

    // 从配置读取获取作业最大重试次数
    maxRetry = conf.getInt(MRJobConfig.MR_CLIENT_JOB_MAX_RETRIES,
      MRJobConfig.DEFAULT_MR_CLIENT_JOB_MAX_RETRIES);

    // 从配置读取重试间隔时间