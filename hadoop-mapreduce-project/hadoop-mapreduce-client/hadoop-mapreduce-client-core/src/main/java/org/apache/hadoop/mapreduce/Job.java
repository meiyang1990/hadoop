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

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.util.ConfigUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ReservationId;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce作业客户端核心类，面向用户提供作业配置、提交、状态查询和执行监控能力。
 * 
 * <p>用户可通过此类完成作业配置、提交到YARN集群，并轮询监控作业执行状态。配置修改仅允许在作业提交前进行，
 * 提交后修改会抛出IllegalStateException异常。</p>
 * 
 * <p>典型使用示例：</p>
 * <p><blockquote><pre>
 *     // 创建新的Job实例
 *     Job job = Job.getInstance();
 *     job.setJarByClass(MyJob.class);
 *     
 *     // 设置作业参数     
 *     job.setJobName("myjob");
 *     
 *     job.setInputPath(new Path("in"));
 *     job.setOutputPath(new Path("out"));
 *     
 *     job.setMapperClass(MyJob.MyMapper.class);
 *     job.setReducerClass(MyJob.MyReducer.class);
 *
 *     // 提交作业并轮询等待完成
 *     job.waitForCompletion(true);
 * </pre></blockquote>
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Job extends JobContextImpl implements JobContext, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(Job.class);

  /** 作业生命周期状态枚举 */
  @InterfaceStability.Evolving
  public enum JobState {DEFINE, RUNNING};
  /** 作业状态缓存最大有效期，超过则需要从集群刷新 */
  private static final long MAX_JOBSTATUS_AGE = 1000 * 2;
  /** 配置键：任务输出日志过滤类型 */
  public static final String OUTPUT_FILTER = "mapreduce.client.output.filter";
  /** 配置键：作业完成状态轮询间隔（毫秒） */
  public static final String COMPLETION_POLL_INTERVAL_KEY = 
    "mapreduce.client.completion.pollinterval";
  
  /** 默认完成轮询间隔：5000毫秒 */
  static final int DEFAULT_COMPLETION_POLL_INTERVAL = 5000;
  /** 配置键：进度监控轮询间隔（毫秒） */
  public static final String PROGRESS_MONITOR_POLL_INTERVAL_KEY =
    "mapreduce.client.progressmonitor.pollinterval";
  /** 默认进度监控轮询间隔：1000毫秒 */
  static final int DEFAULT_MONITOR_POLL_INTERVAL = 1000;

  /** 配置标记：是否使用通用命令行参数解析器 */
  public static final String USED_GENERIC_PARSER = 
      "mapreduce.client.genericoptionsparser.used";
  /** 配置键：提交作业文件副本数 */
  public static final String SUBMIT_REPLICATION = 
      "mapreduce.client.submit.file.replication";
  /** 默认作业提交文件副本数 */
  public static final int DEFAULT_SUBMIT_REPLICATION = 10;
  /** 配置键：是否使用通配符模式处理libjars */
  public static final String USE_WILDCARD_FOR_LIBJARS =
      "mapreduce.client.libjars.wildcard";
  /** 默认开启通配符处理libjars */
  public static final boolean DEFAULT_USE_WILDCARD_FOR_LIBJARS = true;

  /** 任务状态过滤枚举，用于控制日志输出 */
  @InterfaceStability.Evolving
  public enum TaskStatusFilter { NONE, KILLED, FAILED, SUCCEEDED, ALL }

  static {
    ConfigUtil.loadResources();
  }

  private JobState state = JobState.DEFINE;
  private JobStatus status;
  private long statustime;
  private Cluster cluster;
  private ReservationId reservationId;

  /**
   * @deprecated Use {@link #getInstance()}
   */
  @Deprecated
  public Job() throws IOException {
    this(new JobConf(new Configuration()));
  }

  /**
   * @deprecated Use {@link #getInstance(Configuration)}
   */
  @Deprecated
  public Job(Configuration conf) throws IOException {
    this(new JobConf(conf));
  }

  /**
   * @deprecated Use {@link #getInstance(Configuration, String)}
   */
  @Deprecated
  public Job(Configuration conf, String jobName) throws IOException {
    this(new JobConf(conf));
    setJobName(jobName);
  }

  Job(JobConf conf) throws IOException {
    super(conf, null);
    // 传播现有用户凭证到当前作业
    this.credentials.mergeAll(this.ugi.getCredentials());
    this.cluster = null;
  }

  Job(JobStatus status, JobConf conf) throws IOException {
    this(conf);
    setJobID(status.getJobID());
    this.status = status;
    state = JobState.RUNNING;
  }

      
  /**
   * 创建空配置的Job实例，会自动创建默认Cluster连接。
   * 
   * @return 未连接集群的Job实例
   * @throws IOException 创建失败时抛出IO异常
   */
  public static Job getInstance() throws IOException {
    // create with a null Cluster
    return getInstance(new Configuration());
  }
      
  /**
   * 使用指定配置创建Job实例，Cluster连接会在需要时自动创建。
   * Job会复制一份传入的Configuration，内部修改不会影响原配置对象。
   * 
   * @param conf 作业基础配置
   * @return 未连接集群的Job实例
   * @throws IOException 创建失败时抛出IO异常
   */
  public static Job getInstance(Configuration conf) throws IOException {
    // create with a null Cluster
    JobConf jobConf = new JobConf(conf);
    return new Job(jobConf);
  }

      
  /**
   * 使用指定配置和作业名称创建Job实例，Cluster连接会在需要时自动创建。
   * Job会复制一份传入的Configuration，内部修改不会影响原配置对象。
   * 
   * @param conf 作业基础配置
   * @param jobName 作业名称
   * @return 未连接集群的Job实例
   * @throws IOException 创建失败时抛出IO异常
   */
  public static Job getInstance(Configuration conf, String jobName)
           throws IOException {
    // create with a null Cluster
    Job result = getInstance(conf);
    result.setJobName(jobName);
    return result;
  }
  
  /**
   * 使用已有的JobStatus和配置创建Job实例，用于连接已有运行中的作业。
   * Job会复制一份传入的Configuration，内部修改不会影响原配置对象。
   * Cluster连接会在需要时自动创建。
   * 
   * @param status 已有作业状态
   * @param conf 作业配置
   * @return 未连接集群的Job实例
   * @throws IOException 创建失败时抛出IO异常
   */
  public static Job getInstance(JobStatus status, Configuration conf) 
  throws IOException {
    return new Job(status, new JobConf(conf));
  }

  /**
   * Creates a new {@link Job} with no particular {@link Cluster}.
   * A Cluster will be created from the conf parameter only when it's needed.
   *
   * The <code>Job</code> makes a copy of the <code>Configuration</code> so 
   * that any necessary internal modifications do not reflect on the incoming 
   * parameter.
   * 
   * @param ignored
   * @return the {@link Job} , with no connection to a cluster yet.
   * @throws IOException
   * @deprecated Use {@link #getInstance()}
   */
  @Deprecated
  public static Job getInstance(Cluster ignored) throws IOException {
    return getInstance();
  }
  
  /**
   * Creates a new {@link Job} with no particular {@link Cluster} and given
   * {@link Configuration}.
   * A Cluster will be created from the conf parameter only when it's needed.
   * 
   * The <code>Job</code> makes a copy of the <code>Configuration</code> so 
   * that any necessary internal modifications do not reflect on the incoming 
   * parameter.
   * 
   * @param ignored
   * @param conf job configuration
   * @return the {@link Job} , with no connection to a cluster yet.
   * @throws IOException
   * @deprecated Use {@link #getInstance(Configuration)}
   */
  @Deprecated
  public static Job getInstance(Cluster ignored, Configuration conf) 
      throws IOException {
    return getInstance(conf);
  }
  
  /**
   * 使用指定Cluster、JobStatus和配置创建Job实例，私有API仅供内部使用。
   * 
   * @param cluster YARN集群客户端连接对象
   * @param status 已有作业状态
   * @param conf 作业配置
   * @return 绑定了集群连接的Job实例
   * @throws IOException 创建失败时抛出IO异常
   */
  @Private
  public static Job getInstance(Cluster cluster, JobStatus status, 
      Configuration conf) throws IOException {
    Job job = getInstance(status, conf);
    job.setCluster(cluster);
    return job;
  }

  /**
   * 检查作业当前状态是否符合预期，不符合则抛出异常。
   * 用于保证配置修改、状态查询等操作仅在允许的生命周期阶段执行。
   * @param state 预期的作业状态
   * @throws IllegalStateException 当前状态不符合预期时抛出
   */
  private void ensureState(JobState state) throws IllegalStateException {
    if (state != this.state) {
      throw new IllegalStateException("Job in state "+ this.state + 
                                      " instead of " + state);
    }

    if (state == JobState.RUNNING && cluster == null) {
      throw new IllegalStateException
        ("Job in state " + this.state
         + ", but it isn't attached to any job tracker!");
    }
  }

  /**
   * 如果本地缓存的作业状态过期，从集群刷新最新状态。
   * @throws IOException 刷新失败时抛出IO异常
   */
  synchronized void ensureFreshStatus() 
      throws IOException {
    if (System.currentTimeMillis() - statustime > MAX_JOBSTATUS_AGE) {
      updateStatus();
    }
  }
    
  /**
   * 强制从集群拉取最新作业状态更新本地缓存。
   * @throws IOException 更新失败时抛出IO异常
   */
  synchronized void updateStatus() throws IOException {
    try {
      this.status = ugi.doAs(new PrivilegedExceptionAction<JobStatus>() {
        @Override
        public JobStatus run() throws IOException, InterruptedException {
          // 以当前用户身份向集群请求作业状态
          return cluster.getClient().getJobStatus(getJobID());
        }
      });
    }
    catch (InterruptedException ie) {
      throw new IOException(ie);
    }
    if (this.status == null) {
      throw new IOException("Job status not available ");
    }
    // 更新状态缓存时间戳
    this.statustime = System.currentTimeMillis();
  }
  
  /**
   * 获取作业完整状态对象，包含所有状态信息。
   * @return 最新作业状态
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public JobStatus getStatus() throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    updateStatus();
    return status;
  }

  /**
   * 获取作业当前执行状态（PREP/RUNNING/SUCCEEDED/FAILED等）。
   * @return 作业执行状态枚举
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public JobStatus.State getJobState() 
      throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    updateStatus();
    return status.getState();
  }
  
  /**
   * 获取作业进度追踪页面的URL。
   * @return 追踪页面URL字符串
   */
  public String getTrackingURL(){
    ensureState(JobState.RUNNING);
    return status.getTrackingUrl().toString();
  }

  /**
   * 获取提交作业配置文件在HDFS上的路径。
   * @return 作业配置文件路径
   */
  public String getJobFile() {
    ensureState(JobState.RUNNING);
    return status.getJobFile();
  }

  /**
   * 获取作业启动时间戳。
   * @return 作业启动时间（毫秒，从1970年开始计算）
   */
  public long getStartTime() {
    ensureState(JobState.RUNNING);
    return status.getStartTime();
  }

  /**
   * 获取作业完成时间戳。
   * @return 作业完成时间（毫秒，从1970年开始计算）
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public long getFinishTime() throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    updateStatus();
    return status.getFinishTime();
  }

  /**
   * 获取作业调度信息字符串。
   * @return 调度信息文本
   */
  public String getSchedulingInfo() {
    ensureState(JobState.RUNNING);
    return status.getSchedulingInfo();
  }

  /**
   * 获取作业优先级。
   * @return 作业优先级枚举
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public JobPriority getPriority() throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    updateStatus();
    return status.getPriority();
  }

  /**
   * 获取用户指定的作业名称。
   * @return 作业名称
   */
  public String getJobName() {
    if (state == JobState.DEFINE || status == null) {
      return super.getJobName();
    }
    ensureState(JobState.RUNNING);
    return status.getJobName();
  }

  /**
   * 获取作业历史日志URL。
   * @return 历史日志URL
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public String getHistoryUrl() throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    updateStatus();
    return status.getHistoryFile();
  }

  /**
   * 检查作业是否已退役（从活动队列移除）。
   * @return true表示已退役
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public boolean isRetired() throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    updateStatus();
    return status.isRetired();
  }
  
  /**
   * 获取绑定的YARN集群客户端对象，仅供内部使用。
   * @return 集群客户端对象
   */
  @Private
  public Cluster getCluster() {
    return cluster;
  }

  /**
   * 设置绑定的集群客户端，仅供单元测试Mock使用。
   * @param cluster 集群客户端对象
   */
  @Private
  private void setCluster(Cluster cluster) {
    this.cluster = cluster;
  }

  /**
   * 生成作业状态信息字符串，输出到控制台。
   * @return 作业状态文本描述
   */
  @Override
  public String toString() {
    ensureState(JobState.RUNNING);
    String reasonforFailure = " ";
    int numMaps = 0;
    int numReduces = 0;
    try {
      updateStatus();
      if (status.getState().equals(JobStatus.State.FAILED))
        reasonforFailure = getTaskFailureEventString();
      numMaps = getTaskReports(TaskType.MAP).length;
      numReduces = getTaskReports(TaskType.REDUCE).length;
    } catch (IOException e) {
    } catch (InterruptedException ie) {
    }
    StringBuilder