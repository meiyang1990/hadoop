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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.util.ConfigUtil;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件：org.apache.hadoop.mapreduce.Cluster
 * 提供访问MapReduce集群信息和操作集群作业的客户端入口，封装了与服务端通信的底层协议，为上层Job API提供集群服务能力。
 * 支持不同集群部署模式（本地、YARN）的协议适配，通过SPI机制加载对应的协议提供者。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Cluster {
  
  /**
   * JobTracker（资源协调者）的运行状态枚举
   */
  @InterfaceStability.Evolving
  public enum JobTrackerStatus {INITIALIZING, RUNNING};

  private ClientProtocolProvider clientProtocolProvider;
  private ClientProtocol client;
  private UserGroupInformation ugi;
  private Configuration conf;
  private FileSystem fs = null;
  private Path sysDir = null;
  private Path stagingAreaDir = null;
  private Path jobHistoryDir = null;
  private static final Logger LOG =
      LoggerFactory.getLogger(Cluster.class);

  @VisibleForTesting
  static Iterable<ClientProtocolProvider> frameworkLoader =
      ServiceLoader.load(ClientProtocolProvider.class);
  private volatile List<ClientProtocolProvider> providerList = null;

  /**
   * 延迟初始化协议提供者列表，通过SPI加载所有可用的ClientProtocolProvider，线程安全的双重检查锁定实现
   */
  private void initProviderList() {
    if (providerList == null) {
      synchronized (frameworkLoader) {
        if (providerList == null) {
          List<ClientProtocolProvider> localProviderList = new ArrayList<>();
          try {
            for (ClientProtocolProvider provider : frameworkLoader) {
              localProviderList.add(provider);
            }
          } catch(ServiceConfigurationError | LinkageError e) {
            // 加载协议提供者失败，记录日志不中断流程，后续会尝试其他提供者
            LOG.info("Failed to instantiate ClientProtocolProvider, please "
                         + "check the /META-INF/services/org.apache."
                         + "hadoop.mapreduce.protocol.ClientProtocolProvider "
                         + "files on the classpath", e);
          }
          providerList = localProviderList;
        }
      }
    }
  }

  // 静态块加载MapReduce配置资源
  static {
    ConfigUtil.loadResources();
  }
  
  /**
   * 构造Cluster对象，从配置中自动发现集群地址
   * @param conf Hadoop配置对象
   * @throws IOException 初始化失败时抛出IO异常
   */
  public Cluster(Configuration conf) throws IOException {
    this(null, conf);
  }

  /**
   * 构造Cluster对象，指定JobTracker地址
   * @param jobTrackAddr JobTracker的网络地址
   * @param conf Hadoop配置对象
   * @throws IOException 初始化失败时抛出IO异常
   */
  public Cluster(InetSocketAddress jobTrackAddr, Configuration conf) 
      throws IOException {
    this.conf = conf;
    this.ugi = UserGroupInformation.getCurrentUser();
    initialize(jobTrackAddr, conf);
  }
  
  /**
   * 初始化Cluster，遍历所有协议提供者，找到适配当前配置的可用协议并建立连接
   * @param jobTrackAddr JobTracker地址，为null时从配置自动发现
   * @param conf Hadoop配置对象
   * @throws IOException 没有可用协议提供者时抛出异常
   */
  private void initialize(InetSocketAddress jobTrackAddr, Configuration conf)
      throws IOException {

    initProviderList();
    // 初始化异常，收集所有提供者初始化失败的信息
    final IOException initEx = new IOException(
        "Cannot initialize Cluster. Please check your configuration for "
            + MRConfig.FRAMEWORK_NAME
            + " and the correspond server addresses.");
    if (jobTrackAddr != null) {
      LOG.info(
          "Initializing cluster for Job Tracker=" + jobTrackAddr.toString());
    }
    // 遍历所有提供者尝试创建客户端协议
    for (ClientProtocolProvider provider : providerList) {
      LOG.debug("Trying ClientProtocolProvider : "
          + provider.getClass().getName());
      ClientProtocol clientProtocol = null;
      try {
        // 根据是否指定地址调用对应创建方法
        if (jobTrackAddr == null) {
          clientProtocol = provider.create(conf);
        } else {
          clientProtocol = provider.create(jobTrackAddr, conf);
        }

        if (clientProtocol != null) {
          // 找到可用提供者，保存引用并终止遍历
          clientProtocolProvider = provider;
          client = clientProtocol;
          LOG.debug("Picked " + provider.getClass().getName()
              + " as the ClientProtocolProvider");
          break;
        } else {
          // 该提供者不适用当前配置
          LOG.debug("Cannot pick " + provider.getClass().getName()
              + " as the ClientProtocolProvider - returned null protocol");
        }
      } catch (Exception e) {
        // 该提供者初始化失败，记录异常信息到根异常
        final String errMsg = "Failed to use " + provider.getClass().getName()
            + " due to error: ";
        initEx.addSuppressed(new IOException(errMsg, e));
        LOG.info(errMsg, e);
      }
    }

    // 没有找到可用协议提供者，抛出收集后的异常
    if (null == clientProtocolProvider || null == client) {
      throw initEx;
    }
  }

  ClientProtocol getClient() {
    return client;
  }
  
  Configuration getConf() {
    return conf;
  }
  
  /**
   * 关闭Cluster连接，释放底层协议资源
   * @throws IOException 关闭失败时抛出IO异常
   */
  public synchronized void close() throws IOException {
    clientProtocolProvider.close(client);
  }

  /**
   * 将服务端返回的JobStatus数组转换为Job对象数组
   * @param stats 服务端返回的作业状态数组
   * @return 封装好的Job对象数组
   * @throws IOException 创建Job对象失败时抛出IO异常
   */
  private Job[] getJobs(JobStatus[] stats) throws IOException {
    List<Job> jobs = new ArrayList<Job>();
    for (JobStatus stat : stats) {
      jobs.add(Job.getInstance(this, stat, new JobConf(stat.getJobFile())));
    }
    return jobs.toArray(new Job[0]);
  }

  /**
   * 获取存储作业文件的Hadoop文件系统对象
   * @return 作业所在文件系统对象
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public synchronized FileSystem getFileSystem() 
      throws IOException, InterruptedException {
    if (this.fs == null) {
      try {
        // 使用当前用户权限获取文件系统
        this.fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
          public FileSystem run() throws IOException, InterruptedException {
            final Path sysDir = new Path(client.getSystemDir());
            return sysDir.getFileSystem(getConf());
          }
        });
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return fs;
  }

  /**
   * 根据作业ID获取对应Job对象
   * @param jobId 作业ID
   * @return 对应Job对象，如果作业不存在返回null
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public Job getJob(JobID jobId) throws IOException, InterruptedException {
    JobStatus status = client.getJobStatus(jobId);
    if (status != null) {
      JobConf conf;
      try {
        conf = new JobConf(status.getJobFile());
      } catch (RuntimeException ex) {
        // 如果是作业配置文件不存在，说明找不到作业，返回null
        if (ex.getCause() instanceof FileNotFoundException) {
          return null;
        } else {
          throw ex;
        }
      }
      return Job.getInstance(this, status, conf);
    }
    return null;
  }
  
  /**
   * 获取集群所有队列信息
   * @return 队列信息数组
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public QueueInfo[] getQueues() throws IOException, InterruptedException {
    return client.getQueues();
  }
  
  /**
   * 获取指定名称队列的信息
   * @param name 队列名称
   * @return 指定队列的信息对象
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public QueueInfo getQueue(String name) 
      throws IOException, InterruptedException {
    return client.getQueue(name);
  }

  /**
   * 获取指定作业/任务尝试的日志参数（日志地址等信息）
   * @param jobID 作业ID
   * @param taskAttemptID 任务尝试ID，可选
   * @return 日志参数对象
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public LogParams getLogParams(JobID jobID, TaskAttemptID taskAttemptID)
      throws IOException, InterruptedException {
    return client.getLogFileParams(jobID, taskAttemptID);
  }

  /**
   * 获取集群当前指标状态
   * @return 集群指标对象
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public ClusterMetrics getClusterStatus() throws IOException, InterruptedException {
    return client.getClusterMetrics();
  }
  
  /**
   * 获取集群所有活跃的TaskTracker节点信息
   * @return 活跃TaskTracker数组
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public TaskTrackerInfo[] getActiveTaskTrackers() 
      throws IOException, InterruptedException  {
    return client.getActiveTrackers();
  }
  
  /**
   * 获取集群所有被拉黑的TaskTracker节点信息
   * @return 拉黑TaskTracker数组
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public TaskTrackerInfo[] getBlackListedTaskTrackers() 
      throws IOException, InterruptedException  {
    return client.getBlacklistedTrackers();
  }
  
  /**
   * 获取集群所有作业的Job对象数组
   * @return 所有作业数组
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   * @deprecated Use {@link #getAllJobStatuses()} instead.
   */
  @Deprecated
  public Job[] getAllJobs() throws IOException, InterruptedException {
    return getJobs(client.getAllJobs());
  }

  /**
   * 获取集群所有作业的状态数组
   * @return 所有作业状态数组
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public JobStatus[] getAllJobStatuses() throws IOException, InterruptedException {
    return client.getAllJobs();
  }

  /**
   * 获取JobTracker系统目录路径，用于存放作业相关文件
   * @return 系统目录Path对象
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public Path getSystemDir() throws IOException, InterruptedException {
    if (sysDir == null) {
      sysDir = new Path(client.getSystemDir());
    }
    return sysDir;
  }
  
  /**
   * 获取JobTracker的暂存区目录路径，用于存放作业提交过程中的临时文件
   * @return 暂存区目录Path对象
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public Path getStagingAreaDir() throws IOException, InterruptedException {
    if (stagingAreaDir == null) {
      stagingAreaDir = new Path(client.getStagingAreaDir());
    }
    return stagingAreaDir;
  }

  /**
   * 获取指定作业的作业历史文件URL路径，仅已完成作业存在该文件
   * @param jobId 作业ID
   * @return 作业历史文件URL路径字符串
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public String getJobHistoryUrl(JobID jobId) throws IOException, 
    InterruptedException {
    if (jobHistoryDir == null) {
      jobHistoryDir = new Path(client.getJobHistoryDir());
    }
    return new Path(jobHistoryDir, jobId.toString() + "_"
                    + ugi.getShortUserName()).toString();
  }

  /**
   * 获取当前用户有权限访问的队列ACL信息
   * @return 当前用户的队列ACL信息数组
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public QueueAclsInfo[] getQueueAclsForCurrentUser() 
      throws IOException, InterruptedException  {
    return client.getQueueAclsForCurrentUser();
  }

  /**
   * 获取根级队列列表
   * @return 根队列信息数组
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
    return client.getRootQueues();
  }
  
  /**
   * 获取指定队列的直接子队列列表
   * @param queueName 父队列名称
   * @return 子队列信息数组
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public QueueInfo[] getChildQueues(String queueName) 
      throws IOException, InterruptedException {
    return client.getChildQueues(queueName);
  }
  
  /**
   * 获取JobTracker当前运行状态
   * @return JobTracker状态枚举
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public JobTrackerStatus getJobTrackerStatus() throws IOException,
      InterruptedException {
    return client.getJobTrackerStatus();
  }
  
  /**
   * 获取TaskTracker心跳过期时间间隔
   * @return 过期时间间隔，单位毫秒
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public long getTaskTrackerExpiryInterval() throws IOException,
      InterruptedException {
    return client.getTaskTrackerExpiryInterval();
  }

  /**
   * 从JobTracker获取当前用户的 delegation token
   * @param renewer 可以更新该token的用户
   * @return 新生成的delegation token
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public Token<DelegationTokenIdentifier> 
      getDelegationToken(Text renewer) throws IOException, InterruptedException{
    // client has already set the service
    return client.getDelegationToken(renewer);
  }

  /**
   * 更新delegation token有效期
   * @param token 需要更新的token
   * @return 新的过期时间戳
   * @throws InvalidToken token无效时抛出
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   * @deprecated Use {@link Token#renew} instead
   */
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token
                                   ) throws InvalidToken, IOException,
                                            InterruptedException {
    return token.renew(getConf());
  }

  /**
   * 取消delegation token
   * @param token 需要取消的token
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   * @deprecated Use {@link Token#cancel} instead
   */
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token
                                    ) throws IOException,
                                             InterruptedException {
    token.cancel(getConf());
  }

}