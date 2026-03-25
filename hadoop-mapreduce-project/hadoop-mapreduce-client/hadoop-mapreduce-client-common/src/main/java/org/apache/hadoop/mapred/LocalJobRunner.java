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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.KeyGenerator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件描述：本地MapReduce任务运行器，在当前进程内本地执行MapReduce作业，主要用于开发调试
 * 核心职责：实现单节点本地的MapReduce作业执行逻辑，替代分布式集群完成小作业调试
 */
/** Implements MapReduce locally, in-process, for debugging. */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class LocalJobRunner implements ClientProtocol {
  public static final Logger LOG =
      LoggerFactory.getLogger(LocalJobRunner.class);

  /** 本地模式下Map任务最大并发数配置项 */
  /** The maximum number of map tasks to run in parallel in LocalJobRunner */
  public static final String LOCAL_MAX_MAPS =
    "mapreduce.local.map.tasks.maximum";

  /** 本地模式下Reduce任务最大并发数配置项 */
  /** The maximum number of reduce tasks to run in parallel in LocalJobRunner */
  public static final String LOCAL_MAX_REDUCES =
    "mapreduce.local.reduce.tasks.maximum";

  public static final String INTERMEDIATE_DATA_ENCRYPTION_ALGO = "HmacSHA1";

  private FileSystem fs;
  private HashMap<JobID, Job> jobs = new HashMap<JobID, Job>();
  private JobConf conf;
  private AtomicInteger map_tasks = new AtomicInteger(0);
  private AtomicInteger reduce_tasks = new AtomicInteger(0);
  final Random rand = new Random();
  
  private LocalJobRunnerMetrics myMetrics = null;

  private static final String jobDir =  "localRunner/";

  /**
   * 获取RPC协议版本
   * @param protocol 协议名称
   * @param clientVersion 客户端版本
   * @return 协议版本ID
   */
  public long getProtocolVersion(String protocol, long clientVersion) {
    return ClientProtocol.versionID;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }

  /**
   * 内部作业类，代表本地运行的一个MapReduce作业，同时实现任务心跳协议
   * 核心职责：管理作业生命周期，调度执行Map和Reduce任务，维护作业状态与进度
   */
  private class Job extends SubjectInheritingThread implements TaskUmbilicalProtocol {
    // 系统级作业目录，对应JobTracker的系统目录
    // The job directory on the system: JobClient places job configurations here.
    // This is analogous to JobTracker's system directory.
    private Path systemJobDir;
    private Path systemJobFile;
    
    // 任务本地作业目录，对应分布式环境中任务的工作目录
    // The job directory for the task.  Analagous to a task's job directory.
    private Path localJobDir;
    private Path localJobFile;

    private JobID id;
    private JobConf job;

    private int numMapTasks;
    private int numReduceTasks;
    private float [] partialMapProgress;
    private float [] partialReduceProgress;
    private Counters [] mapCounters;
    private Counters [] reduceCounters;

    private JobStatus status;
    private List<TaskAttemptID> mapIds = Collections.synchronizedList(
        new ArrayList<TaskAttemptID>());

    private JobProfile profile;
    private FileSystem localFs;
    boolean killed = false;
    
    private LocalDistributedCacheManager localDistributedCacheManager;

    /**
     * 获取协议版本
     * @param protocol 协议名称
     * @param clientVersion 客户端版本
     * @return 协议版本ID
     */
    public long getProtocolVersion(String protocol, long clientVersion) {
      return TaskUmbilicalProtocol.versionID;
    }
    
    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHash) throws IOException {
      return ProtocolSignature.getProtocolSignature(
          this, protocol, clientVersion, clientMethodsHash);
    }

    /**
     * 构造本地作业实例，初始化目录、分布式缓存、配置，并启动作业线程
     * @param jobid 作业ID
     * @param jobSubmitDir 作业提交目录
     * @throws IOException 初始化过程IO异常
     */
    public Job(JobID jobid, String jobSubmitDir) throws IOException {
      this.systemJobDir = new Path(jobSubmitDir);
      this.systemJobFile = new Path(systemJobDir, "job.xml");
      this.id = jobid;
      JobConf conf = new JobConf(systemJobFile);
      this.localFs = FileSystem.getLocal(conf);
      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      this.localJobDir = localFs.makeQualified(new Path(
          new Path(conf.getLocalPath(jobDir), user), jobid.toString()));
      this.localJobFile = new Path(this.localJobDir, id + ".xml");

      // 初始化分布式缓存，可能更新配置
      // Manage the distributed cache.  If there are files to be copied,
      // this will trigger localFile to be re-written again.
      localDistributedCacheManager = new LocalDistributedCacheManager();
      localDistributedCacheManager.setup(conf, jobid);
      
      // 重新写入配置文件（因为分布式缓存可能更新了配置）
      // Write out configuration file.  Instead of copying it from
      // systemJobFile, we re-write it, since setup(), above, may have
      // updated it.
      OutputStream out = localFs.create(localJobFile);
      try {
        conf.writeXml(out);
      } finally {
        out.close();
      }
      this.job = new JobConf(localJobFile);

      // 如果分布式缓存有额外类路径，设置上下文类加载器
      // Job (the current object) is a Thread, so we wrap its class loader.
      if (localDistributedCacheManager.hasLocalClasspaths()) {
        setContextClassLoader(localDistributedCacheManager.makeClassLoader(
                getContextClassLoader()));
      }
      
      profile = new JobProfile(job.getUser(), id, systemJobFile.toString(), 
                               "http://localhost:8080/", job.getJobName());
      status = new JobStatus(id, 0.0f, 0.0f, JobStatus.RUNNING, 
          profile.getUser(), profile.getJobName(), profile.getJobFile(), 
          profile.getURL().toString());

      jobs.put(id, this);

      // 如果启用了溢写加密，生成加密密钥存入用户凭证
      if (CryptoUtils.isEncryptedSpillEnabled(job)) {
        try {
          int keyLen = conf.getInt(
              MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS,
              MRJobConfig
                  .DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS);
          KeyGenerator keyGen =
              KeyGenerator.getInstance(INTERMEDIATE_DATA_ENCRYPTION_ALGO);
          keyGen.init(keyLen);
          Credentials creds =
              UserGroupInformation.getCurrentUser().getCredentials();
          TokenCache.setEncryptedSpillKey(keyGen.generateKey().getEncoded(),
              creds);
          UserGroupInformation.getCurrentUser().addCredentials(creds);
        } catch (NoSuchAlgorithmException e) {
          throw new IOException("Error generating encrypted spill key", e);
        }
      }

      // 启动作业线程
      this.start();
    }

    /**
     * 可抛出异常的Runnable抽象基类，用于保存任务执行中抛出的异常
     */
    protected abstract class RunnableWithThrowable implements Runnable {
      public volatile Throwable storedException;
    }

    /**
     * Map任务可运行包装类，用于线程池执行单个Map任务
     */
    /**
     * A Runnable instance that handles a map task to be run by an executor.
     */
    protected class MapTaskRunnable extends RunnableWithThrowable {
      private final int taskId;
      private final TaskSplitMetaInfo info;
      private final JobID jobId;
      private final JobConf localConf;

      // 共享的Map输出文件引用，供后续Reduce任务获取Map输出位置
      // This is a reference to a shared object passed in by the
      // external context; this delivers state to the reducers regarding
      // where to fetch mapper outputs.
      private final Map<TaskAttemptID, MapOutputFile> mapOutputFiles;

      public MapTaskRunnable(TaskSplitMetaInfo info, int taskId, JobID jobId,
          Map<TaskAttemptID, MapOutputFile> mapOutputFiles) {
        this.info = info;
        this.taskId = taskId;
        this.mapOutputFiles = mapOutputFiles;
        this.jobId = jobId;
        this.localConf = new JobConf(job);
      }

      public void run() {
        try {
          TaskAttemptID mapId = new TaskAttemptID(new TaskID(
              jobId, TaskType.MAP, taskId), 0);
          LOG.info("Starting task: " + mapId);
          mapIds.add(mapId);
          MapTask map = new MapTask(systemJobFile.toString(), mapId, taskId,
            info.getSplitIndex(), 1);
          map.setUser(UserGroupInformation.getCurrentUser().
              getShortUserName());
          setupChildMapredLocalDirs(map, localConf);

          MapOutputFile mapOutput = new MROutputFiles();
          mapOutput.setConf(localConf);
          mapOutputFiles.put(mapId, mapOutput);

          map.setJobFile(localJobFile.toString());
          localConf.setUser(map.getUser());
          map.localizeConfiguration(localConf);
          map.setConf(localConf);
          try {
            map_tasks.getAndIncrement();
            myMetrics.launchMap(mapId);
            map.run(localConf, Job.this);
            myMetrics.completeMap(mapId);
          } finally {
            map_tasks.getAndDecrement();
          }

          LOG.info("Finishing task: " + mapId);
        } catch (Throwable e) {
          this.storedException = e;
        }
      }
    }

    /**
     * 为所有Map任务生成可运行包装实例列表
     * @param taskInfo Map切分信息数组
     * @param jobId 作业ID
     * @param mapOutputFiles Map输出文件共享映射表
     * @return Map任务可运行实例列表
     */
    /**
     * Create Runnables to encapsulate map tasks for use by the executor
     * service.
     * @param taskInfo Info about the map task splits
     * @param jobId the job id
     * @param mapOutputFiles a mapping from task attempts to output files
     * @return a List of Runnables, one per map task.
     */
    protected List<RunnableWithThrowable> getMapTaskRunnables(
        TaskSplitMetaInfo [] taskInfo, JobID jobId,
        Map<TaskAttemptID, MapOutputFile> mapOutputFiles) {

      int numTasks = 0;
      ArrayList<RunnableWithThrowable> list =
          new ArrayList<RunnableWithThrowable>();
      for (TaskSplitMetaInfo task : taskInfo) {
        list.add(new MapTaskRunnable(task, numTasks++, jobId,
            mapOutputFiles));
      }

      return list;
    }

    /**
     * Reduce任务可运行包装类，用于线程池执行单个Reduce任务
     */
    protected class ReduceTaskRunnable extends RunnableWithThrowable {
      private final int taskId;
      private final JobID jobId;
      private final JobConf localConf;

      // 共享的Map输出文件引用，用于获取所有Map任务的输出位置
      // This is a reference to a shared object passed in by the
      // external context; this delivers state to the reducers regarding
      // where to fetch mapper outputs.
      private final Map<TaskAttemptID, MapOutputFile> mapOutputFiles;

      public ReduceTaskRunnable(int taskId, JobID jobId,
          Map<TaskAttemptID, MapOutputFile> mapOutputFiles) {
        this.taskId = taskId;
        this.jobId = jobId;
        this.mapOutputFiles = mapOutputFiles;
        this.localConf = new JobConf(job);
        this.localConf.set("mapreduce.jobtracker.address", "local");
      }

      public void run() {
        try {
          TaskAttemptID reduceId = new TaskAttemptID(new TaskID(
              jobId, TaskType.REDUCE, taskId), 0);
          LOG.info("Starting task: " + reduceId);

          ReduceTask reduce = new ReduceTask(systemJobFile.toString(),
              reduceId, taskId, mapIds.size(), 1);
          reduce.setUser(UserGroupInformation.getCurrentUser().
              getShortUserName());
          setupChildMapredLocalDirs(reduce, localConf);
          reduce.setLocalMapFiles(mapOutputFiles);

          if (!Job.this.isInterrupted()) {
            reduce.setJobFile(localJobFile.toString());
            localConf.setUser(reduce.getUser());
            reduce.localizeConfiguration(localConf);
            reduce.setConf(localConf);
            try {
              reduce_tasks.getAndIncrement();
              myMetrics.launchReduce(reduce.getTaskID());
              reduce.run(localConf, Job.this);
              myMetrics.completeReduce(reduce.getTaskID());
            } finally {
              reduce_tasks.getAndDecrement();
            }

            LOG.info("Finishing task: " + reduceId);
          } else {
            throw new InterruptedException();
          }
        } catch (Throwable t) {
          // 在主线程上下文重新抛出异常，保存异常引用
          // store this to be rethrown in the initial thread context.
          this.storedException = t;
        }
      }
    }

    /**
     * 为所有Reduce任务生成可运行包装实例列表
     * @param jobId 作业ID
     * @param mapOutputFiles Map输出文件共享映射表
     * @return Reduce任务可运行实例列表
     */
    /**
     * Create Runnables to encapsulate reduce tasks for use by the executor
     * service.
     * @param jobId the job id
     * @param mapOutputFiles a mapping from task attempts to output files
     * @return a List of Runnables, one per reduce task.
     */
    protected List<RunnableWithThrowable> getReduceTaskRunnables(
        JobID jobId, Map<TaskAttemptID, MapOutputFile> mapOutputFiles) {

      int taskId = 0;
      ArrayList<RunnableWithThrowable> list =
          new ArrayList<RunnableWithThrowable>();
      for (int i = 0; i < this.numReduceTasks; i++) {
        list.add