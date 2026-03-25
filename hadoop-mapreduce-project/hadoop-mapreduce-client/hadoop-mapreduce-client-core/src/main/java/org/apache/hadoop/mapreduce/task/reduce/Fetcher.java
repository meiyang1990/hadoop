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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.SecretKey;
import javax.net.ssl.HttpsURLConnection;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.security.IntermediateEncryptedStream;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * MapReduce Shuffle阶段拉取线程，负责从各个Map节点拉取Map输出数据
 * 每个Fetcher线程负责从一个Map主机拉取多个Map任务输出，交给MergeManager合并
 */
@VisibleForTesting
public class Fetcher<K, V> extends SubjectInheritingThread {
  
  private static final Logger LOG = LoggerFactory.getLogger(Fetcher.class);
  
  /** Number of ms before timing out a copy. */
  private static final int DEFAULT_STALLED_COPY_TIMEOUT = 3 * 60 * 1000;
  
  /** Basic/unit connection timeout (in milliseconds). */
  private final static int UNIT_CONNECT_TIMEOUT = 60 * 1000;
  
  /* Default read timeout (in milliseconds) */
  private final static int DEFAULT_READ_TIMEOUT = 3 * 60 * 1000;

  // This should be kept in sync with ShuffleHandler.FETCH_RETRY_DELAY.
  private static final long FETCH_RETRY_DELAY_DEFAULT = 1000L;
  static final int TOO_MANY_REQ_STATUS_CODE = 429;
  private static final String FETCH_RETRY_AFTER_HEADER = "Retry-After";

  protected final Reporter reporter;
  @VisibleForTesting
  public enum ShuffleErrors{IO_ERROR, WRONG_LENGTH, BAD_ID, WRONG_MAP,
                                    CONNECTION, WRONG_REDUCE}

  @VisibleForTesting
  public final static String SHUFFLE_ERR_GRP_NAME = "Shuffle Errors";
  private final JobConf jobConf;
  private final Counters.Counter connectionErrs;
  private final Counters.Counter ioErrs;
  private final Counters.Counter wrongLengthErrs;
  private final Counters.Counter badIdErrs;
  private final Counters.Counter wrongMapErrs;
  private final Counters.Counter wrongReduceErrs;
  protected final MergeManager<K, V> merger;
  protected final ShuffleSchedulerImpl<K, V> scheduler;
  protected final ShuffleClientMetrics metrics;
  protected final ExceptionReporter exceptionReporter;
  protected final int id;
  private static final AtomicInteger NEXT_ID = new AtomicInteger(0);
  protected final int reduce;
  
  private final int connectionTimeout;
  private final int readTimeout;
  
  private final int fetchRetryTimeout;
  private final int fetchRetryInterval;
  
  private final boolean fetchRetryEnabled;
  
  private final SecretKey shuffleSecretKey;

  protected HttpURLConnection connection;
  private volatile boolean stopped = false;
  
  // Initiative value is 0, which means it hasn't retried yet.
  private long retryStartTime = 0;

  private static boolean sslShuffle;
  private static SSLFactory sslFactory;

  /**
   * 构造Fetcher线程实例
   * @param job 作业配置
   * @param reduceId 当前Reduce任务尝试ID
   * @param scheduler Shuffle调度器
   * @param merger 合并管理器
   * @param reporter 任务报告器
   * @param metrics Shuffle客户端指标
   * @param exceptionReporter 异常报告器
   * @param shuffleKey Shuffle加密密钥
   */
  public Fetcher(JobConf job, TaskAttemptID reduceId, 
                 ShuffleSchedulerImpl<K, V> scheduler, MergeManager<K, V> merger,
                 Reporter reporter, ShuffleClientMetrics metrics,
                 ExceptionReporter exceptionReporter, SecretKey shuffleKey) {
    this(job, reduceId, scheduler, merger, reporter, metrics,
        exceptionReporter, shuffleKey, NEXT_ID.incrementAndGet());
  }

  /**
   * 构造Fetcher线程实例（带自定义ID，用于测试）
   * @param job 作业配置
   * @param reduceId 当前Reduce任务尝试ID
   * @param scheduler Shuffle调度器
   * @param merger 合并管理器
   * @param reporter 任务报告器
   * @param metrics Shuffle客户端指标
   * @param exceptionReporter 异常报告器
   * @param shuffleKey Shuffle加密密钥
   * @param id Fetcher线程ID
   */
  @VisibleForTesting
  Fetcher(JobConf job, TaskAttemptID reduceId, 
                 ShuffleSchedulerImpl<K, V> scheduler, MergeManager<K, V> merger,
                 Reporter reporter, ShuffleClientMetrics metrics,
                 ExceptionReporter exceptionReporter, SecretKey shuffleKey,
                 int id) {
    this.jobConf = job;
    this.reporter = reporter;
    this.scheduler = scheduler;
    this.merger = merger;
    this.metrics = metrics;
    this.exceptionReporter = exceptionReporter;
    this.id = id;
    this.reduce = reduceId.getTaskID().getId();
    this.shuffleSecretKey = shuffleKey;
    ioErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.IO_ERROR.toString());
    wrongLengthErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.WRONG_LENGTH.toString());
    badIdErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.BAD_ID.toString());
    wrongMapErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.WRONG_MAP.toString());
    connectionErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.CONNECTION.toString());
    wrongReduceErrs = reporter.getCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.WRONG_REDUCE.toString());
    
    this.connectionTimeout = 
      job.getInt(MRJobConfig.SHUFFLE_CONNECT_TIMEOUT,
                 DEFAULT_STALLED_COPY_TIMEOUT);
    this.readTimeout = 
      job.getInt(MRJobConfig.SHUFFLE_READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
    
    this.fetchRetryInterval = job.getInt(MRJobConfig.SHUFFLE_FETCH_RETRY_INTERVAL_MS,
        MRJobConfig.DEFAULT_SHUFFLE_FETCH_RETRY_INTERVAL_MS);
    
    this.fetchRetryTimeout = job.getInt(MRJobConfig.SHUFFLE_FETCH_RETRY_TIMEOUT_MS, 
        DEFAULT_STALLED_COPY_TIMEOUT);
    
    boolean shuffleFetchEnabledDefault = job.getBoolean(
        YarnConfiguration.NM_RECOVERY_ENABLED, 
        YarnConfiguration.DEFAULT_NM_RECOVERY_ENABLED);
    this.fetchRetryEnabled = job.getBoolean(
        MRJobConfig.SHUFFLE_FETCH_RETRY_ENABLED, 
        shuffleFetchEnabledDefault);
    
    setName("fetcher#" + id);
    setDaemon(true);

    synchronized (Fetcher.class) {
      // 从配置读取SSL Shuffle开关
      sslShuffle = job.getBoolean(MRConfig.SHUFFLE_SSL_ENABLED_KEY,
                                  MRConfig.SHUFFLE_SSL_ENABLED_DEFAULT);
      // 初始化SSL工厂（仅第一次执行）
      if (sslShuffle && sslFactory == null) {
        sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, job);
        try {
          sslFactory.init();
        } catch (Exception ex) {
          sslFactory.destroy();
          throw new RuntimeException(ex);
        }
      }
    }
  }
  
  /**
   * Fetcher线程主工作循环，持续从Map主机拉取Map输出直到停止
   */
  public void work() {
    try {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        MapHost host = null;
        try {
          // 等待合并资源空闲，资源不足时阻塞
          merger.waitForResource();

          // 从调度器获取一个待拉取的Map主机
          host = scheduler.getHost();
          metrics.threadBusy();

          // 从该主机拉取所有Map输出
          copyFromHost(host);
        } finally {
          if (host != null) {
            // 释放主机锁，允许其他Fetcher拉取该主机
            scheduler.freeHost(host);
            metrics.threadFree();            
          }
        }
      }
    } catch (InterruptedException ie) {
      return;
    } catch (Throwable t) {
      exceptionReporter.reportException(t);
    }
  }

  @Override
  public void interrupt() {
    try {
      closeConnection();
    } finally {
      super.interrupt();
    }
  }

  /**
   * 安全关闭Fetcher线程，释放连接和资源
   * @throws InterruptedException 中断异常
   */
  public void shutDown() throws InterruptedException {
    this.stopped = true;
    interrupt();
    try {
      join(5000);
    } catch (InterruptedException ie) {
      LOG.warn("Got interrupt while joining " + getName(), ie);
    }
    if (sslFactory != null) {
      sslFactory.destroy();
    }
  }

  /**
   * 打开HTTP连接，支持SSL加密
   * @param url 目标URL
   * @throws IOException 打开连接失败时抛出
   */
  @VisibleForTesting
  protected synchronized void openConnection(URL url)
      throws IOException {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    if (sslShuffle) {
      HttpsURLConnection httpsConn = (HttpsURLConnection) conn;
      try {
        httpsConn.setSSLSocketFactory(sslFactory.createSSLSocketFactory());
      } catch (GeneralSecurityException ex) {
        throw new IOException(ex);
      }
      httpsConn.setHostnameVerifier(sslFactory.getHostnameVerifier());
    }
    connection = conn;
  }

  /**
   * 关闭当前HTTP连接
   */
  protected synchronized void closeConnection() {
    // Note that HttpURLConnection::disconnect() doesn't trash the object.
    // connect() attempts to reconnect in a loop, possibly reversing this
    if (connection != null) {
      connection.disconnect();
    }
  }

  /**
   * 连接中止处理，将剩余未拉取的任务放回调度队列并关闭连接
   * @param host Map主机
   * @param remaining 剩余未拉取的Map任务列表
   */
  private void abortConnect(MapHost host, Set<TaskAttemptID> remaining) {
    for (TaskAttemptID left : remaining) {
      scheduler.putBackKnownMapOutput(host, left);
    }
    closeConnection();
  }

  /**
   * 打开Shuffle连接，处理重试和异常
   * @param host Map主机
   * @param remaining 待拉取Map任务
   * @param url 目标URL
   * @return 数据输入流，连接失败返回null
   */
  private DataInputStream openShuffleUrl(MapHost host,
      Set<TaskAttemptID> remaining, URL url) {
    DataInputStream input = null;

    try {
      // 带重试建立连接并验证
      setupConnectionsWithRetry(url);
      if (stopped) {
        abortConnect(host, remaining);
      } else {
        input = new DataInputStream(connection.getInputStream());
      }
    } catch (TryAgainLaterException te) {
      // 服务端限流要求稍后重试，惩罚主机后返回
      LOG.warn("Connection rejected by the host " + te.host +
          ". Will retry later.");
      scheduler.penalize(host, te.backoff);
    } catch (IOException ie) {
      boolean connectExcpt = ie instanceof ConnectException;
      ioErrs.increment(1);
      LOG.warn("Failed to connect to " + host + " with " + remaining.size() +
               " map outputs", ie);

      // If connect did not succeed, just mark all the maps as failed,
      // indirectly penalizing the host
      scheduler.hostFailed(host.getHostName());
      for(TaskAttemptID left: remaining) {
        scheduler.copyFailed(left, host, false, connectExcpt);
      }
    }

    return input;
  }

  /**
   * 从指定Map主机拉取所有该主机上的Map输出数据
   * @param host 需要拉取数据的Map主机
   * @throws IOException 拉取过程中发生IO异常时抛出
   */
  @VisibleForTesting
  protected void copyFromHost(MapHost host) throws IOException {
    // 新主机拉取，重置重试开始时间
    retryStartTime = 0;
    // 从调度器获取该主机上所有需要拉取的Map任务列表
    List<TaskAttemptID> maps = scheduler.getMapsForHost(host);
    
    // Sanity check to catch hosts with only 'OBSOLETE' maps, 
    // especially at the tail of large jobs
    if (maps.size() == 0) {
      return;
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Fetcher " + id + " going to fetch from " + host + " for: " + maps);
    }
    
    // 剩余待拉取的Map任务集合
    Set<TaskAttemptID> remaining = new HashSet<TaskAttemptID>(maps);
    
    // 构造请求URL，包含所有待拉取Map任务ID
    URL url = getMapOutputURL(host, maps);
    DataInputStream input = null;
    
    try {
      // 打开连接获取输入流
      input = openShuffleUrl(host, remaining, url);
      if (input == null) {
        return;
      }

      // 循环拉取所有Map输出，遇到错误则退出
      TaskAttemptID[] failedTasks = null;
      while (!remaining.isEmpty() && failedTasks == null) {
        try {
          // 拷贝单个Map输出到本地
          failedTasks = copyMapOutput(host, input, remaining, fetchRetryEnabled);
        } catch (IOException e) {
          IOUtils.cleanupWithLogger(LOG, input);
          // 连接被NM断开，重新建立连接
          connection.disconnect();
          // 仅对剩余任务重新构造URL
          url = getMapOutputURL(host, remaining);
          input = openShuffleUrl(host, remaining, url);
          if (input == null) {
            return;
          }
        }
      }
      
      // 处理失败的Map任务
      if(failedTasks != null && failedTasks.length > 0) {
        LOG.warn("copyMapOutput failed for tasks "+Arrays.toString(failedTasks));
        scheduler.hostFailed(host.getHostName());
        for(TaskAttemptID left: failedTasks) {
          scheduler.copyFailed(left, host, true, false);
        }
      }

      // 完整性校验：必须拉取到所有预期Map输出
      if (failedTasks == null && !remaining.isEmpty()) {
        throw new IOException("server didn't return all expected map outputs: "
            + remaining.size() + " left.");
      }
      input.close();
      input = null;
    } finally {
      // 清理资源
      if (input != null) {
        IOUtils.cleanupWithLogger(LOG, input);
        input = null;
      }
      // 将剩余未拉取的任务放回调度队列
      for (TaskAttemptID left : remaining) {
        scheduler.putBackKnownMapOutput(host, left);
      }
    }
  }

  /**
   * 带重试建立连接，完成安全验证
   * @param url 目标URL
   * @throws IOException 连接或验证失败时抛出
   */
  private void setupConnectionsWithRetry(URL url) throws IOException {
    openConnectionWithRetry(url);
    if (stopped) {
      return;
    }
      
    // 计算URL的HMAC哈希用于安全验证
    String msgToEncode = SecureShuffleUtils.buildMsgFrom(url);
    String encHash = SecureShuffleUtils.hashFromString(msgToEncode,