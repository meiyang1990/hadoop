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

package org.apache.hadoop.yarn.server.resourcemanager.security;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SystemCredentialsForAppsProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * YARN ResourceManager 应用代理令牌续租服务，负责为运行中的应用续租HDFS等服务的委托令牌，
 * 并在应用完成后按需取消令牌，保障长时间运行应用的凭证有效性。
 */
@Private
@Unstable
public class DelegationTokenRenewer extends AbstractService {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(DelegationTokenRenewer.class);
  @VisibleForTesting
  public static final Text HDFS_DELEGATION_KIND =
      new Text("HDFS_DELEGATION_TOKEN");
  public static final String SCHEME = "hdfs";

  private volatile int lastEventQueueSizeLogged = 0;

  // 全局单例续租定时器（守护线程）
  private Timer renewalTimer;
  private RMContext rmContext;
  
  // 委托令牌取消线程
  private DelegationTokenCancelThread dtCancelThread =
    new DelegationTokenCancelThread();
  // 令牌续租线程池
  private ThreadPoolExecutor renewerService;

  // 按应用ID分组存储该应用需要续租的所有令牌
  private ConcurrentMap<ApplicationId, Set<DelegationTokenToRenew>> appTokens =
      new ConcurrentHashMap<ApplicationId, Set<DelegationTokenToRenew>>();

  // 全局存储所有需要续租的令牌，按令牌对象索引
  private ConcurrentMap<Token<?>, DelegationTokenToRenew> allTokens =
      new ConcurrentHashMap<Token<?>, DelegationTokenToRenew>();

  // 延迟删除映射表：存储应用ID和计划删除时间，用于日志聚合场景的令牌保活
  private final ConcurrentMap<ApplicationId, Long> delayedRemovalMap =
      new ConcurrentHashMap<ApplicationId, Long>();

  // 令牌删除延迟毫秒数
  private long tokenRemovalDelayMs;
  
  // 延迟删除线程
  private Thread delayedRemovalThread;
  // 服务状态读写锁，保障服务启动状态的线程安全
  private ReadWriteLock serviceStateLock = new ReentrantReadWriteLock();
  // 标记服务是否已启动
  private volatile boolean isServiceStarted;
  // 服务启动前缓存待处理事件的队列
  private LinkedBlockingQueue<DelegationTokenRenewerEvent> pendingEventQueue;
  
  // 是否总是在应用结束后取消委托令牌
  private boolean alwaysCancelDelegationTokens;
  // 是否启用令牌保活（用于日志聚合场景）
  private boolean tokenKeepAliveEnabled;
  // 是否启用RM代理用户权限，允许RM代表用户获取新HDFS令牌
  private boolean hasProxyUserPrivileges;
  // 凭证剩余有效时间阈值，低于该值时申请新令牌
  private long credentialsValidTimeRemaining;
  // 续租线程超时时间
  private long tokenRenewerThreadTimeout;
  // 续租失败重试间隔
  private long tokenRenewerThreadRetryInterval;
  // 续租失败最大重试次数
  private int tokenRenewerThreadRetryMaxAttempts;
  // 存储所有异步续租任务的Future，用于超时跟踪
  private final LinkedBlockingQueue<DelegationTokenRenewerFuture> futures =
      new LinkedBlockingQueue<>();
  // 是否启用线程池跟踪器，用于超时处理
  private boolean delegationTokenRenewerPoolTrackerFlag = true;

  // 系统凭证剩余有效时间配置项，不建议终端用户修改
  public static final String RM_SYSTEM_CREDENTIALS_VALID_TIME_REMAINING =
      YarnConfiguration.RM_PREFIX + "system-credentials.valid-time-remaining";
  public static final long DEFAULT_RM_SYSTEM_CREDENTIALS_VALID_TIME_REMAINING =
      10800000; // 3h

  /**
   * 构造委托令牌续租服务
   */
  public DelegationTokenRenewer() {
    super(DelegationTokenRenewer.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 读取配置：是否总是取消令牌
    this.alwaysCancelDelegationTokens =
        conf.getBoolean(YarnConfiguration.RM_DELEGATION_TOKEN_ALWAYS_CANCEL,
            YarnConfiguration.DEFAULT_RM_DELEGATION_TOKEN_ALWAYS_CANCEL);
    // 读取配置：是否启用代理用户权限
    this.hasProxyUserPrivileges =
        conf.getBoolean(YarnConfiguration.RM_PROXY_USER_PRIVILEGES_ENABLED,
          YarnConfiguration.DEFAULT_RM_PROXY_USER_PRIVILEGES_ENABLED);
    // 读取配置：是否启用日志聚合，关联启用令牌保活
    this.tokenKeepAliveEnabled =
        conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
            YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED);
    // 读取配置：令牌删除延迟时间
    this.tokenRemovalDelayMs =
        conf.getInt(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);
    // 读取配置：系统凭证剩余有效时间阈值
    this.credentialsValidTimeRemaining =
        conf.getLong(RM_SYSTEM_CREDENTIALS_VALID_TIME_REMAINING,
          DEFAULT_RM_SYSTEM_CREDENTIALS_VALID_TIME_REMAINING);
    // 读取配置：续租线程超时时间
    tokenRenewerThreadTimeout =
        conf.getTimeDuration(YarnConfiguration.RM_DT_RENEWER_THREAD_TIMEOUT,
            YarnConfiguration.DEFAULT_RM_DT_RENEWER_THREAD_TIMEOUT,
            TimeUnit.MILLISECONDS);
    // 读取配置：重试间隔
    tokenRenewerThreadRetryInterval = conf.getTimeDuration(
        YarnConfiguration.RM_DT_RENEWER_THREAD_RETRY_INTERVAL,
        YarnConfiguration.DEFAULT_RM_DT_RENEWER_THREAD_RETRY_INTERVAL,
        TimeUnit.MILLISECONDS);
    // 读取配置：最大重试次数
    tokenRenewerThreadRetryMaxAttempts =
        conf.getInt(YarnConfiguration.RM_DT_RENEWER_THREAD_RETRY_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_DT_RENEWER_THREAD_RETRY_MAX_ATTEMPTS);
    // 设置本地密钥管理器和服务地址，允许RM直接短路处理自身令牌操作
    setLocalSecretManagerAndServiceAddr();
    // 创建续租线程池
    renewerService = createNewThreadPoolService(conf);
    // 初始化待处理事件队列
    pendingEventQueue = new LinkedBlockingQueue<DelegationTokenRenewerEvent>();
    // 创建续租定时器
    renewalTimer = new Timer(true);
    super.serviceInit(conf);
  }

  /**
   * 创建委托令牌续租线程池
   * @param conf 配置对象
   * @return 初始化完成的线程池
   */
  protected ThreadPoolExecutor createNewThreadPoolService(Configuration conf) {
    int nThreads = conf.getInt(
        YarnConfiguration.RM_DELEGATION_TOKEN_RENEWER_THREAD_COUNT,
        YarnConfiguration.DEFAULT_RM_DELEGATION_TOKEN_RENEWER_THREAD_COUNT);

    ThreadFactory tf = new ThreadFactoryBuilder()
        .setNameFormat("DelegationTokenRenewer #%d")
        .build();
    ThreadPoolExecutor pool =
        new ThreadPoolExecutor(nThreads, nThreads, 3L,
            TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
    pool.setThreadFactory(tf);
    // 允许核心线程超时退出，节省资源
    pool.allowCoreThreadTimeOut(true);
    return pool;
  }

  /**
   * 设置本地RMDelegationToken密钥管理器和服务地址，允许RM直接处理自身令牌的续租操作，无需远程调用
   */
  private void setLocalSecretManagerAndServiceAddr() {
    RMDelegationTokenIdentifier.Renewer.setSecretManager(rmContext
      .getRMDelegationTokenSecretManager(), rmContext.getClientRMService()
      .getBindAddress());
  }

  @Override
  protected void serviceStart() throws Exception {
    // 启动令牌取消线程
    dtCancelThread.start();
    // 如果启用令牌保活，启动延迟删除线程
    if (tokenKeepAliveEnabled) {
      delayedRemovalThread =
          new SubjectInheritingThread(new DelayedTokenRemovalRunnable(getConfig()),
              "DelayedTokenCanceller");
      delayedRemovalThread.start();
    }

    // 重新设置本地密钥管理器和服务地址
    setLocalSecretManagerAndServiceAddr();
    // 更新服务启动状态，加写锁
    serviceStateLock.writeLock().lock();
    isServiceStarted = true;
    serviceStateLock.writeLock().unlock();

    // 如果启用线程池跟踪，启动跟踪线程
    if (delegationTokenRenewerPoolTrackerFlag) {
      renewerService.submit(new DelegationTokenRenewerPoolTracker());
    }

    // 处理服务启动前缓存的所有待处理事件
    while(!pendingEventQueue.isEmpty()) {
      processDelegationTokenRenewerEvent(pendingEventQueue.take());
    }
    super.serviceStart();
  }

  /**
   * 处理委托令牌续租事件，服务未启动时缓存事件，启动后提交给线程池异步处理
   * @param evt 待处理的续租事件
   */
  private void processDelegationTokenRenewerEvent(
      DelegationTokenRenewerEvent evt) {
    serviceStateLock.readLock().lock();
    try {
      if (isServiceStarted) {
        // 服务已启动，提交异步任务处理
        Future<?> future =
            renewerService.submit(new DelegationTokenRenewerRunnable(evt));
        futures.add(new DelegationTokenRenewerFuture(evt, future));
      } else {
        // 服务未启动，缓存事件
        pendingEventQueue.add(evt);
        int qSize = pendingEventQueue.size();
        // 每1000个事件打印一次队列大小日志
        if (qSize != 0 && qSize % 1000 == 0
            && lastEventQueueSizeLogged != qSize) {
          lastEventQueueSizeLogged = qSize;
          LOG.info("Size of pending " +
              "DelegationTokenRenewerEvent queue is " + qSize);
        }
      }
    } finally {
      serviceStateLock.readLock().unlock();
    }
  }

  @Override
  protected void serviceStop() {
    // 停止续租定时器
    if (renewalTimer != null) {
      renewalTimer.cancel();
    }
    // 清空令牌存储
    appTokens.clear();
    allTokens.clear();

    // 更新服务状态，关闭线程池
    serviceStateLock.writeLock().lock();
    try {
      isServiceStarted = false;
      this.renewerService.shutdown();
    } finally {
      serviceStateLock.writeLock().unlock();
    }

    // 停止令牌取消线程
    dtCancelThread.interrupt();
    try {
      dtCancelThread.join(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    // 如果启用了保活，停止延迟删除线程
    if (tokenKeepAliveEnabled && delayedRemovalThread != null) {
      delayedRemovalThread.interrupt();
      try {
        delayedRemovalThread.join(1000);
      } catch (InterruptedException e) {
        LOG.info("Interrupted while joining on delayed removal thread.", e);
      }
    }
  }

  /**
   * 存储待续租委托令牌信息的内部类，记录令牌、关联应用、配置、过期时间等信息
   */
  @VisibleForTesting
  protected class DelegationTokenToRenew {
    public final Token<?> token;
    public final Collection<ApplicationId> referringAppIds;
    public final Configuration conf;
    public long expirationDate;
    public RenewalTimerTask timerTask;
    public volatile boolean shouldCancelAtEnd;
    public long maxDate;
    public String user;

    public DelegationTokenToRenew(Collection<ApplicationId> applicationIds,
        Token<?> token,
        Configuration conf, long expirationDate, boolean shouldCancelAtEnd,
        String user) {
      this.token = token;
      this.user = user;
      // 如果是HDFS委托令牌，读取最大过期时间
      if (token.getKind().equals(HDFS_DELEGATION_KIND)) {
        try {
          AbstractDelegationTokenIdentifier identifier =
              (AbstractDelegationTokenIdentifier) token.decodeIdentifier();
          maxDate = identifier.getMaxDate();
        } catch (IOException e) {
          throw new YarnRuntimeException(e);
        }
      }
      this.referringAppIds = Collections.synchronizedSet(
          new HashSet<ApplicationId>(applicationIds));
      this.conf = conf;
      this.expirationDate = expirationDate;
      this.timerTask = null;
      // 合并配置的总是取消设置
      this.shouldCancelAtEnd = shouldCancelAtEnd | alwaysCancelDelegationTokens;
    }
    
    public void setTimerTask(RenewalTimerTask tTask) {
      timerTask = tTask;
    }

    @VisibleForTesting
    public void cancelTimer() {
      if (timerTask != null) {
        timerTask.cancel();
      }
    }

    @VisibleForTesting
    public boolean isTimerCancelled() {
      return (timerTask != null) && timerTask.cancelled.get();
    }

    @Override
    public String toString() {
      return token + ";exp=" + expirationDate + "; apps=" + referringAppIds;
    }
    
    @Override
    public boolean equals(Object obj) {
      return obj instanceof DelegationTokenToRenew &&
        token.equals(((DelegationTokenToRenew)obj).token);
    }
    
    @Override
    public int hashCode() {
      return token.hashCode();
    }
  }
  
  
  /**
   * 专门用于异步取消委托令牌的后台线程，通过队列接收取消请求逐个处理
   */
  private static class DelegationTokenCancelThread extends SubjectInheritingThread {
    /**
     * 存储待取消令牌和对应配置的内部类
     */
    private static class TokenWithConf {
      Token<?> token;
      Configuration conf;
      TokenWithConf(Token<?> token, Configuration conf) {
        this.token = token;
        this.conf = conf;
      }
    }
    // 待取消令牌队列
    private LinkedBlockingQueue<TokenWithConf> queue =  
      new LinkedBlockingQueue<TokenWithConf>();
     
    public DelegationTokenCancelThread() {
      super("Delegation Token Canceler");
      setDa