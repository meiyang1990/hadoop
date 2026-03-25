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

package org.apache.hadoop.mapreduce.v2.app.rm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster.RunningAppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明: MapReduce ApplicationMaster与YARN ResourceManager通信基类，负责向RM注册/反注册应用，并定期发送心跳维持会话
 * 核心职责: 封装与RM通信的基础逻辑，提供心跳线程管理，子类可扩展实现具体的容器分配请求逻辑
 */
public abstract class RMCommunicator extends AbstractService
    implements RMHeartbeatHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(RMCommunicator.class);
  // 心跳轮询间隔，单位毫秒
  private int rmPollInterval;
  protected ApplicationId applicationId;
  private final AtomicBoolean stopped;
  protected Thread allocatorThread;
  @SuppressWarnings("rawtypes")
  protected EventHandler eventHandler;
  protected ApplicationMasterProtocol scheduler;
  private final ClientService clientService;
  private Resource maxContainerCapability;
  protected Map<ApplicationAccessType, String> applicationACLs;
  private volatile long lastHeartbeatTime;
  private ConcurrentLinkedQueue<Runnable> heartbeatCallbacks;

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  private final AppContext context;
  private Job job;
  // 是否已经收到终止信号（如SIGTERM）
  protected volatile boolean isSignalled = false;
  private volatile boolean shouldUnregister = true;
  private boolean isApplicationMasterRegistered = false;

  private EnumSet<SchedulerResourceTypes> schedulerResourceTypes;

  /**
   * 构造RMCommunicator实例，初始化基础组件
   * @param clientService AM客户端服务实例，用于获取服务绑定地址
   * @param context MapReduce AppMaster上下文，保存应用全局状态信息
   */
  public RMCommunicator(ClientService clientService, AppContext context) {
    super("RMCommunicator");
    this.clientService = clientService;
    this.context = context;
    this.eventHandler = context.getEventHandler();
    this.applicationId = context.getApplicationID();
    this.stopped = new AtomicBoolean(false);
    this.heartbeatCallbacks = new ConcurrentLinkedQueue<Runnable>();
    this.schedulerResourceTypes = EnumSet.of(SchedulerResourceTypes.MEMORY);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    // 从配置加载心跳间隔，使用默认值作为兜底
    rmPollInterval =
        conf.getInt(MRJobConfig.MR_AM_TO_RM_HEARTBEAT_INTERVAL_MS,
            MRJobConfig.DEFAULT_MR_AM_TO_RM_HEARTBEAT_INTERVAL_MS);
  }

  @Override
  protected void serviceStart() throws Exception {
    // 创建与ResourceManager通信的代理对象
    scheduler= createSchedulerProxy();
    // 获取当前作业ID和作业实例
    JobID id = TypeConverter.fromYarn(this.applicationId);
    JobId jobId = TypeConverter.toYarn(id);
    job = context.getJob(jobId);
    // 向RM注册当前ApplicationMaster
    register();
    // 启动心跳分配线程
    startAllocatorThread();
    super.serviceStart();
  }

  /**
   * 获取应用上下文实例
   * @return 应用上下文
   */
  protected AppContext getContext() {
    return context;
  }

  /**
   * 获取当前运行的作业实例
   * @return 当前作业实例
   */
  protected Job getJob() {
    return job;
  }

  /**
   * 获取当前应用的整体进度，用于向RM汇报
   * @return 应用进度，范围0-1
   */
  protected float getApplicationProgress() {
    // For now just a single job. In future when we have a DAG, we need an
    // aggregate progress.
    return this.job.getProgress();
  }

  /**
   * 向YARN ResourceManager注册当前ApplicationMaster，完成注册后获取集群信息
   */
  protected void register() {
    //Register
    InetSocketAddress serviceAddr = null;
    if (clientService != null ) {
      serviceAddr = clientService.getBindAddress();
    }
    try {
      // 构造注册请求对象
      RegisterApplicationMasterRequest request =
        recordFactory.newRecordInstance(RegisterApplicationMasterRequest.class);
      if (serviceAddr != null) {
        // 设置AM的服务地址和端口，方便RM转发客户端请求
        request.setHost(serviceAddr.getHostName());
        request.setRpcPort(serviceAddr.getPort());
        // 设置AM追踪页面URL
        request.setTrackingUrl(MRWebAppUtil
            .getAMWebappScheme(getConfig())
            + serviceAddr.getHostName() + ":" + clientService.getHttpPort());
      }
      // 发送注册请求到RM
      RegisterApplicationMasterResponse response =
        scheduler.registerApplicationMaster(request);
      // 标记注册成功
      isApplicationMasterRegistered = true;
      // 获取RM支持的最大容器资源能力，保存到集群信息
      maxContainerCapability = response.getMaximumResourceCapability();
      this.context.getClusterInfo().setMaxContainerCapability(
          maxContainerCapability);
      // 安全启用情况下，保存客户端到AM的令牌密钥
      if (UserGroupInformation.isSecurityEnabled()) {
        setClientToAMToken(response.getClientToAMTokenMasterKey());        
      }
      // 获取应用访问控制列表
      this.applicationACLs = response.getApplicationACLs();
      LOG.info("maxContainerCapability: " + maxContainerCapability);
      String queue = response.getQueue();
      LOG.info("queue: " + queue);
      // 将队列名称保存到作业
      job.setQueueName(queue);
      // 添加RM支持的资源类型
      this.schedulerResourceTypes.addAll(response.getSchedulerResourceTypes());
    } catch (Exception are) {
      LOG.error("Exception while registering", are);
      throw new YarnRuntimeException(are);
    }
  }

  private void setClientToAMToken(ByteBuffer clientToAMTokenMasterKey) {
    byte[] key = clientToAMTokenMasterKey.array();
    // 将密钥保存到AM的密钥管理器，用于客户端认证
    context.getClientToAMTokenSecretManager().setMasterKey(key);
  }

  /**
   * 尝试向RM反注册应用，处理反注册失败场景
   */
  protected void unregister() {
    try {
      doUnregistration();
    } catch(Exception are) {
      LOG.error("Exception while unregistering ", are);
      // 反注册失败，重新判断是否为最后一次AM重试，处理故障转移场景
      RunningAppContext raContext = (RunningAppContext) context;
      raContext.resetIsLastAMRetry();
    }
  }

  /**
   * 执行向RM反注册应用的实际逻辑，上报应用最终状态
   * @throws YarnException YARN服务异常
   * @throws IOException IO通信异常
   * @throws InterruptedException 线程中断异常
   */
  @VisibleForTesting
  protected void doUnregistration()
      throws YarnException, IOException, InterruptedException {
    // 根据作业最终状态确定YARN应用最终状态
    FinalApplicationStatus finishState = FinalApplicationStatus.UNDEFINED;
    JobImpl jobImpl = (JobImpl)job;
    if (jobImpl.getInternalState() == JobStateInternal.SUCCEEDED) {
      finishState = FinalApplicationStatus.SUCCEEDED;
    } else if (jobImpl.getInternalState() == JobStateInternal.KILLED
        || (jobImpl.getInternalState() == JobStateInternal.RUNNING && isSignalled)) {
      finishState = FinalApplicationStatus.KILLED;
    } else if (jobImpl.getInternalState() == JobStateInternal.FAILED
        || jobImpl.getInternalState() == JobStateInternal.ERROR) {
      finishState = FinalApplicationStatus.FAILED;
    }
    // 拼接作业诊断信息
    StringBuilder sb = new StringBuilder();
    for (String s : job.getDiagnostics()) {
      sb.append(s).append("\n");
    }
    LOG.info("Setting job diagnostics to " + sb.toString());

    // 获取作业历史服务器URL
    String historyUrl = context.getHistoryUrl();
    LOG.info("History url is " + historyUrl);
    // 构造反注册请求
    FinishApplicationMasterRequest request =
        FinishApplicationMasterRequest.newInstance(finishState,
          sb.toString(), historyUrl);
    try {
      // 循环重试直到反注册成功
      while (true) {
        FinishApplicationMasterResponse response =
            scheduler.finishApplicationMaster(request);
        if (response.getIsUnregistered()) {
          // 反注册成功，标记状态，通知上下文
          RunningAppContext raContext = (RunningAppContext) context;
          raContext.markSuccessfulUnregistration();
          break;
        }
        LOG.info("Waiting for application to be successfully unregistered.");
        Thread.sleep(rmPollInterval);
      }
    } catch (ApplicationMasterNotRegisteredException e) {
      // RM重启或故障转移后丢失了注册信息，重新注册后再反注册
      register();
      doUnregistration();
    }
  }

  /**
   * 获取RM支持的最大容器资源能力
   * @return 最大容器资源
   */
  protected Resource getMaxContainerCapability() {
    return maxContainerCapability;
  }

  @Override
  protected void serviceStop() throws Exception {
    // 原子标记停止，避免重复停止
    if (stopped.getAndSet(true)) {
      // return if already stopped
      return;
    }
    // 中断并等待分配线程退出
    if (allocatorThread != null) {
      allocatorThread.interrupt();
      try {
        allocatorThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("InterruptedException while stopping", ie);
      }
    }
    // 如果已注册且需要反注册，执行反注册
    if (isApplicationMasterRegistered && shouldUnregister) {
      unregister();
    }
    super.serviceStop();
  }

  /**
   * 心跳线程执行类，定期向RM发送心跳
   */
  @VisibleForTesting
  public class AllocatorRunnable implements Runnable {
    @Override
    public void run() {
      while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
        try {
          // 间隔心跳后再发送
          Thread.sleep(rmPollInterval);
          try {
            // 执行心跳，子类实现具体逻辑
            heartbeat();
          } catch (RMContainerAllocationException e) {
            LOG.error("Error communicating with RM: " + e.getMessage() , e);
            return;
          } catch (Exception e) {
            LOG.error("ERROR IN CONTACTING RM. ", e);
            continue;
            // TODO: for other exceptions
          }

          // 更新最后一次心跳时间
          lastHeartbeatTime = context.getClock().getTime();
          // 执行所有等待在下一次心跳的回调
          executeHeartbeatCallbacks();
        } catch (InterruptedException e) {
          if (!stopped.get()) {
            LOG.warn("Allocated thread interrupted. Returning.");
          }
          return;
        }
      }
    }
  }

  /**
   * 启动心跳分配线程，定期向RM发送心跳
   */
  protected void startAllocatorThread() {
    allocatorThread = new SubjectInheritingThread(new AllocatorRunnable());
    allocatorThread.setName("RMCommunicator Allocator");
    allocatorThread.start();
  }

  /**
   * 创建与RM ApplicationMasterProtocol服务的代理对象
   * @return RM服务代理实例
   */
  protected ApplicationMasterProtocol createSchedulerProxy() {
    final Configuration conf = getConfig();

    try {
      return ClientRMProxy.createRMProxy(conf, ApplicationMasterProtocol.class);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }

  /**
   * 抽象心跳方法，子类实现具体的心跳和容器请求逻辑
   * @throws Exception 通信或处理异常
   */
  protected abstract void heartbeat() throws Exception;

  /**
   * 执行所有排队等待在下一次心跳后执行的回调
   */
  private void executeHeartbeatCallbacks() {
    Runnable callback = null;
    while ((callback = heartbeatCallbacks.poll()) != null) {
      callback.run();
    }
  }

  @Override
  public long getLastHeartbeatTime() {
    return lastHeartbeatTime;
  }

  @Override
  public void runOnNextHeartbeat(Runnable callback) {
    // 将回调添加到队列，在下一次心跳完成后执行
    heartbeatCallbacks.add(callback);
  }

  /**
   * 设置停止时是否需要向RM反注册
   * @param shouldUnregister true表示需要反注册，false表示不需要
   */
  public void setShouldUnregister(boolean shouldUnregister) {
    this.shouldUnregister = shouldUnregister;
    LOG.info("RMCommunicator notified that shouldUnregistered is: " 
        + shouldUnregister);
  }
  
  /**
   * 设置是否已经收到终止信号
   * @param isSignalled true表示已收到终止信号
   */
  public void setSignalled(boolean isSignalled) {
    this.isSignalled = isSignalled;
    LOG.info("RMCommunicator notified that isSignalled is: " 
        + isSignalled);
  }

  /**
   * 检查ApplicationMaster是否已经成功注册到RM
   * @return true表示已注册
   */
  @VisibleForTesting
  protected boolean isApplicationMasterRegistered() {
    return isApplicationMasterRegistered;
  }

  /**
   * 获取RM调度器支持的资源类型集合
   * @return 支持的资源类型枚举集合
   */
  public EnumSet<SchedulerResourceTypes> getSchedulerResourceTypes() {
    return schedulerResourceTypes;
  }
}