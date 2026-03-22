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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.SortedRanges.Range;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.util.MRJobConfUtil;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.TaskHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptFailEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.app.rm.preemption.AMPreemptionPolicy;
import org.apache.hadoop.mapreduce.v2.app.security.authorize.MRAMPolicyProvider;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * 文件说明：MapReduce ApplicationMaster端任务尝试监听器实现，处理旧MapReduce API任务尝试与YARN的交互
 * 核心职责：接收子任务JVM通过RPC发来的状态更新、心跳等请求，将旧版MapReduce数据结构转换为YARN格式，
 *          转发给ApplicationMaster内部事件处理系统，管理任务尝试生命周期和JVM任务分配
 * 说明：本类必须放在当前包才能访问包私有方法/类
 */
public class TaskAttemptListenerImpl extends CompositeService 
    implements TaskUmbilicalProtocol, TaskAttemptListener {

  private static final JvmTask TASK_FOR_INVALID_JVM = new JvmTask(null, true);

  private static final Logger LOG =
      LoggerFactory.getLogger(TaskAttemptListenerImpl.class);

  private AppContext context;
  private Server server;
  protected TaskHeartbeatHandler taskHeartbeatHandler;
  private RMHeartbeatHandler rmHeartbeatHandler;
  private long commitWindowMs;
  private InetSocketAddress address;
  // JVMID到当前运行任务的映射，用于JVM复用场景给启动的JVM分配任务
  private ConcurrentMap<WrappedJvmID, org.apache.hadoop.mapred.Task>
    jvmIDToActiveAttemptMap
      = new ConcurrentHashMap<WrappedJvmID, org.apache.hadoop.mapred.Task>();

  // 任务尝试ID到最新状态的映射，用于合并状态更新
  private ConcurrentMap<TaskAttemptId,
      AtomicReference<TaskAttemptStatus>> attemptIdToStatus
        = new ConcurrentHashMap<>();

  /**
   * A Map to keep track of the History of logging each task attempt.
   */
  // 保存每个任务尝试的进度日志记录，用于控制日志输出频率
  private ConcurrentHashMap<TaskAttemptID, TaskProgressLogPair>
      taskAttemptLogProgressStamps = new ConcurrentHashMap<>();

  // 已启动的JVM集合，只有完成注册的JVM才能分配任务
  private Set<WrappedJvmID> launchedJVMs = Collections
      .newSetFromMap(new ConcurrentHashMap<WrappedJvmID, Boolean>());

  private JobTokenSecretManager jobTokenSecretManager = null;
  private AMPreemptionPolicy preemptionPolicy;
  private byte[] encryptedSpillKey;

  /**
   * 构造TaskAttemptListenerImpl实例
   * @param context ApplicationMaster上下文对象
   * @param jobTokenSecretManager Job令牌密钥管理器，用于RPC安全认证
   * @param rmHeartbeatHandler ResourceManager心跳处理器，用于判断是否可以提交输出
   * @param preemptionPolicy 任务抢占策略处理器
   */
  public TaskAttemptListenerImpl(AppContext context,
      JobTokenSecretManager jobTokenSecretManager,
      RMHeartbeatHandler rmHeartbeatHandler,
      AMPreemptionPolicy preemptionPolicy) {
    this(context, jobTokenSecretManager, rmHeartbeatHandler,
            preemptionPolicy, null);
  }

  /**
   * 构造TaskAttemptListenerImpl实例，支持指定shuffle加密密钥
   * @param context ApplicationMaster上下文对象
   * @param jobTokenSecretManager Job令牌密钥管理器，用于RPC安全认证
   * @param rmHeartbeatHandler ResourceManager心跳处理器，用于判断是否可以提交输出
   * @param preemptionPolicy 任务抢占策略处理器
   * @param secretShuffleKey shuffle溢写加密密钥
   */
  public TaskAttemptListenerImpl(AppContext context,
      JobTokenSecretManager jobTokenSecretManager,
      RMHeartbeatHandler rmHeartbeatHandler,
      AMPreemptionPolicy preemptionPolicy, byte[] secretShuffleKey) {
    super(TaskAttemptListenerImpl.class.getName());
    this.context = context;
    this.jobTokenSecretManager = jobTokenSecretManager;
    this.rmHeartbeatHandler = rmHeartbeatHandler;
    this.preemptionPolicy = preemptionPolicy;
    this.encryptedSpillKey = secretShuffleKey;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    registerHeartbeatHandler(conf);
    // 从配置读取提交窗口超时时间，用于避免脑裂场景下重复提交
    commitWindowMs = conf.getLong(MRJobConfig.MR_AM_COMMIT_WINDOW_MS,
        MRJobConfig.DEFAULT_MR_AM_COMMIT_WINDOW_MS);
    // 初始化任务进度日志的增量阈值
    MRJobConfUtil.setTaskLogProgressDeltaThresholds(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    startRpcServer();
    super.serviceStart();
  }

  /**
   * 注册任务心跳处理器，将其作为复合服务添加到当前服务
   * @param conf 配置对象
   */
  protected void registerHeartbeatHandler(Configuration conf) {
    taskHeartbeatHandler = new TaskHeartbeatHandler(context.getEventHandler(), 
        context.getClock(), conf.getInt(MRJobConfig.MR_AM_TASK_LISTENER_THREAD_COUNT, 
            MRJobConfig.DEFAULT_MR_AM_TASK_LISTENER_THREAD_COUNT));
    addService(taskHeartbeatHandler);
  }

  /**
   * 启动监听任务JVM请求的RPC服务器
   */
  protected void startRpcServer() {
    Configuration conf = getConfig();
    try {
      // 构建RPC服务器，绑定TaskUmbilicalProtocol协议
      server = new RPC.Builder(conf).setProtocol(TaskUmbilicalProtocol.class)
          .setInstance(this).setBindAddress("0.0.0.0")
          .setPortRangeConfig(MRJobConfig.MR_AM_JOB_CLIENT_PORT_RANGE)
          .setNumHandlers(
          conf.getInt(MRJobConfig.MR_AM_TASK_LISTENER_THREAD_COUNT, 
          MRJobConfig.DEFAULT_MR_AM_TASK_LISTENER_THREAD_COUNT))
          .setVerbose(false).setSecretManager(jobTokenSecretManager).build();

      // 如果开启服务授权，刷新服务访问控制列表
      if (conf.getBoolean(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
          false)) {
        refreshServiceAcls(conf, new MRAMPolicyProvider());
      }

      server.start();
      // 构建对外提供服务的地址，使用NodeManager主机名+RPC服务器端口
      this.address = NetUtils.createSocketAddrForHost(
          context.getNMHostname(),
          server.getListenerAddress().getPort());
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }

  void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  @Override
  protected void serviceStop() throws Exception {
    stopRpcServer();
    super.serviceStop();
  }

  /**
   * 停止RPC服务器，释放端口资源
   */
  protected void stopRpcServer() {
    if (server != null) {
      server.stop();
    }
  }

  @Override
  public InetSocketAddress getAddress() {
    return address;
  }

  /**
   * 任务尝试询问ApplicationMaster是否可以提交输出，实现两阶段提交协议
   * @param taskAttemptID 旧API格式任务尝试ID
   * @return true表示允许提交，false表示需要重试
   * @throws IOException IO异常
   */
  @Override
  public boolean canCommit(TaskAttemptID taskAttemptID) throws IOException {
    LOG.info("Commit go/no-go request from " + taskAttemptID.toString());
    // 将旧API任务尝试ID转换为YARN格式
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
        TypeConverter.toYarn(taskAttemptID);

    // 标记该任务尝试正在进展，更新心跳时间
    taskHeartbeatHandler.progressing(attemptID);

    // 如果AM超过提交窗口未收到RM心跳，返回false避免脑裂场景重复提交
    long now = context.getClock().getTime();
    if (now - rmHeartbeatHandler.getLastHeartbeatTime() > commitWindowMs) {
      return false;
    }

    // 转发请求给对应Task实例，由Task判断当前尝试是否可以提交（处理推测执行场景）
    Job job = context.getJob(attemptID.getTaskId().getJobId());
    Task task = job.getTask(attemptID.getTaskId());
    return task.canCommit(attemptID);
  }

  /**
   * 任务尝试通知ApplicationMaster已进入提交等待状态
   * @param taskAttemptID 旧API格式任务尝试ID
   * @param taskStatsu 任务状态
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  @Override
  public void commitPending(TaskAttemptID taskAttemptID, TaskStatus taskStatsu)
          throws IOException, InterruptedException {
    LOG.info("Commit-pending state update from " + taskAttemptID.toString());
    // 将旧API任务尝试ID转换为YARN格式
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
        TypeConverter.toYarn(taskAttemptID);

    // 标记该任务尝试正在进展，更新心跳时间
    taskHeartbeatHandler.progressing(attemptID);
    // 发送提交等待事件给事件处理器
    context.getEventHandler().handle(
        new TaskAttemptEvent(attemptID, 
            TaskAttemptEventType.TA_COMMIT_PENDING));
  }

  @Override
  public void preempted(TaskAttemptID taskAttemptID, TaskStatus taskStatus)
          throws IOException, InterruptedException {
    LOG.info("Preempted state update from " + taskAttemptID.toString());
    // 将旧API任务尝试ID转换为YARN格式
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
        TypeConverter.toYarn(taskAttemptID);

    // 向抢占策略报告抢占成功
    preemptionPolicy.reportSuccessfulPreemption(attemptID);
    // 标记该任务尝试正在进展，更新心跳时间
    taskHeartbeatHandler.progressing(attemptID);

    // 发送任务已抢占事件给事件处理器
    context.getEventHandler().handle(
        new TaskAttemptEvent(attemptID,
            TaskAttemptEventType.TA_PREEMPTED));
  }

  @Override
  public void done(TaskAttemptID taskAttemptID) throws IOException {
    LOG.info("Done acknowledgment from " + taskAttemptID.toString());

    // 将旧API任务尝试ID转换为YARN格式
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
        TypeConverter.toYarn(taskAttemptID);

    // 标记该任务尝试正在进展，更新心跳时间
    taskHeartbeatHandler.progressing(attemptID);

    // 发送任务完成事件给事件处理器
    context.getEventHandler().handle(
        new TaskAttemptEvent(attemptID, TaskAttemptEventType.TA_DONE));
  }

  @Override
  public void fatalError(TaskAttemptID taskAttemptID, String msg, boolean fastFail)
      throws IOException {
    // 该方法仅在子任务JVM中触发，报告致命错误
    LOG.error("Task: " + taskAttemptID + " - exited : " + msg);
    // 上报诊断信息
    reportDiagnosticInfo(taskAttemptID, "Error: " + msg);

    // 将旧API任务尝试ID转换为YARN格式
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
        TypeConverter.toYarn(taskAttemptID);

    // 通知抢占策略该容器失败
    preemptionPolicy.handleFailedContainer(attemptID);

    // 发送任务失败事件给事件处理器
    context.getEventHandler().handle(
        new TaskAttemptFailEvent(attemptID, fastFail));
  }

  @Override
  public void fsError(TaskAttemptID taskAttemptID, String message)
      throws IOException {
    // 该方法仅在子任务JVM中触发，报告文件系统错误
    LOG.error("Task: " + taskAttemptID + " - failed due to FSError: "
        + message);
    // 上报诊断信息
    reportDiagnosticInfo(taskAttemptID, "FSError: " + message);

    // 将旧API任务尝试ID转换为YARN格式
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
        TypeConverter.toYarn(taskAttemptID);

    // 通知抢占策略该容器失败
    preemptionPolicy.handleFailedContainer(attemptID);

    // 发送任务失败事件给事件处理器
    context.getEventHandler().handle(
        new TaskAttemptFailEvent(attemptID));
  }

  @Override
  public void shuffleError(TaskAttemptID taskAttemptID, String message) throws IOException {
    // TODO: This isn't really used in any MR code. Ask for removal.    
  }

  @Override
  public MapTaskCompletionEventsUpdate getMapCompletionEvents(
      JobID jobIdentifier, int startIndex, int maxEvents,
      TaskAttemptID taskAttemptID) throws IOException {
    LOG.info("MapCompletionEvents request from " + taskAttemptID.toString()
        + ". startIndex " + startIndex + " maxEvents " + maxEvents);

    // TODO: shouldReset is never used. See TT. Ask for Removal.
    boolean shouldReset = false;
    // 将旧API任务尝试ID转换为YARN格式
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
      TypeConverter.toYarn(taskAttemptID);
    // 从Job中获取指定范围的Map尝试完成事件
    TaskCompletionEvent[] events =
        context.getJob(attemptID.getTaskId().getJobId()).getMapAttemptCompletionEvents(
            startIndex, maxEvents);

    // 标记该任务尝试正在进展，更新心跳时间
    taskHeartbeatHandler.progressing(attemptID);
    
    return new MapTaskCompletionEventsUpdate(events, shouldReset);
  }

  @Override
  public void reportDiagnosticInfo(TaskAttemptID taskAttemptID, String diagnosticInfo)
 throws IOException {
    // 使用弱引用驻留字符串，节省内存
    diagnosticInfo = StringInterner.weakIntern(diagnosticInfo);
    LOG.info("Diagnostics report from " + taskAttemptID.toString() + ": "
        + diagnosticInfo);

    // 将旧API任务尝试ID转换为YARN格式
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
      TypeConverter.toYarn(taskAttemptID);
    // 标记该任务尝试正在进展，更新心跳时间
    taskHeartbeatHandler.progressing(attemptID);

    // 发送诊断信息更新事件给事件处理器
    context.getEventHandler().handle(
        new TaskAttemptDiagnosticsUpdateEvent(attemptID, diagnosticInfo));
  }

  @Override
  public AMFeedback statusUpdate(TaskAttemptID taskAttemptID,
      TaskStatus taskStatus) throws IOException, InterruptedException {

    // 将旧API任务尝试