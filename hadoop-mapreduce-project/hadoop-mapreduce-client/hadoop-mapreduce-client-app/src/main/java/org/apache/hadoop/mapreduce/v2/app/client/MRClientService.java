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

package org.apache.hadoop.mapreduce.v2.app.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpConfig.Policy;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.security.authorize.MRAMPolicyProvider;
import org.apache.hadoop.mapreduce.v2.app.webapp.AMWebApp;
import org.apache.hadoop.mapreduce.v2.app.webapp.AMWebServices;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.app.webapp.JAXBContextResolver;
import org.apache.hadoop.mapreduce.v2.app.webapp.jsonprovider.JsonProviderFeature;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：MapReduce ApplicationMaster 客户端服务实现
 * 核心职责：负责处理来自Job客户端的RPC请求，同时提供AM Web服务端点，
 * 支持客户端查询作业/任务状态、杀作业、杀任务等操作，是客户端与AM之间的交互入口
 */

/**
 * This module is responsible for talking to the 
 * jobclient (user facing).
 *
 * MR客户端服务，负责处理MapReduce客户端对ApplicationMaster的各类请求，
 * 包括RPC服务和Web应用服务两个部分，提供作业状态查询、作业/任务终止等功能
 */
public class MRClientService extends AbstractService implements ClientService {

  static final Logger LOG = LoggerFactory.getLogger(MRClientService.class);
  
  private MRClientProtocol protocolHandler;
  private Server server;
  private WebApp webApp;
  private InetSocketAddress bindAddress;
  private AppContext appContext;

  /**
   * 构造MR客户端服务
   * @param appContext ApplicationMaster上下文，包含当前作业运行时信息
   */
  public MRClientService(AppContext appContext) {
    super(MRClientService.class.getName());
    this.appContext = appContext;
    this.protocolHandler = new MRClientProtocolHandler();
  }

  /**
   * 服务启动方法，初始化并启动RPC服务和Web应用服务
   * @throws Exception 启动过程中抛出异常
   */
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress address = new InetSocketAddress(0);

    // 创建RPC服务端，绑定MR客户端协议处理器
    server =
        rpc.getServer(MRClientProtocol.class, protocolHandler, address,
            conf, appContext.getClientToAMTokenSecretManager(),
            conf.getInt(MRJobConfig.MR_AM_JOB_CLIENT_THREAD_COUNT, 
                MRJobConfig.DEFAULT_MR_AM_JOB_CLIENT_THREAD_COUNT),
                MRJobConfig.MR_AM_JOB_CLIENT_PORT_RANGE);
    
    // 如果开启服务授权，刷新访问控制列表
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      refreshServiceAcls(conf, new MRAMPolicyProvider());
    }

    // 启动RPC服务
    server.start();
    // 构造服务绑定地址，使用NM提供的主机名和RPC服务端口
    this.bindAddress = NetUtils.createSocketAddrForHost(appContext.getNMHostname(),
        server.getListenerAddress().getPort());
    LOG.info("Instantiated MRClientService at " + this.bindAddress);
    try {
      // 根据配置确定HTTP策略：仅HTTPS或仅HTTP
      HttpConfig.Policy httpPolicy = conf.getBoolean(
          MRJobConfig.MR_AM_WEBAPP_HTTPS_ENABLED,
          MRJobConfig.DEFAULT_MR_AM_WEBAPP_HTTPS_ENABLED)
          ? Policy.HTTPS_ONLY : Policy.HTTP_ONLY;
      // 获取HTTPS客户端认证配置
      boolean needsClientAuth = conf.getBoolean(
          MRJobConfig.MR_AM_WEBAPP_HTTPS_CLIENT_AUTH,
          MRJobConfig.DEFAULT_MR_AM_WEBAPP_HTTPS_CLIENT_AUTH);
      // 启动AM Web应用服务
      webApp =
          WebApps.$for("mapreduce", AppContext.class, appContext, "rm-ws")
            .withHttpPolicy(conf, httpPolicy)
            .withPortRange(conf, MRJobConfig.MR_AM_WEBAPP_PORT_RANGE)
            .needsClientAuth(needsClientAuth)
            .withResourceConfig(configure())
            .start(new AMWebApp(appContext));
    } catch (Exception e) {
      LOG.error("Webapps failed to start. Ignoring for now:", e);
    }
    super.serviceStart();
  }

  /**
   * 刷新服务访问控制列表，更新安全授权配置
   * @param configuration 配置对象
   * @param policyProvider 权限策略提供者
   */
  void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (server != null) {
      server.stop();
    }
    if (webApp != null) {
      webApp.stop();
    }
    super.serviceStop();
  }

  @Override
  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }

  @Override
  public int getHttpPort() {
    return webApp.port();
  }

  /**
   * MR客户端协议处理器，实现MRClientProtocol接口，处理所有客户端RPC请求
   */
  class MRClientProtocolHandler implements MRClientProtocol {

    private RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);

    @Override
    public InetSocketAddress getConnectAddress() {
      return getBindAddress();
    }
    
    /**
     * 验证作业权限并获取作业实例
     * @param jobID 作业ID
     * @param accessType 访问类型（查看/修改）
     * @param exceptionThrow 作业不存在时是否抛出异常
     * @return 作业实例
     * @throws IOException 作业不存在或权限验证失败时抛出异常
     */
    private Job verifyAndGetJob(JobId jobID, JobACL accessType,
        boolean exceptionThrow) throws IOException {
      Job job = appContext.getJob(jobID);
      if (job == null && exceptionThrow) {
        throw new IOException("Unknown Job " + jobID);
      }
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      if (job != null && !job.checkAccess(ugi, accessType)) {
        throw new AccessControlException("User " + ugi.getShortUserName()
            + " cannot perform operation " + accessType.name() + " on "
            + jobID);
      }
      return job;
    }
 
    /**
     * 验证任务权限并获取任务实例
     * @param taskID 任务ID
     * @param accessType 访问类型（查看/修改）
     * @return 任务实例
     * @throws IOException 任务/作业不存在或权限验证失败时抛出异常
     */
    private Task verifyAndGetTask(TaskId taskID, 
        JobACL accessType) throws IOException {
      Task task =
          verifyAndGetJob(taskID.getJobId(), accessType, true).getTask(taskID);
      if (task == null) {
        throw new IOException("Unknown Task " + taskID);
      }
      return task;
    }

    /**
     * 验证任务尝试权限并获取任务尝试实例
     * @param attemptID 任务尝试ID
     * @param accessType 访问类型（查看/修改）
     * @return 任务尝试实例
     * @throws IOException 任务尝试/任务/作业不存在或权限验证失败时抛出异常
     */
    private TaskAttempt verifyAndGetAttempt(TaskAttemptId attemptID, 
        JobACL accessType) throws IOException {
      TaskAttempt attempt = verifyAndGetTask(attemptID.getTaskId(), 
          accessType).getAttempt(attemptID);
      if (attempt == null) {
        throw new IOException("Unknown TaskAttempt " + attemptID);
      }
      return attempt;
    }

    @Override
    public GetCountersResponse getCounters(GetCountersRequest request) 
      throws IOException {
      JobId jobId = request.getJobId();
      Job job = verifyAndGetJob(jobId, JobACL.VIEW_JOB, true);
      GetCountersResponse response =
        recordFactory.newRecordInstance(GetCountersResponse.class);
      response.setCounters(TypeConverter.toYarn(job.getAllCounters()));
      return response;
    }
    
    @Override
    public GetJobReportResponse getJobReport(GetJobReportRequest request) 
      throws IOException {
      JobId jobId = request.getJobId();
      // false 保留向后兼容性
      Job job = verifyAndGetJob(jobId, JobACL.VIEW_JOB, false);
      GetJobReportResponse response = 
        recordFactory.newRecordInstance(GetJobReportResponse.class);
      if (job != null) {
        response.setJobReport(job.getReport());
      }
      else {
        response.setJobReport(null);
      }
      return response;
    }

    @Override
    public GetTaskAttemptReportResponse getTaskAttemptReport(
        GetTaskAttemptReportRequest request) throws IOException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      GetTaskAttemptReportResponse response =
        recordFactory.newRecordInstance(GetTaskAttemptReportResponse.class);
      response.setTaskAttemptReport(
          verifyAndGetAttempt(taskAttemptId, JobACL.VIEW_JOB).getReport());
      return response;
    }

    @Override
    public GetTaskReportResponse getTaskReport(GetTaskReportRequest request) 
      throws IOException {
      TaskId taskId = request.getTaskId();
      GetTaskReportResponse response = 
        recordFactory.newRecordInstance(GetTaskReportResponse.class);
      response.setTaskReport(
          verifyAndGetTask(taskId, JobACL.VIEW_JOB).getReport());
      return response;
    }

    @Override
    public GetTaskAttemptCompletionEventsResponse getTaskAttemptCompletionEvents(
        GetTaskAttemptCompletionEventsRequest request) 
        throws IOException {
      JobId jobId = request.getJobId();
      int fromEventId = request.getFromEventId();
      int maxEvents = request.getMaxEvents();
      Job job = verifyAndGetJob(jobId, JobACL.VIEW_JOB, true);
      
      GetTaskAttemptCompletionEventsResponse response = 
        recordFactory.newRecordInstance(GetTaskAttemptCompletionEventsResponse.class);
      response.addAllCompletionEvents(Arrays.asList(
          job.getTaskAttemptCompletionEvents(fromEventId, maxEvents)));
      return response;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public KillJobResponse killJob(KillJobRequest request) 
      throws IOException {
      JobId jobId = request.getJobId();
      UserGroupInformation callerUGI = UserGroupInformation.getCurrentUser();
      String message = "Kill job " + jobId + " received from " + callerUGI
          + " at " + Server.getRemoteAddress();
      LOG.info(message);
      verifyAndGetJob(jobId, JobACL.MODIFY_JOB, false);
      // 发布诊断更新事件
      appContext.getEventHandler().handle(
          new JobDiagnosticsUpdateEvent(jobId, message));
      // 发布作业杀死事件，触发作业杀死流程
      appContext.getEventHandler().handle(
          new JobEvent(jobId, JobEventType.JOB_KILL));
      KillJobResponse response = 
        recordFactory.newRecordInstance(KillJobResponse.class);
      return response;
    }

    @SuppressWarnings("unchecked")
    @Override
    public KillTaskResponse killTask(KillTaskRequest request) 
      throws IOException {
      TaskId taskId = request.getTaskId();
      UserGroupInformation callerUGI = UserGroupInformation.getCurrentUser();
      String message = "Kill task " + taskId + " received from " + callerUGI
          + " at " + Server.getRemoteAddress();
      LOG.info(message);
      verifyAndGetTask(taskId, JobACL.MODIFY_JOB);
      // 发布任务杀死事件，触发任务杀死流程
      appContext.getEventHandler().handle(
          new TaskEvent(taskId, TaskEventType.T_KILL));
      KillTaskResponse response = 
        recordFactory.newRecordInstance(KillTaskResponse.class);
      return response;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public KillTaskAttemptResponse killTaskAttempt(
        KillTaskAttemptRequest request) throws IOException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      UserGroupInformation callerUGI = UserGroupInformation.getCurrentUser();
      String message = "Kill task attempt " + taskAttemptId
          + " received from " + callerUG