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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
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
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.security.authorize.ClientHSPolicyProvider;
import org.apache.hadoop.mapreduce.v2.hs.webapp.HsWebApp;
import org.apache.hadoop.mapreduce.v2.hs.webapp.HsWebServices;
import org.apache.hadoop.mapreduce.v2.hs.webapp.JAXBContextResolver;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

import org.apache.hadoop.classification.VisibleForTesting;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 历史服务器客户端服务，负责处理用户/客户端对已完成MapReduce作业历史数据的查询请求
 * 同时提供RPC服务和Web UI服务两种访问方式
 */
public class HistoryClientService extends AbstractService {

  private static final Logger LOG =
      LoggerFactory.getLogger(HistoryClientService.class);

  private HSClientProtocol protocolHandler;
  private Server server;
  private WebApp webApp;
  private InetSocketAddress bindAddress;
  private HistoryContext history;
  private JHSDelegationTokenSecretManager jhsDTSecretManager;
  
  /**
   * 构造历史客户端服务实例
   * @param history 历史作业上下文，提供对已完成作业的查询能力
   * @param jhsDTSecretManager 历史服务器委派令牌密钥管理器，用于安全认证
   */
  public HistoryClientService(HistoryContext history,
      JHSDelegationTokenSecretManager jhsDTSecretManager) {
    super("HistoryClientService");
    this.history = history;
    this.protocolHandler = new HSClientProtocolHandler();
    this.jhsDTSecretManager = jhsDTSecretManager;
  }

  /**
   * 启动服务：初始化RPC服务器和Web应用
   * @throws Exception 启动过程中抛出异常
   */
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    // 创建YARN RPC实例
    YarnRPC rpc = YarnRPC.create(conf);
    // 初始化Web应用
    initializeWebApp(conf);
    // 从配置获取RPC服务绑定地址
    InetSocketAddress address = conf.getSocketAddr(
        JHAdminConfig.MR_HISTORY_BIND_HOST,
        JHAdminConfig.MR_HISTORY_ADDRESS,
        JHAdminConfig.DEFAULT_MR_HISTORY_ADDRESS,
        JHAdminConfig.DEFAULT_MR_HISTORY_PORT);

    // 创建RPC服务器
    server =
        rpc.getServer(HSClientProtocol.class, protocolHandler, address,
            conf, jhsDTSecretManager,
            conf.getInt(JHAdminConfig.MR_HISTORY_CLIENT_THREAD_COUNT,
                JHAdminConfig.DEFAULT_MR_HISTORY_CLIENT_THREAD_COUNT));

    // 如果开启了服务授权，刷新ACL配置
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      server.refreshServiceAcl(conf, new ClientHSPolicyProvider());
    }
    
    // 启动RPC服务器
    server.start();
    // 更新绑定地址信息（处理端口自动分配场景）
    this.bindAddress = conf.updateConnectAddr(JHAdminConfig.MR_HISTORY_BIND_HOST,
                                              JHAdminConfig.MR_HISTORY_ADDRESS,
                                              JHAdminConfig.DEFAULT_MR_HISTORY_ADDRESS,
                                              server.getListenerAddress());
    LOG.info("Instantiated HistoryClientService at " + this.bindAddress);

    super.serviceStart();
  }

  @VisibleForTesting
  /**
   * 初始化历史服务器Web应用
   * @param conf 配置对象
   * @throws IOException 初始化过程IO异常
   */
  protected void initializeWebApp(Configuration conf) throws IOException {
    webApp = new HsWebApp(history);

    // 设置过滤器（如跨域过滤器）
    setupFilters(conf);

    // 获取Web服务绑定地址
    InetSocketAddress bindAddress = MRWebAppUtil.getJHSWebBindAddress(conf);
    // 创建RM代理，用于和ResourceManager交互
    ApplicationClientProtocol appClientProtocol =
        ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class);
    // 启动Jetty Web服务
    WebApps
        .$for("jobhistory", HistoryClientService.class, this, "hs-ws")
        .with(conf)
        .withHttpSpnegoKeytabKey(
            JHAdminConfig.MR_WEBAPP_SPNEGO_KEYTAB_FILE_KEY)
        .withHttpSpnegoPrincipalKey(
            JHAdminConfig.MR_WEBAPP_SPNEGO_USER_NAME_KEY)
        .withCSRFProtection(JHAdminConfig.MR_HISTORY_CSRF_PREFIX)
        .withXFSProtection(JHAdminConfig.MR_HISTORY_XFS_PREFIX)
        .withAppClientProtocol(appClientProtocol)
        .withResourceConfig(configure(conf, appClientProtocol))
        .at(NetUtils.getHostPortString(bindAddress)).start(webApp);
    
    // 更新配置中Web服务的访问端口，处理自动端口分配场景
    String connectHost = MRWebAppUtil.getJHSWebappURLWithoutScheme(conf).split(":")[0];
    MRWebAppUtil.setJHSWebappURLWithoutScheme(conf,
        connectHost + ":" + webApp.getListenerAddress().getPort());
  }

  @Override
  /**
   * 停止服务，关闭RPC服务器和Web应用
   * @throws Exception 停止过程异常
   */
  protected void serviceStop() throws Exception {
    if (server != null) {
      server.stop();
    }
    if (webApp != null) {
      webApp.stop();
    }
    super.serviceStop();
  }

  @Private
  /**
   * 获取客户端协议处理器实例
   * @return 客户端协议处理器
   */
  public MRClientProtocol getClientHandler() {
    return this.protocolHandler;
  }

  @Private
  /**
   * 获取RPC服务绑定地址
   * @return RPC绑定地址
   */
  public InetSocketAddress getBindAddress() {
    return this.bindAddress;
  }

  /**
   * 设置Web过滤器，根据配置启用跨域过滤器
   * @param conf 配置对象
   */
  private void setupFilters(Configuration conf) {
    boolean enableCorsFilter =
        conf.getBoolean(JHAdminConfig.MR_HISTORY_ENABLE_CORS_FILTER,
            JHAdminConfig.DEFAULT_MR_HISTORY_ENABLE_CORS_FILTER);

    if (enableCorsFilter) {
      conf.setBoolean(HttpCrossOriginFilterInitializer.PREFIX
          + HttpCrossOriginFilterInitializer.ENABLED_SUFFIX, true);
    }
  }

  /**
   * HSClientProtocol协议实现类，处理所有客户端查询历史作业的RPC请求
   */
  private class HSClientProtocolHandler implements HSClientProtocol {

    private RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

    public InetSocketAddress getConnectAddress() {
      return getBindAddress();
    }
    
    /**
     * 验证权限并获取作业实例
     * @param jobID 作业ID
     * @param exceptionThrow 作业不存在时是否抛出异常
     * @return 验证通过的作业实例
     * @throws IOException 验证异常、作业不存在或权限不足
     */
    private Job verifyAndGetJob(final JobId jobID, boolean exceptionThrow)
        throws IOException {
      UserGroupInformation loginUgi = null;
      Job job = null;
      try {
        loginUgi = UserGroupInformation.getLoginUser();
        job = loginUgi.doAs(new PrivilegedExceptionAction<Job>() {

          @Override
          public Job run() throws Exception {
            Job job = history.getJob(jobID);
            return job;
          }
        });
      } catch (InterruptedException e) {
        throw new IOException(e);
      }

      if (job == null && exceptionThrow) {
        throw new IOException("Unknown Job " + jobID);
      }

      if (job != null) {
        JobACL operation = JobACL.VIEW_JOB;
        checkAccess(job, operation);
      }
      return job;
    }

    @Override
    public GetCountersResponse getCounters(GetCountersRequest request)
        throws IOException {
      JobId jobId = request.getJobId();
      Job job = verifyAndGetJob(jobId, true);
      GetCountersResponse response = recordFactory.newRecordInstance(GetCountersResponse.class);
      response.setCounters(TypeConverter.toYarn(job.getAllCounters()));
      return response;
    }

    @Override
    public GetJobReportResponse getJobReport(GetJobReportRequest request)
        throws IOException {
      JobId jobId = request.getJobId();
      Job job = verifyAndGetJob(jobId, false);
      GetJobReportResponse response = recordFactory.newRecordInstance(GetJobReportResponse.class);
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
      Job job = verifyAndGetJob(taskAttemptId.getTaskId().getJobId(), true);
      GetTaskAttemptReportResponse response = recordFactory.newRecordInstance(GetTaskAttemptReportResponse.class);
      response.setTaskAttemptReport(job.getTask(taskAttemptId.getTaskId()).getAttempt(taskAttemptId).getReport());
      return response;
    }

    @Override
    public GetTaskReportResponse getTaskReport(GetTaskReportRequest request)
        throws IOException {
      TaskId taskId = request.getTaskId();
      Job job = verifyAndGetJob(taskId.getJobId(), true);
      GetTaskReportResponse response = recordFactory.newRecordInstance(GetTaskReportResponse.class);
      response.setTaskReport(job.getTask(taskId).getReport());
      return response;
    }

    @Override
    public GetTaskAttemptCompletionEventsResponse
        getTaskAttemptCompletionEvents(
            GetTaskAttemptCompletionEventsRequest request) throws IOException {
      JobId jobId = request.getJobId();
      int fromEventId = request.getFromEventId();
      int maxEvents = request.getMaxEvents();

      Job job = verifyAndGetJob(jobId, true);
      GetTaskAttemptCompletionEventsResponse response = recordFactory.newRecordInstance(GetTaskAttemptCompletionEventsResponse.class);
      response.addAllCompletionEvents(Arrays.asList(job.getTaskAttemptCompletionEvents(fromEventId, maxEvents)));
      return response;
    }

    @Override
    public KillJobResponse killJob(KillJobRequest request) throws IOException {
      throw new IOException("Invalid operation on completed job");
    }

    @Override
    public KillTaskResponse killTask(KillTaskRequest request)
        throws IOException {
      throw new IOException("Invalid operation on completed job");
    }

    @Override
    public KillTaskAttemptResponse killTaskAttempt(
        KillTaskAttemptRequest request) throws IOException {
      throw new IOException("Invalid operation on completed job");
    }

    @Override
    public GetDiagnosticsResponse getDiagnostics(GetDiagnosticsRequest request)
        throws IOException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();

      Job job = verifyAndGetJob(taskAttemptId.getTaskId().getJobId(), true);

      GetDiagnosticsResponse response = recordFactory.newRecordInstance(GetDiagnosticsResponse.class);
      response.addAllDiagnostics(job.getTask(taskAttemptId.getTaskId()).getAttempt(taskAttemptId).getDiagnostics());
      return response;
    }

    @Override
    public FailTaskAttemptResponse failTaskAttempt(
        FailTaskAttemptRequest request) throws IOException {
      throw new IOException("Invalid operation on completed job");
    }

    @Override
    public GetTaskReportsResponse getTaskReports(GetTaskReportsRequest request)
        throws IOException {
      JobId jobId = request.getJobId();
      TaskType taskType = request.getTaskType();

      GetTaskReportsResponse response = recordFactory.newRecordInstance(GetTaskReportsResponse.class);
      Job job = verifyAndGetJob(jobId, true);
      Collection<Task> tasks = job.getTasks(taskType).values();
      for (Task task : tasks) {
        response.addTaskReport(task.getReport());
      }
      return response;
    }
    
    @Override
    public GetDelegationTokenResponse getDelegationToken(
        GetDelegationTokenRequest request) throws IOException {

      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

      // 仅允许Kerberos认证场景签发委派令牌
        if (!isAllowedDelegationTokenOp()) {
          throw new IOException(
              "Delegation Token