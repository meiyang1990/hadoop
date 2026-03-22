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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * MapReduce作业客户端服务代理委托类，负责对接YARN RM、ApplicationMaster和历史服务器，
 * 统一处理不同服务端节点的作业查询、控制等RPC调用，负责自动切换代理目标。
 * 核心职责：根据作业运行状态自动选择调用AM、RM或历史服务器，处理连接重试和故障转移。
 */
public class ClientServiceDelegate {
  private static final Logger LOG =
      LoggerFactory.getLogger(ClientServiceDelegate.class);
  private static final String UNAVAILABLE = "N/A";

  // 未运行作业的缓存，按用户、作业状态分类缓存
  private HashMap<JobState, HashMap<String, NotRunningJob>> notRunningJobs;

  private final Configuration conf;
  private final JobID jobId;
  private final ApplicationId appId;
  private final ResourceMgrDelegate rm;
  private final MRClientProtocol historyServerProxy;
  private MRClientProtocol realProxy = null;
  private RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private static String UNKNOWN_USER = "Unknown User";
  private String trackingUrl;
  private AtomicBoolean usingAMProxy = new AtomicBoolean(false);
  private int maxClientRetry;
  private boolean amAclDisabledStatusLogged = false;

  /**
   * 构造客户端服务代理委托实例
   * @param conf 配置对象
   * @param rm YARN资源管理器代理委托
   * @param jobId 作业ID
   * @param historyServerProxy 作业历史服务器代理，可为null
   */
  public ClientServiceDelegate(Configuration conf, ResourceMgrDelegate rm,
      JobID jobId, MRClientProtocol historyServerProxy) {
    this.conf = new Configuration(conf); // 克隆配置用于修改
    // 优化从AM跳转到历史服务器的连接超时配置
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        this.conf.getInt(MRJobConfig.MR_CLIENT_TO_AM_IPC_MAX_RETRIES,
            MRJobConfig.DEFAULT_MR_CLIENT_TO_AM_IPC_MAX_RETRIES));
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        this.conf.getInt(MRJobConfig.MR_CLIENT_TO_AM_IPC_MAX_RETRIES_ON_TIMEOUTS,
            MRJobConfig.DEFAULT_MR_CLIENT_TO_AM_IPC_MAX_RETRIES_ON_TIMEOUTS));
    this.rm = rm;
    this.jobId = jobId;
    this.historyServerProxy = historyServerProxy;
    this.appId = TypeConverter.toYarn(jobId).getAppId();
    notRunningJobs = new HashMap<JobState, HashMap<String, NotRunningJob>>();
  }

  /**
   * 根据应用报告和作业状态获取对应未运行作业实例，按用户分组缓存
   * @param applicationReport YARN应用报告
   * @param state 作业状态
   * @return 缓存的未运行作业实例
   */
  private NotRunningJob getNotRunningJob(ApplicationReport applicationReport,
      JobState state) {
    synchronized (notRunningJobs) {
      HashMap<String, NotRunningJob> map = notRunningJobs.get(state);
      if (map == null) {
        map = new HashMap<String, NotRunningJob>();
        notRunningJobs.put(state, map);
      }
      String user =
          (applicationReport == null) ?
              UNKNOWN_USER : applicationReport.getUser();
      NotRunningJob notRunningJob = map.get(user);
      if (notRunningJob == null) {
        notRunningJob = new NotRunningJob(applicationReport, state);
        map.put(user, notRunningJob);
      }
      return notRunningJob;
    }
  }

  /**
   * 根据作业运行状态获取正确的MR客户端代理，自动选择AM/历史服务器/未运行作业代理
   * @return 可调用的MRClientProtocol代理实例
   * @throws IOException 获取代理失败时抛出异常
   */
  private MRClientProtocol getProxy() throws IOException {
    if (realProxy != null) {
      return realProxy;
    }
    
    // 从RM获取应用报告，如果找不到则跳转历史服务器
    ApplicationReport application = null;
    try {
      application = rm.getApplicationReport(appId);
    } catch (ApplicationNotFoundException e) {
      application = null;
    } catch (YarnException e2) {
      throw new IOException(e2);
    }
    if (application != null) {
      trackingUrl = application.getTrackingUrl();
    }
    InetSocketAddress serviceAddr = null;
    while (application == null
        || YarnApplicationState.RUNNING == application
            .getYarnApplicationState()) {
      if (application == null) {
        LOG.info("Could not get Job info from RM for job " + jobId
            + ". Redirecting to job history server.");
        return checkAndGetHSProxy(null, JobState.NEW);
      }
      try {
        // AM尚未分配节点，等待重试
        if (application.getHost() == null || "".equals(application.getHost())) {
          LOG.debug("AM not assigned to Job. Waiting to get the AM ...");
          Thread.sleep(2000);

          LOG.debug("Application state is " + application.getYarnApplicationState());
          application = rm.getApplicationReport(appId);
          continue;
        } else if (UNAVAILABLE.equals(application.getHost())) {
          // 用户没有权限查看AM地址，返回未运行作业代理
          if (!amAclDisabledStatusLogged) {
            LOG.info("Job " + jobId + " is running, but the host is unknown."
                + " Verify user has VIEW_JOB access.");
            amAclDisabledStatusLogged = true;
          }
          return getNotRunningJob(application, JobState.RUNNING);
        }
        if(!conf.getBoolean(MRJobConfig.JOB_AM_ACCESS_DISABLED, false)) {
          // 创建远程用户UGI，并添加ClientToAM令牌用于认证
          UserGroupInformation newUgi = UserGroupInformation.createRemoteUser(
              UserGroupInformation.getCurrentUser().getUserName());
          serviceAddr = NetUtils.createSocketAddrForHost(
              application.getHost(), application.getRpcPort());
          if (UserGroupInformation.isSecurityEnabled()) {
            org.apache.hadoop.yarn.api.records.Token clientToAMToken =
                application.getClientToAMToken();
            Token<ClientToAMTokenIdentifier> token =
                ConverterUtils.convertFromYarn(clientToAMToken, serviceAddr);
            newUgi.addToken(token);
          }
          LOG.debug("Connecting to " + serviceAddr);
          final InetSocketAddress finalServiceAddr = serviceAddr;
          // 在对应UGI下创建AM代理
          realProxy = newUgi.doAs(new PrivilegedExceptionAction<MRClientProtocol>() {
            @Override
            public MRClientProtocol run() throws IOException {
              return instantiateAMProxy(finalServiceAddr);
            }
          });
        } else {
          // AM访问被ACL禁用，返回未运行作业代理
          if (!amAclDisabledStatusLogged) {
            LOG.info("Network ACL closed to AM for job " + jobId
                + ". Not going to try to reach the AM.");
            amAclDisabledStatusLogged = true;
          }
          return getNotRunningJob(null, JobState.RUNNING);
        }
        return realProxy;
      } catch (IOException e) {
        // 连接AM失败，可能AM已崩溃或正在重启，等待重试从RM获取最新地址
        LOG.info("Could not connect to " + serviceAddr +
        ". Waiting for getting the latest AM address...");
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e1) {
          LOG.warn("getProxy() call interrupted", e1);
          throw new YarnRuntimeException(e1);
        }
        try {
          application = rm.getApplicationReport(appId);
        } catch (YarnException e1) {
          throw new IOException(e1);
        }
        if (application == null) {
          LOG.info("Could not get Job info from RM for job " + jobId
              + ". Redirecting to job history server.");
          return checkAndGetHSProxy(null, JobState.RUNNING);
        }
      } catch (InterruptedException e) {
        LOG.warn("getProxy() call interrupted", e);
        throw new YarnRuntimeException(e);
      } catch (YarnException e) {
        throw new IOException(e);
      }
    }

    // 若应用未到运行状态，直接返回对应状态的未运行作业代理，不阻塞
    String user = application.getUser();
    if (user == null) {
      throw new IOException("User is not set in the application report");
    }
    if (application.getYarnApplicationState() == YarnApplicationState.NEW
        || application.getYarnApplicationState() ==
            YarnApplicationState.NEW_SAVING
        || application.getYarnApplicationState() == YarnApplicationState.SUBMITTED
        || application.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
      realProxy = null;
      return getNotRunningJob(application, JobState.NEW);
    }

    if (application.getYarnApplicationState() == YarnApplicationState.FAILED) {
      realProxy = null;
      return getNotRunningJob(application, JobState.FAILED);
    }

    if (application.getYarnApplicationState() == YarnApplicationState.KILLED) {
      realProxy = null;
      return getNotRunningJob(application, JobState.KILLED);
    }

    // 作业已完成，跳转历史服务器
    if (application.getYarnApplicationState() == YarnApplicationState.FINISHED) {
      LOG.info("Application state is completed. FinalApplicationStatus="
          + application.getFinalApplicationStatus().toString()
          + ". Redirecting to job history server");
      realProxy = checkAndGetHSProxy(application, JobState.SUCCEEDED);
    }
    return realProxy;
  }

  /**
   * 检查并返回历史服务器代理，如果未配置则返回未运行作业代理
   * @param applicationReport YARN应用报告
   * @param state 作业状态
   * @return 历史服务器代理或未运行作业代理
   */
  private MRClientProtocol checkAndGetHSProxy(
      ApplicationReport applicationReport, JobState state) {
    if (null == historyServerProxy) {
      LOG.warn("Job History Server is not configured.");
      return getNotRunningJob(applicationReport, state);
    }
    return historyServerProxy;
  }

  /**
   * 创建ApplicationMaster的RPC代理
   * @param serviceAddr AM地址和端口
   * @return AM的MRClientProtocol代理
   * @throws IOException 创建代理失败时抛出异常
   */
  MRClientProtocol instantiateAMProxy(final InetSocketAddress serviceAddr)
      throws IOException {
    LOG.trace("Connecting to ApplicationMaster at: " + serviceAddr);
    YarnRPC rpc = YarnRPC.create(conf);
    MRClientProtocol proxy = 
         (MRClientProtocol) rpc.getProxy(MRClientProtocol.class,
            serviceAddr, conf);
    usingAMProxy.set(true);
    LOG.trace("Connected to ApplicationMaster at: " + serviceAddr);
    return proxy;
  }

  /**
   * 通过反射调用MRClientProtocol方法，内置重试机制，连接失败自动重连
   * @param method 方法名
   * @param argClass 参数类型
   * @param args 参数对象
   * @return 方法调用返回结果
   * @throws IOException 调用失败且重耗尽重试次数抛出异常
   */
  private synchronized Object invoke(String method, Class argClass,
      Object args) throws IOException {
    Method methodOb = null;
    try {
      methodOb = MRClientProtocol.class.getMethod(method, argClass);
    } catch (SecurityException e) {
      throw new YarnRuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new YarnRuntimeException("Method name mismatch", e);
    }
    maxClientRetry = this.conf.getInt(
        MRJobConfig.MR_CLIENT_MAX_RETRIES,
        MRJobConfig.DEFAULT_MR_CLIENT_MAX_RETRIES);
    IOException lastException = null;
    while (maxClientRetry > 0) {
      MRClientProtocol MRClientProxy = null;
      try {
        MRClientProxy = getProxy();
        return methodOb.invoke(MRClientProxy, args);
      } catch (InvocationTargetException e) {
        // 调用失败，清空代理强制重连
        LOG.debug("Failed to contact AM/History for job " + jobId + 
            " retrying..", e.getTargetException());
        realProxy = null;

        if (e.getCause() instanceof AuthorizationException) {
          throw new IOException(e.getTargetException());
        }

        // 如果不是连接AM，不消耗重试次数
        if (!usingAMProxy.get()) {
          maxClientRetry--;
        }
        usingAMProxy.set(false);
        lastException = new IOException(e.getTargetException());
        try {
          Thread.sleep(100);
        } catch (InterruptedException ie) {
          LOG.warn("ClientServiceDelegate invoke call interrupted", ie);
          throw new YarnRuntimeException(ie);
        }
      } catch (Exception e) {
        // 其他异常，清空代理强制重连
        LOG.debug("Failed to contact AM/History for job " + jobId
            + "  Will retry..", e);