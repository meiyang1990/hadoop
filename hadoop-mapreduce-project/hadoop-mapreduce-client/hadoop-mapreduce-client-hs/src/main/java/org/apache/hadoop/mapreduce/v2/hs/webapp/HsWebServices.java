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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import java.io.IOException;
import java.util.Set;

import javax.annotation.Nullable;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.AMWebServices;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ConfInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskAttemptCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.MapTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ReduceTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TasksInfo;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.AMAttemptInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.AMAttemptsInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.HistoryInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.logaggregation.ExtendedLogMetaRequest;
import org.apache.hadoop.yarn.server.webapp.WrappedLogMetaRequest;
import org.apache.hadoop.yarn.server.webapp.YarnWebServiceParams;
import org.apache.hadoop.yarn.server.webapp.LogServlet;
import org.apache.hadoop.yarn.server.webapp.WebServices;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.WebApp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 历史任务服务器REST API服务实现类
 * 为MapReduce历史任务查询提供JSON/XML格式的REST接口，支持任务、任务尝试、计数器、日志等信息查询
 */
@Singleton
@Path("/ws/v1/history")
public class HsWebServices extends WebServices {
  private static final Logger LOG = LoggerFactory.getLogger(HsWebServices.class);
  private final HistoryContext ctx;
  private WebApp webapp;
  private LogServlet logServlet;
  private boolean mrAclsEnabled;

  @Context
  private HttpServletResponse response;

  @Context
  private UriInfo uriInfo;

  /**
   * 构造方法，通过依赖注入初始化历史服务REST API
   * @param ctx 历史上下文，提供已完成作业的访问能力
   * @param conf Hadoop配置对象
   * @param webapp 历史服务Web应用实例
   * @param appBaseProto YARN应用客户端协议，可空
   */
  @Inject
  public HsWebServices(
      final @Named("ctx") HistoryContext ctx,
      final @Named("conf") Configuration conf,
      final @Named("hsWebApp") WebApp webapp,
      final @Named("appClient") @Nullable ApplicationClientProtocol appBaseProto) {
    super(appBaseProto);
    this.ctx = ctx;
    this.webapp = webapp;
    this.logServlet = new LogServlet(conf, this);
    this.mrAclsEnabled = conf.getBoolean(MRConfig.MR_ACLS_ENABLED, false);
  }

  /**
   * 检查当前请求用户是否有权限访问指定作业
   * @param job 待检查的作业对象
   * @param request HTTP请求对象
   * @return true表示有权限，false表示无权限
   */
  private boolean hasAccess(Job job, HttpServletRequest request) {
    String remoteUser = request.getRemoteUser();
    if (remoteUser != null) {
      return job.checkAccess(UserGroupInformation.createRemoteUser(remoteUser),
          JobACL.VIEW_JOB);
    }
    return true;
  }

  /**
   * 检查访问权限，如果无权限则抛出未授权异常
   * @param job 待检查的作业对象
   * @param request HTTP请求对象
   */
  private void checkAccess(Job job, HttpServletRequest request) {
    if (!hasAccess(job, request)) {
      throw new WebApplicationException(Status.UNAUTHORIZED);
    }
  }

  /**
   * 根据容器ID检查用户是否有权限访问对应容器日志
   * 仅对属于MapReduce作业的容器启用MR ACL检查，非MR容器不做权限校验
   * @param containerIdStr 容器ID字符串
   * @param hsr HTTP请求对象
   */
  private void checkAccess(String containerIdStr, HttpServletRequest hsr) {
    // Apply MR ACLs only if the container belongs to a MapReduce job.
    // For non-MapReduce jobs, no corresponding Job will be found,
    // so ACLs are not enforced.
    if (mrAclsEnabled && isMRJobContainer(containerIdStr)) {
      Job job = AMWebServices.getJobFromContainerIdString(containerIdStr, ctx);
      checkAccess(job, hsr);
    }
  }

  /**
   * 判断给定容器是否属于当前历史服务器管理的MapReduce作业
   * @param containerIdStr 容器ID字符串
   * @return true表示属于MapReduce作业，false表示不属于
   */
  private boolean isMRJobContainer(String containerIdStr) {
    try {
      AMWebServices.getJobFromContainerIdString(containerIdStr, ctx);
      return true;
    } catch (NotFoundException e) {
      LOG.trace("Container {} does not belong to a MapReduce job", containerIdStr);
      return false;
    }
  }

  /**
   * 初始化响应，清空之前设置的ContentType
   */
  private void init() {
    //clear content type
    response.setContentType(null);
  }

  /**
   * 仅用于测试，设置HTTP响应对象
   * @param response HTTP响应对象
   */
  @VisibleForTesting
  void setResponse(HttpServletResponse response) {
    this.response = response;
  }

  /**
   * 获取历史服务器基本信息
   * @return 历史服务器基本信息对象
   */
  @GET
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public HistoryInfo get() {
    return getHistoryInfo();
  }

  /**
   * 获取历史服务器基本信息
   * @return 历史服务器基本信息对象
   */
  @GET
  @Path("/info")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public HistoryInfo getHistoryInfo() {
    init();
    return new HistoryInfo();
  }

  /**
   * 按条件分页查询已完成的作业列表
   * @param userQuery 过滤提交用户，可空
   * @param count 返回结果数量限制，可空
   * @param stateQuery 过滤作业状态，可空
   * @param queueQuery 过滤队列，可空
   * @param startedBegin 作业开始时间起始戳，可空
   * @param startedEnd 作业开始时间结束戳，可空
   * @param finishBegin 作业结束时间起始戳，可空
   * @param finishEnd 作业结束时间结束戳，可空
   * @return 符合条件的作业信息列表
   */
  @GET
  @Path("/mapreduce/jobs")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public JobsInfo getJobs(@QueryParam("user") String userQuery,
      @QueryParam("limit") String count,
      @QueryParam("state") String stateQuery,
      @QueryParam("queue") String queueQuery,
      @QueryParam("startedTimeBegin") String startedBegin,
      @QueryParam("startedTimeEnd") String startedEnd,
      @QueryParam("finishedTimeBegin") String finishBegin,
      @QueryParam("finishedTimeEnd") String finishEnd) {

    Long countParam = null;
    init();
    
    // 解析返回结果数量限制参数
    if (count != null && !count.isEmpty()) {
      try {
        countParam = Long.parseLong(count);
      } catch (NumberFormatException e) {
        throw new BadRequestException(e.getMessage());
      }
      if (countParam <= 0) {
        throw new BadRequestException("limit value must be greater then 0");
      }
    }

    // 解析作业开始时间起始参数
    Long sBegin = null;
    if (startedBegin != null && !startedBegin.isEmpty()) {
      try {
        sBegin = Long.parseLong(startedBegin);
      } catch (NumberFormatException e) {
        throw new BadRequestException("Invalid number format: " + e.getMessage());
      }
      if (sBegin < 0) {
        throw new BadRequestException("startedTimeBegin must be greater than 0");
      }
    }
    
    // 解析作业开始时间结束参数
    Long sEnd = null;
    if (startedEnd != null && !startedEnd.isEmpty()) {
      try {
        sEnd = Long.parseLong(startedEnd);
      } catch (NumberFormatException e) {
        throw new BadRequestException("Invalid number format: " + e.getMessage());
      }
      if (sEnd < 0) {
        throw new BadRequestException("startedTimeEnd must be greater than 0");
      }
    }
    // 检查时间范围合法性
    if (sBegin != null && sEnd != null && sBegin > sEnd) {
      throw new BadRequestException(
          "startedTimeEnd must be greater than startTimeBegin");
    }

    // 解析作业结束时间起始参数
    Long fBegin = null;
    if (finishBegin != null && !finishBegin.isEmpty()) {
      try {
        fBegin = Long.parseLong(finishBegin);
      } catch (NumberFormatException e) {
        throw new BadRequestException("Invalid number format: " + e.getMessage());
      }
      if (fBegin < 0) {
        throw new BadRequestException("finishedTimeBegin must be greater than 0");
      }
    }
    // 解析作业结束时间结束参数
    Long fEnd = null;
    if (finishEnd != null && !finishEnd.isEmpty()) {
      try {
        fEnd = Long.parseLong(finishEnd);
      } catch (NumberFormatException e) {
        throw new BadRequestException("Invalid number format: " + e.getMessage());
      }
      if (fEnd < 0) {
        throw new BadRequestException("finishedTimeEnd must be greater than 0");
      }
    }
    // 检查时间范围合法性
    if (fBegin != null && fEnd != null && fBegin > fEnd) {
      throw new BadRequestException(
          "finishedTimeEnd must be greater than finishedTimeBegin");
    }
    
    // 解析作业状态过滤参数
    JobState jobState = null;
    if (stateQuery != null) {
      jobState = JobState.valueOf(stateQuery);
    }

    // 调用历史上下文查询符合条件的作业
    return ctx.getPartialJobs(0l, countParam, userQuery, queueQuery, 
        sBegin, sEnd, fBegin, fEnd, jobState);
  }

  /**
   * 根据作业ID获取单个作业的详细信息
   * @param hsr HTTP请求对象
   * @param jid 作业ID字符串
   * @return 作业详细信息对象
   */
  @GET
  @Path("/mapreduce/jobs/{jobid}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public JobInfo getJob(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {

    init();
    Job job = AMWebServices.getJobFromJobIdString(jid, ctx);
    checkAccess(job, hsr);
    return new JobInfo(job);
  }

  /**
   * 获取指定作业的所有ApplicationMaster尝试信息
   * @param jid 作业ID字符串
   * @return AM尝试信息列表
   */
  @GET
  @Path("/mapreduce/jobs/{jobid}/jobattempts")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public AMAttemptsInfo getJobAttempts(@PathParam("jobid") String jid) {

    init();
    Job job = AMWebServices.getJobFromJobIdString(jid, ctx);
    AMAttemptsInfo amAttempts = new AMAttemptsInfo();
    for (AMInfo amInfo : job.getAMInfos()) {
      AMAttemptInfo attempt = new AMAttemptInfo(amInfo, MRApps.toString(job
          .getID()), job.getUserName(), uriInfo.getBaseUri().toString(),
          webapp.name);
      amAttempts.add(attempt);
    }
    return amAttempts;
  }

  /**
   * 获取指定作业的所有计数器信息
   * @param hsr HTTP请求对象
   * @param jid 作业ID字符串
   * @return 作业计数器信息对象
   */
  @GET
  @Path("/mapreduce/jobs/{jobid}/counters")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public JobCounterInfo getJobCounters(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {

    init();
    Job job = AMWebServices.getJobFromJobIdString(jid, ctx);
    checkAccess(job, hsr);
    return new JobCounterInfo(this.ctx, job);
  }

  /**
   * 获取指定作业提交时的配置信息
   * @param hsr HTTP请求对象
   * @param jid 作业ID字符串
   * @return 作业配置信息对象
   */
  @GET
  @Path("/mapreduce/jobs/{jobid}/conf")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public ConfInfo getJobConf(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {

    init();
    Job job = AMWebServices.getJobFromJobIdString(jid, ctx);
    checkAccess(job, hsr);
    ConfInfo info;
    try {
      info = new ConfInfo(job);
    } catch (IOException e) {
      throw new NotFoundException("unable to load configuration for job