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

package org.apache.hadoop.mapreduce.v2.app.webapp;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.inject.Named;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.KillTaskAttemptRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.MRClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.AppInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.AMAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.AMAttemptsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.BlacklistedNodesInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ConfInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskAttemptCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskAttemptState;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.MapTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ReduceTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TasksInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import org.apache.hadoop.util.Preconditions;

/**
 * MapReduce ApplicationMaster RESTful Web服务入口类，提供对ApplicationMaster、Job、Task、TaskAttempt等资源的查询和操作能力
 * 对外提供JSON/XML格式的API，供Web UI或外部系统查询MR作业运行状态和管理任务尝试
 */
@Singleton
@Path("/ws/v1/mapreduce")
public class AMWebServices {
  private final AppContext appCtx;
  private final App app;
  private final MRClientService service;

  @Context
  private HttpServletResponse response;
  
  /**
   * 构造AMWeb服务实例，依赖注入App和AppContext上下文
   * @param app AM Web应用实例
   * @param context Application上下文对象
   */
  @Inject
  public AMWebServices(final @Named("app") App app, final @Named("am") AppContext context) {
    this.appCtx = context;
    this.app = app;
    this.service = new MRClientService(context);
  }

  /**
   * 检查当前请求用户是否有权限访问指定作业
   * @param job 待访问的作业对象
   * @param request HTTP请求对象，包含请求用户信息
   * @return true有权限，false无权限
   */
  Boolean hasAccess(Job job, HttpServletRequest request) {
    String remoteUser = request.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    if (callerUGI != null && !job.checkAccess(callerUGI, JobACL.VIEW_JOB)) {
      return false;
    }
    return true;
  }

  /**
   * 初始化响应，清除ContentType供后续设置
   */
  private void init() {
    //clear content type
    response.setContentType(null);
  }

  /**
   * 从容器ID字符串中提取JobID并获取对应的作业对象，处理错误检查
   * @param cid 容器ID字符串
   * @param appCtx Application上下文对象
   * @return 对应的作业对象
   * @throws NotFoundException 当作业不存在或ID格式错误时抛出
   */
  public static Job getJobFromContainerIdString(String cid, AppContext appCtx)
      throws NotFoundException {
    //example container_e06_1724414851587_0004_01_000001
    String[] parts = cid.split("_");
    return getJobFromJobIdString(JobID.JOB + "_" + parts[2] + "_" + parts[3], appCtx);
  }


    /**
     * convert a job id string to an actual job and handle all the error checking.
     */
 /**
  * 将字符串格式的JobID转换为实际作业对象，处理格式错误和不存在场景
  * @param jid 字符串格式的JobID
  * @param appCtx Application上下文对象
  * @return 对应的作业对象
  * @throws NotFoundException 当作业不存在或ID格式错误时抛出
  */
 public static Job getJobFromJobIdString(String jid, AppContext appCtx) throws NotFoundException {
    JobId jobId;
    Job job;
    try {
      jobId = MRApps.toJobID(jid);
    } catch (YarnRuntimeException e) {
      // TODO: after MAPREDUCE-2793 YarnRuntimeException is probably not expected here
      // anymore but keeping it for now just in case other stuff starts failing.
      // Also, the webservice should ideally return BadRequest (HTTP:400) when
      // the id is malformed instead of NotFound (HTTP:404). The webserver on
      // top of which AMWebServices is built seems to automatically do that for
      // unhandled exceptions
      throw new NotFoundException(e.getMessage());
    } catch (IllegalArgumentException e) {
      throw new NotFoundException(e.getMessage());
    }
    if (jobId == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    job = appCtx.getJob(jobId);
    if (job == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    return job;
  }

  /**
   * convert a task id string to an actual task and handle all the error
   * checking.
   */
  /**
   * 将字符串格式的TaskID转换为实际任务对象，处理格式错误和不存在场景
   * @param tid 字符串格式的TaskID
   * @param job 任务所属作业对象
   * @return 对应的任务对象
   * @throws NotFoundException 当任务不存在或ID格式错误时抛出
   */
  public static Task getTaskFromTaskIdString(String tid, Job job) throws NotFoundException {
    TaskId taskID;
    Task task;
    try {
      taskID = MRApps.toTaskID(tid);
    } catch (YarnRuntimeException e) {
      // TODO: after MAPREDUCE-2793 YarnRuntimeException is probably not expected here
      // anymore but keeping it for now just in case other stuff starts failing.
      // Also, the webservice should ideally return BadRequest (HTTP:400) when
      // the id is malformed instead of NotFound (HTTP:404). The webserver on
      // top of which AMWebServices is built seems to automatically do that for
      // unhandled exceptions
      throw new NotFoundException(e.getMessage());
    } catch (NumberFormatException ne) {
      throw new NotFoundException(ne.getMessage());
    } catch (IllegalArgumentException e) {
      throw new NotFoundException(e.getMessage());
    } 
    if (taskID == null) {
      throw new NotFoundException("taskid " + tid + " not found or invalid");
    }
    task = job.getTask(taskID);
    if (task == null) {
      throw new NotFoundException("task not found with id " + tid);
    }
    return task;
  }

  /**
   * convert a task attempt id string to an actual task attempt and handle all
   * the error checking.
   */
  /**
   * 将字符串格式的TaskAttemptID转换为实际任务尝试对象，处理格式错误和不存在场景
   * @param attId 字符串格式的TaskAttemptID
   * @param task 任务尝试所属任务对象
   * @return 对应的任务尝试对象
   * @throws NotFoundException 当任务尝试不存在或ID格式错误时抛出
   */
  public static TaskAttempt getTaskAttemptFromTaskAttemptString(String attId, Task task)
      throws NotFoundException {
    TaskAttemptId attemptId;
    TaskAttempt ta;
    try {
      attemptId = MRApps.toTaskAttemptID(attId);
    } catch (YarnRuntimeException e) {
      // TODO: after MAPREDUCE-2793 YarnRuntimeException is probably not expected here
      // anymore but keeping it for now just in case other stuff starts failing.
      // Also, the webservice should ideally return BadRequest (HTTP:400) when
      // the id is malformed instead of NotFound (HTTP:404). The webserver on
      // top of which AMWebServices is built seems to automatically do that for
      // unhandled exceptions
      throw new NotFoundException(e.getMessage());
    } catch (NumberFormatException ne) {
      throw new NotFoundException(ne.getMessage());
    } catch (IllegalArgumentException e) {
      throw new NotFoundException(e.getMessage());
    }
    if (attemptId == null) {
      throw new NotFoundException("task attempt id " + attId
          + " not found or invalid");
    }
    ta = task.getAttempt(attemptId);
    if (ta == null) {
      throw new NotFoundException("Error getting info on task attempt id "
          + attId);
    }
    return ta;
  }


  /**
   * check for job access.
   *
   * @param job
   *          the job that is being accessed
   */
  /**
   * 检查当前用户对指定作业的访问权限，无权限则抛出未授权异常
   * @param job 待访问作业对象
   * @param request HTTP请求对象
   */
  void checkAccess(Job job, HttpServletRequest request) {
    if (!hasAccess(job, request)) {
      throw new WebApplicationException(Status.UNAUTHORIZED);
    }
  }

  /**
   * 获取ApplicationMaster基本信息的根接口
   * @return Application基本信息对象
   */
  @GET
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public AppInfo get() {
    return getAppInfo();
  }

  /**
   * 获取ApplicationMaster基本信息接口
   * @return Application基本信息对象
   */
  @GET
  @Path("/info")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public AppInfo getAppInfo() {
    init();
    return new AppInfo(this.app, this.app.context);
  }
  
  /**
   * 获取被黑名单节点列表接口，返回AM判定的不健康节点信息
   * @return 黑名单节点信息对象
   */
  @GET
  @Path("/blacklistednodes")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public BlacklistedNodesInfo getBlacklistedNodes() {
    init();
    return new BlacklistedNodesInfo(this.app.context);
  }

  /**
   * 获取当前AM所有作业列表接口，根据访问权限过滤作业
   * @param hsr HTTP请求对象
   * @return 作业列表信息对象
   */
  @GET
  @Path("/jobs")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public JobsInfo getJobs(@Context HttpServletRequest hsr) {
    init();
    JobsInfo allJobs = new JobsInfo();
    for (Job job : appCtx.getAllJobs().values()) {
      // getAllJobs only gives you a partial we want a full
      Job fullJob = appCtx.getJob(job.getID());
      if (fullJob == null) {
        continue;
      }
      allJobs.add(new JobInfo(fullJob, hasAccess(fullJob, hsr)));
    }
    return allJobs;
  }

  /**
   * 获取指定作业基本信息接口
   * @param hsr HTTP请求对象
   * @param jid 字符串格式的JobID
   * @return 作业信息对象
   */
  @GET
  @Path("/jobs/{jobid}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public JobInfo getJob(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {
    init();
    Job job = getJobFromJobIdString(jid, appCtx);
    return new JobInfo(job, hasAccess(job, hsr));
  }

  /**
   * 获取指定作业的AM尝试列表接口
   * @param jid 字符串格式的JobID
   * @return AM尝试列表信息对象
   */
  @GET
  @Path("/jobs/{jobid}/jobattempts")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public AMAttemptsInfo getJobAttempts(@PathParam("jobid") String jid) {
    init();
    Job job = getJobFromJobIdString(jid, appCtx);
    AMAttemptsInfo amAttempts = new AMAttemptsInfo();
    for (AMInfo amInfo : job.getAMInfos()) {
      AMAttemptInfo attempt = new AMAttemptInfo(amInfo, MRApps.toString(
            job.getID()), job.getUserName());
      amAttempts.add(attempt);
    }
    return amAttempts;
  }

  /**
   * 获取指定作业所有计数器信息接口
   * @param hsr HTTP请求对象
   * @param jid 字符串格式的JobID
   * @return 作业计数器信息对象
   */
  @GET
  @Path("/jobs/{jobid}/counters")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public JobCounterInfo getJobCounters(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {
    init();
    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    return new JobCounterInfo(this.appCtx, job);
  }