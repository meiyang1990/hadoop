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

import static org.apache.hadoop.yarn.util.StringHelper.join;

import java.io.IOException;
import java.net.URLDecoder;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.AppInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明: MapReduce ApplicationMaster Web UI控制器，处理各个页面请求，负责权限校验、参数解析与页面渲染调度
 *
 * 类说明: MapReduce ApplicationMaster Web页面控制器，处理各类Web请求，渲染不同功能页面
 * 核心职责: 处理ApplicationMaster各个页面的路由请求，进行参数校验、权限检查，调度对应视图渲染页面
 */
public class AppController extends Controller implements AMParams {
  private static final Logger LOG =
      LoggerFactory.getLogger(AppController.class);
  private static final Joiner JOINER = Joiner.on("");
  
  protected final App app;
  
  /**
   * 构造函数，初始化ApplicationMaster Web控制器
   * @param app Application上下文对象，持有当前Application的全局状态
   * @param conf Hadoop配置对象
   * @param ctx 请求上下文对象
   * @param title 页面标题
   */
  protected AppController(App app, Configuration conf, RequestContext ctx,
      String title) {
    super(ctx);
    this.app = app;
    // 设置当前ApplicationID到请求上下文
    set(APP_ID, app.context.getApplicationID().toString());
    // 拼接ResourceManager Web地址，保存到上下文供页面跳转使用
    set(RM_WEB,
        JOINER.join(MRWebAppUtil.getYARNWebappScheme(),
            WebAppUtils.getResolvedRemoteRMWebAppURLWithoutScheme(conf,
                MRWebAppUtil.getYARNHttpPolicy())));
  }

  /**
   * 依赖注入构造函数，使用默认标题初始化控制器
   * @param app Application上下文对象
   * @param conf Hadoop配置对象
   * @param ctx 请求上下文对象
   */
  @Inject
  protected AppController(App app, Configuration conf, RequestContext ctx) {
    this(app, conf, ctx, "am");
  }

  /**
   * 处理首页请求，渲染ApplicationMaster默认首页
   */
  @Override public void index() {
    setTitle(join("MapReduce Application ", $(APP_ID)));
  }

  /**
   * 处理/info请求，渲染ApplicationMaster概览信息页
   */
  public void info() {
    AppInfo info = new AppInfo(app, app.context);
    info("Application Master Overview").
        __("Application ID:", info.getId()).
        __("Application Name:", info.getName()).
        __("User:", info.getUser()).
        __("Started on:", Times.format(info.getStartTime())).
        __("Elasped: ", org.apache.hadoop.util.StringUtils.formatTime(
          info.getElapsedTime() ));
    render(InfoPage.class);
  }

  /**
   * 获取作业页面视图类
   * @return 作业页面对应的视图类
   */
  protected Class<? extends View> jobPage() {
    return JobPage.class;
  }
  
  /**
   * 处理/job请求，渲染单个作业信息页面
   */
  public void job() {
    try {
      // 校验作业ID存在且有权限访问
      requireJob();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    render(jobPage());
  }

  /**
   * 获取作业计数器页面视图类
   * @return 作业计数器页面对应的视图类
   */
  protected Class<? extends View> countersPage() {
    return CountersPage.class;
  }
  
  /**
   * 处理/jobcounters请求，渲染作业计数器信息页面
   */
  public void jobCounters() {
    try {
      requireJob();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    if (app.getJob() != null) {
      setTitle(join("Counters for ", $(JOB_ID)));
    }
    render(countersPage());
  }
  
  /**
   * 处理/taskcounters请求，渲染单个任务计数器信息页面
   */
  public void taskCounters() {
    try {
      requireTask();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    if (app.getTask() != null) {
      setTitle(StringHelper.join("Counters for ", $(TASK_ID)));
    }
    render(countersPage());
  }
  
  /**
   * 获取单个计数器详情页面视图类
   * @return 单个计数器详情页面对应的视图类
   */
  protected Class<? extends View> singleCounterPage() {
    return SingleCounterPage.class;
  }
  
  /**
   * 处理/singlejobcounter请求，渲染作业单个计数器详情页面
   * @throws IOException 解码或IO异常
   */
  public void singleJobCounter() throws IOException{
    try {
      requireJob();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    // URL解码计数器组名，保存到上下文
    set(COUNTER_GROUP, URLDecoder.decode($(COUNTER_GROUP), "UTF-8"));
    // URL解码计数器名称，保存到上下文
    set(COUNTER_NAME, URLDecoder.decode($(COUNTER_NAME), "UTF-8"));
    if (app.getJob() != null) {
      setTitle(StringHelper.join($(COUNTER_GROUP)," ",$(COUNTER_NAME),
          " for ", $(JOB_ID)));
    }
    render(singleCounterPage());
  }
  
  /**
   * 处理/singletaskcounter请求，渲染任务单个计数器详情页面
   * @throws IOException 解码或IO异常
   */
  public void singleTaskCounter() throws IOException{
    try {
      requireTask();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    set(COUNTER_GROUP, URLDecoder.decode($(COUNTER_GROUP), "UTF-8"));
    set(COUNTER_NAME, URLDecoder.decode($(COUNTER_NAME), "UTF-8"));
    if (app.getTask() != null) {
      setTitle(StringHelper.join($(COUNTER_GROUP)," ",$(COUNTER_NAME),
          " for ", $(TASK_ID)));
    }
    render(singleCounterPage());
  }

  /**
   * 获取任务列表页面视图类
   * @return 任务列表页面对应的视图类
   */
  protected Class<? extends View> tasksPage() {
    return TasksPage.class;
  }
  
  /**
   * 处理/tasks请求，渲染任务列表页面
   */
  public void tasks() {
    try {
      requireJob();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    if (app.getJob() != null) {
      try {
        String tt = $(TASK_TYPE);
        // 任务类型为空默认显示所有任务
        tt = tt.isEmpty() ? "All" : StringUtils.capitalize(
            org.apache.hadoop.util.StringUtils.toLowerCase(
                MRApps.taskType(tt).toString()));
        setTitle(join(tt, " Tasks for ", $(JOB_ID)));
      } catch (Exception e) {
        LOG.error("Failed to render tasks page with task type : "
            + $(TASK_TYPE) + " for job id : " + $(JOB_ID), e);
        badRequest(e.getMessage());
      }
    }
    render(tasksPage());
  }
  
  /**
   * 获取单个任务页面视图类
   * @return 单个任务页面对应的视图类
   */
  protected Class<? extends View> taskPage() {
    return TaskPage.class;
  }
  
  /**
   * 处理/task请求，渲染单个任务信息页面
   */
  public void task() {
    try {
      requireTask();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    if (app.getTask() != null) {
      setTitle(join("Attempts for ", $(TASK_ID)));
    }
    render(taskPage());
  }

  /**
   * 获取任务尝试列表页面视图类
   * @return 任务尝试列表页面对应的视图类
   */
  protected Class<? extends View> attemptsPage() {
    return AttemptsPage.class;
  }
  
  /**
   * 处理/attempts请求，渲染按状态过滤的任务尝试列表页面
   */
  public void attempts() {
    try {
      requireJob();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    if (app.getJob() != null) {
      try {
        String taskType = $(TASK_TYPE);
        if (taskType.isEmpty()) {
          throw new RuntimeException("missing task-type.");
        }
        String attemptState = $(ATTEMPT_STATE);
        if (attemptState.isEmpty()) {
          throw new RuntimeException("missing attempt-state.");
        }
        setTitle(join(attemptState, " ",
            MRApps.taskType(taskType).toString(), " attempts in ", $(JOB_ID)));

        render(attemptsPage());
      } catch (Exception e) {
        LOG.error("Failed to render attempts page with task type : "
            + $(TASK_TYPE) + " for job id : " + $(JOB_ID), e);
        badRequest(e.getMessage());
      }
    }
  }

  /**
   * 获取作业配置页面视图类
   * @return 作业配置页面对应的视图类
   */
  protected Class<? extends View> confPage() {
    return JobConfPage.class;
  }

  /**
   * 处理/conf请求，渲染作业配置展示页面
   */
  public void conf() {
    try {
      requireJob();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    render(confPage());
  }

  /**
   * 处理/downloadConf请求，处理作业配置文件下载请求
   */
  public void downloadConf() {
    try {
      requireJob();
    } catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    writeJobConf();
  }

  /**
   * 将作业配置写入HTTP响应流，提供下载
   */
  private void writeJobConf() {
    String jobId = $(JOB_ID);
    assert(!jobId.isEmpty());

    JobId jobID = MRApps.toJobID($(JOB_ID));
    Job job = app.context.getJob(jobID);
    assert(job != null);

    try {
      Configuration jobConf = job.loadConfFile();
      // 设置响应内容类型为XML
      response().setContentType("text/xml");
      // 设置下载响应头，指定文件名
      response().setHeader("Content-Disposition",
          "attachment; filename=" + jobId + ".xml");
      // 将配置写入响应输出流
      jobConf.writeXml(writer());
    } catch (IOException e) {
      LOG.error("Error reading/writing job" +
          " conf file for job: " + jobId, e);
      renderText(e.getMessage());
    }
  }

  /**
   * 处理错误请求，设置400状态码和错误标题
   * @param s 错误信息
   */
  void badRequest(String s) {
    setStatus(HttpServletResponse.SC_BAD_REQUEST);
    String title = "Bad request: ";
    setTitle((s != null) ? join(title, s) : title);
  }

  /**
   * 处理资源未找到，设置404状态码和错误标题
   * @param s 错误信息
   */
  void notFound(String s) {
    setStatus(HttpServletResponse.SC_NOT_FOUND);
    setTitle(join("Not found: ", s));
  }
  
  /**
   * 处理访问拒绝，设置403状态码和错误标题
   * @param s 错误信息
   */
  void accessDenied(String s) {
    setStatus(HttpServletResponse.SC_FORBIDDEN);
    setTitle(join("Access denied: ", s));
  }

  /**
   * 检查当前请求用户是否有权限访问指定作业
   * @param job 待访问的作业对象
   * @return 有权限返回true，否则返回false
   */
  boolean checkAccess(Job job) {
    String remoteUser = request().getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    // 用户存在且没有查看权限，返回false
    if (callerUGI != null && !job.checkAccess(callerUGI, JobACL.VIEW_JOB)) {
      return false;
    }
    return true;
  }

  /**
   * 校验请求中作业ID存在、作业存在且当前用户有权限访问，校验失败抛出异常
   */
  public void requireJob() {
    if ($(JOB_ID).isEmpty()) {
      badRequest("missing job ID");
      throw new RuntimeException("Bad Request: Missing job ID");
    }

    JobId jobID = MRApps.toJobID($(JOB_ID));
    app.setJob(app.context.getJob(jobID));
    if (app.getJob() == null) {
      notFound($(JOB_ID));
      throw new RuntimeException("Not Found: " + $(JOB_ID));
    }

    /* check for acl access */
    Job job = app.context.getJob(jobID);
    if (!checkAccess(job)) {
      accessDenied("User " + request().getRemoteUser() + " does not have " +
          " permission to view job " + $(JOB_ID));
      throw new RuntimeException("Access denied: User " +
          request().getRemoteUser() + " does not have permission to view job " +
          $(JOB_ID));
    }
  }

  /**
   * 校验请求中任务ID存在、任务存在且当前用户有权限访问，校验失败抛出异常
   */
  public void requireTask() {
    if ($(TASK_ID).isEmpty()) {
      badRequest("missing task ID");
      throw new RuntimeException("missing task ID");
    }

    TaskId taskID = MRApps.toTaskID($(TASK_ID));
    Job job = app.context.getJob(taskID.getJobId());
    app.setJob(job);
    if (app.getJob() == null) {
      notFound(MRApps.toString(taskID.getJobId()));
      throw new RuntimeException("Not Found: " + $(JOB_ID));
    } else {
      app.setTask(app.getJob().getTask(taskID));
      if (app.getTask() == null) {
        notFound($(TASK_ID));
        throw new RuntimeException("Not Found: " + $(TASK_ID));
      }
    }
    if (!checkAccess(job)) {
      accessDenied("User " + request().getRemoteUser() + " does not have " +
          " permission to view job " + $(JOB_ID));
      throw new RuntimeException("Access denied: User " +
          request().getRemoteUser() + " does not have permission to view job " +
          $(JOB_ID));
    }
  }
}