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

package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import java.util.Collections;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.server.webapp.LogServlet;
import org.apache.hadoop.yarn.server.webapp.WebServices;
import org.apache.hadoop.yarn.server.webapp.WrappedLogMetaRequest;
import org.apache.hadoop.yarn.server.webapp.YarnWebServiceParams;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;

/**
 * 应用历史服务(AHS) REST Web服务实现类
 * 提供查询已完成应用历史信息和容器日志的REST接口
 */
@Singleton
@Path("/ws/v1/applicationhistory")
public class AHSWebServices extends WebServices {

  private LogServlet logServlet;

  /**
   * 构造函数，注入应用基础协议和配置，初始化日志Servlet
   * @param appBaseProt 应用基础协议，用于查询应用信息
   * @param conf 配置对象
   */
  @Inject
  public AHSWebServices(
      final @Named("appBaseProt") ApplicationBaseProtocol appBaseProt,
      final @Named("conf") Configuration conf) {
    super(appBaseProt);
    this.logServlet = new LogServlet(conf, this);
  }

  /**
   * 获取应用历史服务基本信息接口
   * @param req HTTP请求
   * @param res HTTP响应
   * @return 时间线服务基本信息
   */
  @GET
  @Path("/about")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public TimelineAbout about(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res) {
    initForReadableEndpoints(res);
    return TimelineUtils.createTimelineAbout("Generic History Service API");
  }

  /**
   * 获取所有已完成应用列表接口（无过滤条件）
   * @param req HTTP请求
   * @param res HTTP响应
   * @return 应用列表信息
   */
  @GET
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public AppsInfo get(@Context HttpServletRequest req,
      @Context HttpServletResponse res) {
    return getApps(req, res, null, Collections.emptySet(), null, null,
      null, null, null, null, null, null, null,
        Collections.emptySet());
  }

  /**
   * 根据过滤条件获取已完成应用列表接口
   * @param req HTTP请求
   * @param res HTTP响应
   * @param stateQuery 应用状态查询（已弃用）
   * @param statesQuery 应用状态集合查询
   * @param finalStatusQuery 最终状态查询
   * @param userQuery 提交用户查询
   * @param queueQuery 队列查询
   * @param count 返回结果数量限制
   * @param startedBegin 启动时间起始查询
   * @param startedEnd 启动时间结束查询
   * @param finishBegin 完成时间起始查询
   * @param finishEnd 完成时间结束查询
   * @param name 应用名称查询
   * @param applicationTypes 应用类型集合查询
   * @return 过滤后的应用列表信息
   */
  @GET
  @Path("/apps")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppsInfo getApps(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @QueryParam("state") String stateQuery,
      @QueryParam("states") Set<String> statesQuery,
      @QueryParam("finalStatus") String finalStatusQuery,
      @QueryParam("user") String userQuery,
      @QueryParam("queue") String queueQuery,
      @QueryParam("limit") String count,
      @QueryParam("startedTimeBegin") String startedBegin,
      @QueryParam("startedTimeEnd") String startedEnd,
      @QueryParam("finishedTimeBegin") String finishBegin,
      @QueryParam("finishedTimeEnd") String finishEnd,
      @QueryParam("name") String name,
      @QueryParam("applicationTypes") Set<String> applicationTypes) {
    initForReadableEndpoints(res);
    validateStates(stateQuery, statesQuery);
    return super.getApps(req, res, stateQuery, statesQuery, finalStatusQuery,
      userQuery, queueQuery, count, startedBegin, startedEnd, finishBegin,
      finishEnd, name, applicationTypes);
  }

  /**
   * 获取指定应用的详细信息接口
   * @param req HTTP请求
   * @param res HTTP响应
   * @param appId 应用ID
   * @return 应用详细信息
   */
  @GET
  @Path("/apps/{appid}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppInfo getApp(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId) {
    initForReadableEndpoints(res);
    return super.getApp(req, res, appId);
  }

  /**
   * 获取指定应用的所有尝试attempt信息接口
   * @param req HTTP请求
   * @param res HTTP响应
   * @param appId 应用ID
   * @return 应用尝试attempt列表信息
   */
  @GET
  @Path("/apps/{appid}/appattempts")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppAttemptsInfo getAppAttempts(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId) {
    initForReadableEndpoints(res);
    return super.getAppAttempts(req, res, appId);
  }

  /**
   * 获取指定应用指定尝试attempt的详细信息接口
   * @param req HTTP请求
   * @param res HTTP响应
   * @param appId 应用ID
   * @param appAttemptId 尝试attempt ID
   * @return 应用尝试attempt详细信息
   */
  @GET
  @Path("/apps/{appid}/appattempts/{appattemptid}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppAttemptInfo getAppAttempt(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId,
      @PathParam("appattemptid") String appAttemptId) {
    initForReadableEndpoints(res);
    return super.getAppAttempt(req, res, appId, appAttemptId);
  }

  /**
   * 获取指定应用指定尝试attempt的所有容器信息接口
   * @param req HTTP请求
   * @param res HTTP响应
   * @param appId 应用ID
   * @param appAttemptId 尝试attempt ID
   * @return 容器列表信息
   */
  @GET
  @Path("/apps/{appid}/appattempts/{appattemptid}/containers")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ContainersInfo getContainers(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId,
      @PathParam("appattemptid") String appAttemptId) {
    initForReadableEndpoints(res);
    return super.getContainers(req, res, appId, appAttemptId);
  }

  /**
   * 获取指定容器的详细信息接口
   * @param req HTTP请求
   * @param res HTTP响应
   * @param appId 应用ID
   * @param appAttemptId 尝试attempt ID
   * @param containerId 容器ID
   * @return 容器详细信息
   */
  @GET
  @Path("/apps/{appid}/appattempts/{appattemptid}/containers/{containerid}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ContainerInfo getContainer(@Context HttpServletRequest req,
      @Context HttpServletResponse res, @PathParam("appid") String appId,
      @PathParam("appattemptid") String appAttemptId,
      @PathParam("containerid") String containerId) {
    initForReadableEndpoints(res);
    return super.getContainer(req, res, appId, appAttemptId, containerId);
  }

  /**
   * 验证应用状态查询参数，应用历史服务只允许查询终态应用
   * @param stateQuery 单个状态查询参数（已弃用）
   * @param statesQuery 状态集合查询参数
   */
  private static void
      validateStates(String stateQuery, Set<String> statesQuery) {
    // stateQuery is deprecated.
    if (stateQuery != null && !stateQuery.isEmpty()) {
      statesQuery.add(stateQuery);
    }
    Set<String> appStates = parseQueries(statesQuery, true);
    for (String appState : appStates) {
      switch (YarnApplicationState.valueOf(
          StringUtils.toUpperCase(appState))) {
        case FINISHED:
        case FAILED:
        case KILLED:
          continue;
        default:
          throw new BadRequestException("Invalid application-state " + appState
              + " specified. It should be a final state");
      }
    }
  }

  // TODO: YARN-6080: Create WebServiceUtils to have common functions used in
  //       RMWebService, NMWebService and AHSWebService.
  /**
   * 获取容器日志元信息接口，返回日志文件名和当前大小
   *
   * @param req HTTP请求
   * @param res HTTP响应
   * @param containerIdStr 容器ID
   * @param nmId NodeManager节点ID
   * @param redirectedFromNode 是否从NM重定向而来
   * @param manualRedirection 是否手动重定向
   * @return 日志元信息响应
   */
  @GET
  @Path("/containers/{containerid}/logs")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response getContainerLogsInfo(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    initForReadableEndpoints(res);

    WrappedLogMetaRequest.Builder logMetaRequestBuilder =
        LogServlet.createRequestFromContainerId(containerIdStr);

    return logServlet.getContainerLogsInfo(req, logMetaRequestBuilder, nmId,
        redirectedFromNode, null, manualRedirection);
  }

  /**
   * 获取容器指定日志文件的内容接口
   *
   * @param req HTTP请求
   * @param res HTTP响应
   * @param containerIdStr 容器ID
   * @param filename 日志文件名
   * @param format 响应内容格式
   * @param size 返回内容大小限制
   * @param nmId NodeManager节点ID
   * @param redirectedFromNode 是否从NM重定向而来
   * @param manualRedirection 是否手动重定向
   * @return 日志文件内容响应
   */
  @GET
  @Path("/containers/{containerid}/logs/{filename}")
  @Produces({ MediaType.TEXT_PLAIN })
  @Public
  @Unstable
  public Response getContainerLogFile(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @PathParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME) String filename,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_FORMAT) String format,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_SIZE) String size,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    return getLogs(req, res, containerIdStr, filename, format,
        size, nmId, redirectedFromNode, manualRedirection);
  }

  //TODO: YARN-4993: Refactory ContainersLogsBlock, AggregatedLogsBlock and
  //      container log webservice introduced in AHS to minimize
  //      the duplication.
  /**
   * 获取容器日志文件内容兼容接口
   *
   * @param req HTTP请求
   * @param res HTTP响应
   * @param containerIdStr 容器ID
   * @param filename 日志文件名
   * @param format 响应内容格式
   * @param size 返回内容大小限制
   * @param nmId NodeManager节点ID
   * @param redirectedFromNode 是否从NM重定向而来
   * @param manualRedirection 是否手动重定向
   * @return 日志文件内容响应
   */
  @GET
  @Path("/containerlogs/{containerid}/{filename}")
  @Produces({ MediaType.TEXT_PLAIN + "; " + JettyUtils.UTF_8 })
  @Public
  @Unstable
  public Response getLogs(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @PathParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME) String filename,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_FORMAT) String format,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_SIZE) String size,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    initForReadableEndpoints(res);
    return logServlet.getLogFile(req, containerIdStr, filename, format, size,
        nmId, redirectedFromNode, null, manualRedirection);
  }

  /**
   * 仅用于测试，获取日志Servlet实例
   * @return 日志Servlet实例
   */
  @VisibleForTesting
  @Private
  LogServlet getLogServlet() {
    return this.logServlet