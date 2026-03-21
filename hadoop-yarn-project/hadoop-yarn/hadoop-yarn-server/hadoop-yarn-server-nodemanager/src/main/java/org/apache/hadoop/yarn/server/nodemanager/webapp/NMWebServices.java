// 这个文件已经全部加上中文注释
/** * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.records.AuxServiceRecord;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.records.AuxServiceRecords;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AuxiliaryServicesInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMResourceInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfoes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.LogToolUtils;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMContainerLogsInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.webapp.YarnWebServiceParams;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * NodeManager REST API 入口实现，提供节点信息、应用、容器、日志等资源的HTTP访问接口
 */
@Singleton
@Path("/ws/v1/node")
public class NMWebServices {
  private static final Logger LOG =
       LoggerFactory.getLogger(NMWebServices.class);
  private Context nmContext;
  private ResourceView rview;
  private WebApp webapp;
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private String redirectWSUrl;
  private LogAggregationFileControllerFactory factory;
  private boolean filterAppsByUser = false;

  @javax.ws.rs.core.Context
  private HttpServletRequest request;
  
  @javax.ws.rs.core.Context
  private HttpServletResponse response;

  @javax.ws.rs.core.Context
  private UriInfo uriInfo;

  /**
   * 构造函数，通过依赖注入初始化NMWebService
   * @param nm NodeManager上下文对象
   * @param view 节点资源视图
   * @param webapp Web应用实例
   */
  @Inject
  public NMWebServices(final @javax.inject.Named("nm") Context nm,
      final @javax.inject.Named("view") ResourceView view,
      final @javax.inject.Named("webapp") WebApp webapp) {
    this.nmContext = nm;
    this.rview = view;
    this.webapp = webapp;
    // 从配置读取日志服务器重定向地址
    this.redirectWSUrl = this.nmContext.getConf().get(
        YarnConfiguration.YARN_LOG_SERVER_WEBSERVICE_URL);
    // 初始化日志聚合文件控制器工厂
    this.factory = new LogAggregationFileControllerFactory(
        this.nmContext.getConf());
    // 读取是否按用户过滤应用列表配置
    this.filterAppsByUser = this.nmContext.getConf().getBoolean(
        YarnConfiguration.FILTER_ENTITY_LIST_BY_USER,
        YarnConfiguration.DEFAULT_DISPLAY_APPS_FOR_LOGGED_IN_USER);
  }

  /**
   * 带response参数的构造函数，用于测试
   * @param nm NodeManager上下文
   * @param view 节点资源视图
   * @param webapp Web应用实例
   * @param response HTTP响应对象
   */
  public NMWebServices(final Context nm, final ResourceView view,
      final WebApp webapp, HttpServletResponse response) {
    this(nm, view, webapp);
    this.response = response;
  }

  /**
   * 初始化响应内容类型，清除默认ContentType
   */
  private void init() {
    //clear content type
    response.setContentType(null);
  }

  /**
   * 获取节点基础信息入口
   * @return 节点信息对象
   */
  @GET
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public NodeInfo get() {
    return getNodeInfo();
  }

  /**
   * 获取节点详细信息
   * @return 节点信息对象
   */
  @GET
  @Path("/info")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public NodeInfo getNodeInfo() {
    init();
    return new NodeInfo(this.nmContext, this.rview);
  }

  /**
   * 获取节点上运行的应用列表，支持按状态和用户过滤
   * @param hsr HTTP请求对象
   * @param stateQuery 状态过滤参数
   * @param userQuery 用户过滤参数
   * @return 应用列表信息
   */
  @GET
  @Path("/apps")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public AppsInfo getNodeApps(@javax.ws.rs.core.Context HttpServletRequest hsr,
      @QueryParam("state") String stateQuery,
      @QueryParam("user") String userQuery) {
    init();
    AppsInfo allApps = new AppsInfo();
    // 遍历所有应用
    for (Entry<ApplicationId, Application> entry : this.nmContext
        .getApplications().entrySet()) {

      AppInfo appInfo = new AppInfo(entry.getValue());
      // 按状态过滤
      if (stateQuery != null && !stateQuery.isEmpty()) {
        ApplicationState.valueOf(stateQuery);
        if (!appInfo.getState().equalsIgnoreCase(stateQuery)) {
          continue;
        }
      }
      // 按用户过滤
      if (userQuery != null) {
        if (userQuery.isEmpty()) {
          String msg = "Error: You must specify a non-empty string for the user";
          throw new BadRequestException(msg);
        }
        if (!appInfo.getUser().equals(userQuery)) {
          continue;
        }
      }

      // 开启按用户过滤后，仅允许应用所有者/管理员查看应用信息
      if (filterAppsByUser
          && !hasAccess(appInfo.getUser(), entry.getKey(), hsr)) {
        continue;
      }

      allApps.add(appInfo);
    }
    return allApps;
  }

  /**
   * 获取指定应用的详细信息
   * @param appId 应用ID字符串
   * @return 应用信息对象
   */
  @GET
  @Path("/apps/{appid}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public AppInfo getNodeApp(@PathParam("appid") String appId) {
    init();
    // 解析应用ID
    ApplicationId id = WebAppUtils.parseApplicationId(recordFactory, appId);
    Application app = this.nmContext.getApplications().get(id);
    if (app == null) {
      throw new NotFoundException("app with id " + appId + " not found");
    }
    return new AppInfo(app);

  }

  /**
   * 获取节点上所有容器列表
   * @param hsr HTTP请求对象
   * @return 容器列表信息
   */
  @GET
  @Path("/containers")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public ContainersInfo getNodeContainers(@javax.ws.rs.core.Context
      HttpServletRequest hsr) {
    init();
    ContainersInfo allContainers = new ContainersInfo();
    // 遍历所有容器
    for (Entry<ContainerId, Container> entry : this.nmContext.getContainers()
        .entrySet()) {
      if (entry.getValue() == null) {
        // just skip it
        continue;
      }
      ContainerInfo info = new ContainerInfo(this.nmContext, entry.getValue(),
          uriInfo.getBaseUri().toString(), webapp.name(), hsr.getRemoteUser());

      ApplicationId appId = entry.getKey().getApplicationAttemptId()
          .getApplicationId();
      // 仅允许应用所有者/管理员查看容器信息
      if (filterAppsByUser
          && !hasAccess(entry.getValue().getUser(), appId, hsr)) {
        continue;
      }

      allContainers.add(info);
    }
    return allContainers;
  }

  /**
   * 获取指定容器的详细信息
   * @param hsr HTTP请求对象
   * @param id 容器ID字符串
   * @return 容器信息对象
   */
  @GET
  @Path("/containers/{containerid}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public ContainerInfo getNodeContainer(@javax.ws.rs.core.Context
      HttpServletRequest hsr, @PathParam("containerid") String id) {
    ContainerId containerId;
    init();
    try {
      // 解析容器ID
      containerId = ContainerId.fromString(id);
    } catch (Exception e) {
      throw new BadRequestException("invalid container id, " + id);
    }

    Container container = nmContext.getContainers().get(containerId);
    if (container == null) {
      throw new NotFoundException("container with id, " + id + ", not found");
    }
    return new ContainerInfo(this.nmContext, container, uriInfo.getBaseUri()
        .toString(), webapp.name(), hsr.getRemoteUser());

  }

  /**
   * 返回容器日志文件的名称和当前大小信息
   *
   * @param hsr
   *    HttpServletRequest
   * @param res
   *    HttpServletResponse
   * @param containerIdStr
   *    容器ID
   * @return
   *    日志文件元信息
   */
  @GET
  @Path("/containers/{containerid}/logs")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public Response getContainerLogsInfo(
      @javax.ws.rs.core.Context HttpServletRequest hsr,
      @javax.ws.rs.core.Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr) {
    ContainerId containerId;
    init();
    try {
      containerId = ContainerId.fromString(containerIdStr);
    } catch (IllegalArgumentException ex) {
      throw new BadRequestException("invalid container id, " + containerIdStr);
    }

    try {
      List<ContainerLogsInfo> containersLogsInfo = new ArrayList<>();
      // 添加本地日志元信息
      containersLogsInfo.add(new NMContainerLogsInfo(
          this.nmContext, containerId,
          hsr.getRemoteUser(), ContainerLogAggregationType.LOCAL));
      // 获取聚合日志元信息（如果存在）
      ApplicationId appId = containerId.getApplicationAttemptId()
          .getApplicationId();
      Application app = this.nmContext.getApplications().get(appId);
      String appOwner = app == null ? null : app.getUser();
      try {
        ContainerLogsRequest logRequest = new ContainerLogsRequest();
        logRequest.setAppId(appId);
        logRequest.setAppOwner(appOwner);
        logRequest.setContainerId(containerIdStr);
        logRequest.setNodeId(this.nmContext.getNodeId().toString());
        List<ContainerLogMeta> containerLogMeta = factory
            .getFileControllerForRead(appId, appOwner)
            .readAggregatedLogsMeta(logRequest);
        if (!containerLogMeta.isEmpty()) {
          for (ContainerLogMeta logMeta : containerLogMeta) {
            containersLogsInfo.add(new ContainerLogsInfo(logMeta,
                ContainerLogAggregationType.AGGREGATED));
          }
        }
      } catch (IOException ex) {
        // 访问远程日志失败，忽略错误仅记录debug日志
        LOG.debug("{}", ex);
      }
      // 使用包装类保证Jersey 1 JSON向后兼容性
      ResponseBuilder resp = Response.ok().entity(new ContainerLogsInfoes(containersLogsInfo));
      // 添加安全响应头，防止MIME类型嗅探
      resp.header("X-Content-Type-Options", "nosniff");
      return resp.build();
    } catch (Exception ex) {
      // 如果配置了日志服务器，重定向到外部日志服务器
      if (redirectWSUrl == null || redirectWSUrl.isEmpty()) {
        throw new WebApplicationException(ex);
      }
      // redirect the request to the configured log server
      String redirectURI = "/containers/" + containerIdStr
          + "/logs";
      return createRedirectResponse(hsr, redirectWSUrl, redirectURI);
    }
  }

  /**
   * 返回容器指定日志文件的内容（纯文本）
   * 仅对仍在NodeManager内存中的容器有效，应用结束后本地日志不再可用
   *
   * @param containerIdStr
   *    容器ID
   * @param filename
   *    日志文件名
   * @param format
   *    响应内容类型
   * @param size
   *    返回日志最大字节数
   * @return
   *    日志文件内容响应
   */
  @GET
  @Path("/containers/{containerid}/logs/{filename}")
  @Produces({ MediaType.TEXT_PLAIN + "; " + JettyUtils.UTF_8 })
  @Public
  @Unstable