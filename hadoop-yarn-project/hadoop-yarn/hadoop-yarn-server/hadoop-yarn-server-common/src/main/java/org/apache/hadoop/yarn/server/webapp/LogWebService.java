// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.webapp;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.security.PrivilegedExceptionAction;

/**
 * YARN应用日志REST Web服务实现，仅支持ATSv2（时间线服务V2），提供容器日志查询能力。
 */
@Singleton
@Path("/ws/v2/applicationlog")
public class LogWebService implements AppInfoProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(LogWebService.class);
  // ATSv2服务基础URI路径
  private static final String RESOURCE_URI_STR_V2 = "/ws/v2/timeline/";
  // NodeManager日志下载URI路径
  private static final String NM_DOWNLOAD_URI_STR = "/ws/v1/node/containers";
  // 路径拼接器
  private static final Joiner JOINER = Joiner.on("");
  private static Configuration yarnConf = new YarnConfiguration();
  // 日志聚合文件控制器工厂
  private static LogAggregationFileControllerFactory factory;
  // ATSv2服务基础地址
  private static String base;
  // 默认集群ID
  private static String defaultClusterid;

  private final LogServlet logServlet;
  // 时间线服务REST客户端，双重检查锁延迟初始化
  private volatile Client webTimelineClient;

  static {
    init();
  }

  /**
   * 初始化公共静态资源，初始化顺序很重要。
   */
  // initialize all the common resources - order is important
  private static void init() {
    // 创建日志聚合文件控制器工厂
    factory = new LogAggregationFileControllerFactory(yarnConf);
    // 拼接ATSv2服务完整基础地址
    base = JOINER.join(WebAppUtils.getHttpSchemePrefix(yarnConf),
        WebAppUtils.getTimelineReaderWebAppURLWithoutScheme(yarnConf),
        RESOURCE_URI_STR_V2);
    // 读取默认集群ID配置
    defaultClusterid = yarnConf.get(YarnConfiguration.RM_CLUSTER_ID,
        YarnConfiguration.DEFAULT_RM_CLUSTER_ID);
    LOG.info("Initialized LogWeService with clusterid {} for URI: {}.",
        defaultClusterid, base);
  }

  /**
   * 构造函数，初始化日志Servlet。
   */
  public LogWebService() {
    this.logServlet = new LogServlet(yarnConf, this);
  }

  /**
   * 创建支持Hadoop认证的时间线服务REST客户端。
   * @return 配置好的REST客户端实例
   */
  private Client createTimelineWebClient() {
    ClientConfig cfg = new ClientConfig();
    // 注册JSON序列化提供者
    cfg.register(YarnJacksonJaxbJsonProvider.class);

    // 创建自定义连接工厂，使用Hadoop认证URL打开连接
    HttpUrlConnectorProvider httpUrlConnectorProvider =
        new HttpUrlConnectorProvider().connectionFactory(url -> {
          AuthenticatedURL.Token token = new AuthenticatedURL.Token();
          HttpURLConnection conn;
          try {
            conn = new AuthenticatedURL().openConnection(url, token);
            LOG.info("LogWeService:Connecetion created.");
          } catch (AuthenticationException e) {
            throw new IOException(e);
          }
          return conn;
        });
    cfg.connectorProvider(httpUrlConnectorProvider);

    return ClientBuilder.newBuilder().withConfig(cfg).build();
  }

  /**
   * 初始化响应，清空默认内容类型以便后续自行设置。
   * @param response HTTP响应对象
   */
  private void initForReadableEndpoints(HttpServletResponse response) {
    // clear content type
    response.setContentType(null);
  }

  /**
   * 获取容器日志元信息（日志文件名及当前文件大小）。
   *
   * @param req                HttpServletRequest
   * @param res                HttpServletResponse
   * @param containerIdStr     容器ID
   * @param nmId               NodeManager节点ID
   * @param redirectedFromNode 是否是从NM重定向来的请求
   * @param clusterId          集群ID
   * @param manualRedirection  是否手动重定向（返回Location头而非自动跳转）
   * @return 包含日志元信息的响应
   */
  @GET
  @Path("/containers/{containerid}/logs")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response getContainerLogsInfo(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.CLUSTER_ID) String clusterId,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    initForReadableEndpoints(res);

    // 根据容器ID构建日志元信息请求
    WrappedLogMetaRequest.Builder logMetaRequestBuilder =
        LogServlet.createRequestFromContainerId(containerIdStr);

    // 委托LogServlet处理请求
    return logServlet.getContainerLogsInfo(req, logMetaRequestBuilder, nmId,
        redirectedFromNode, clusterId, manualRedirection);
  }

  /**
   * 从时间线服务查询容器所在NodeManager的HTTP地址。
   * @param req HTTP请求
   * @param appId 应用ID
   * @param appAttemptId 应用尝试ID
   * @param containerId 容器ID
   * @param clusterId 集群ID
   * @return NodeManager HTTP地址，查询失败返回null
   */
  @Override
  public String getNodeHttpAddress(HttpServletRequest req, String appId,
      String appAttemptId, String containerId, String clusterId) {
    // 获取请求发起者用户信息
    UserGroupInformation callerUGI = LogWebServiceUtils.getUser(req);
    // 使用传入集群ID或默认集群ID
    String cId = clusterId != null ? clusterId : defaultClusterid;
    // 构造查询参数，只需要INFO字段
    MultivaluedMap<String, String> params = new MultivaluedHashMap();
    params.add("fields", "INFO");
    // 构造时间线服务查询路径
    String path = JOINER.join("clusters/", cId, "/apps/", appId, "/entities/",
        TimelineEntityType.YARN_CONTAINER.toString(), "/", containerId);
    TimelineEntity conEntity = null;
    try {
      // 无用户信息直接查询，否则以调用者身份特权查询
      if (callerUGI == null) {
        conEntity = getEntity(path, params);
      } else {
        setUserName(params, callerUGI.getShortUserName());
        conEntity =
            callerUGI.doAs(new PrivilegedExceptionAction<TimelineEntity>() {
              @Override public TimelineEntity run() throws Exception {
                return getEntity(path, params);
              }
            });
      }
    } catch (Exception e) {
      LogWebServiceUtils.rewrapAndThrowException(e);
    }
    if (conEntity == null) {
      return null;
    }
    // 从时间线实体信息中提取容器分配的NM HTTP地址
    return (String) conEntity.getInfo()
        .get(ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO);
  }

  /**
   * 从时间线服务查询应用基本信息（状态和拥有者）。
   * @param req HTTP请求
   * @param appId 应用ID
   * @param clusterId 集群ID
   * @return 应用基本信息，查询失败返回null
   */
  @Override
  public BasicAppInfo getApp(HttpServletRequest req, String appId,
      String clusterId) {
    // 获取请求发起者用户信息
    UserGroupInformation callerUGI = LogWebServiceUtils.getUser(req);

    // 使用传入集群ID或默认集群ID
    String cId = clusterId != null ? clusterId : defaultClusterid;
    // 构造查询参数，只需要INFO字段
    MultivaluedMap<String, String> params = new MultivaluedHashMap();
    params.add("fields", "INFO");
    // 构造时间线服务查询路径
    String path = JOINER.join("clusters/", cId, "/apps/", appId);
    TimelineEntity appEntity = null;

    try {
      // 无用户信息直接查询，否则以调用者身份特权查询
      if (callerUGI == null) {
        appEntity = getEntity(path, params);
      } else {
        setUserName(params, callerUGI.getShortUserName());
        appEntity =
            callerUGI.doAs(new PrivilegedExceptionAction<TimelineEntity>() {
              @Override public TimelineEntity run() throws Exception {
                return getEntity(path, params);
              }
            });
      }
    } catch (Exception e) {
      LogWebServiceUtils.rewrapAndThrowException(e);
    }

    if (appEntity == null) {
      return null;
    }
    // 从时间线实体信息中提取应用拥有者和状态
    String appOwner = (String) appEntity.getInfo()
        .get(ApplicationMetricsConstants.USER_ENTITY_INFO);
    String state = (String) appEntity.getInfo()
        .get(ApplicationMetricsConstants.STATE_EVENT_INFO);
    YarnApplicationState appState = YarnApplicationState.valueOf(state);
    return new BasicAppInfo(appState, appOwner);
  }

  /**
   * 获取指定容器指定日志文件的内容，返回纯文本格式。
   *
   * @param req                HttpServletRequest
   * @param res                HttpServletResponse
   * @param containerIdStr     容器ID
   * @param filename           日志文件名
   * @param format             响应内容格式
   * @param size               需要返回的日志文件大小
   * @param nmId               NodeManager节点ID
   * @param redirectedFromNode 是否是从NM重定向来的请求
   * @param clusterId          集群ID
   * @param manualRedirection  是否手动重定向（返回Location头而非自动跳转）
   * @return 包含日志文件内容的响应
   */
  @GET
  @Path("/containers/{containerid}/logs/{filename}")
  @Produces({ MediaType.TEXT_PLAIN })
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public Response getContainerLogFile(
      @Context HttpServletRequest req, @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @PathParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME) String filename,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_FORMAT) String format,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_SIZE) String size,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
          boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.CLUSTER_ID) String clusterId,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    return getLogs(req, res, containerIdStr, filename, format, size, nmId,
        redirectedFromNode, clusterId, manualRedirection);
  }

  //TODO: YARN-4993: Refactory ContainersLogsBlock, AggregatedLogsBlock and
  //      container log webservice introduced in AHS to minimize
  //      the duplication.
  /**
   * 获取容器日志文件内容的兼容接口，与旧路径格式兼容。
   *
   * @param req                HttpServletRequest
   * @param res                HttpServletResponse
   * @param containerIdStr     容器ID
   * @param filename           日志文件名
   * @param format             响应内容格式
   * @param size               需要返回的日志文件大小
   * @param nmId               NodeManager节点ID
   * @param redirectedFromNode 是否是从NM重定向来的请求
   * @param clusterId          集群ID
   * @param manualRedirection  是否手动重定向（返回Location头而非自动跳转）
   * @return 包含日志文件内容的响应
   */
  @GET
  @Path("/containerlogs/{containerid}/{filename}")
  @Produces({ MediaType.TEXT_PLAIN + "; " + JettyUtils.UTF_8 })
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public Response getLogs(@Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam(YarnWebServiceParams.CONTAINER_ID) String containerIdStr,
      @PathParam(YarnWebServiceParams.CONTAINER_LOG_FILE_NAME) String filename,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_FORMAT) String format,
      @QueryParam(YarnWebServiceParams.RESPONSE_CONTENT_SIZE) String size,
      @QueryParam(YarnWebServiceParams.NM_ID) String nmId,
      @QueryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE)
      @DefaultValue("false") boolean redirectedFromNode,
      @QueryParam(YarnWebServiceParams.CLUSTER_ID) String clusterId,
      @QueryParam(YarnWebServiceParams.MANUAL_REDIRECTION)
      @DefaultValue("false") boolean manualRedirection) {
    initForReadableEndpoints(res);
    // 委托LogServlet处理日志文件获取逻辑
    return logServlet.getLogFile(req, containerIdStr, filename, format, size,
        nmId, redirectedFromNode, clusterId, manualRedirection);
  }

  /**
   * 从时间线服务查询指定实体数据。
   * @param path 查询路径
   * @param params 查询参数
   * @return 时间线实体对象
   * @throws IOException 查询失败或服务返回错误时抛出
   */
  @VisibleForTesting protected TimelineEntity getEntity(String path,
      MultivaluedMap<String, String> params) throws IOException {
    // 向时间线服务发起GET请求
    Response resp =
        getClient().target(base).path(path)
            .request(MediaType.APPLICATION_JSON)
            .get(Response.class);
    // 检查响应状态，非OK则抛出异常
    if (resp == null
        || resp.getStatusInfo().getStatusCode() != Response.Status.OK
        .getStatusCode()) {
      String msg =
          "Response from the timeline reader server is " + ((resp == null) ?
              "null" :
              "not successful," + " HTTP error code: " + resp.getStatus()
                  + ", Server response:\n" + resp.readEntity(String.class));
      LOG.error(msg);
      throw new IOException(msg);
    }
    // 解析JSON响应为时间线实体对象
    TimelineEntity entity = resp.readEntity(TimelineEntity.class);
    return entity;
  }

  /**
   * 获取时间线服务REST客户端，使用双重检查锁实现