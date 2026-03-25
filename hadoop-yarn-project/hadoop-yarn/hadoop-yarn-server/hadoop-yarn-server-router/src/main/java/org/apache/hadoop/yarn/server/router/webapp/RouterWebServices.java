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

package org.apache.hadoop.yarn.server.router.webapp;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServiceProtocol;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppPriority;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppQueue;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterUserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.DelegationToken;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntryList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.RMQueueAclInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationSubmissionRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceOptionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.BulkActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServices.DEFAULT_ACTIVITIES_COUNT;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServices.DEFAULT_SUMMARIZE;

/**
 * RouterWebServices 是运行在每个Router上的REST服务，用于拦截并处理客户端发往集群ResourceManager的
 * {@link RMWebServiceProtocol} 请求。它接收客户端的REST请求，为每个用户创建请求拦截处理流水线，
 * 流水线由一系列 {@link RESTRequestInterceptor} 实例组成，可以按需检查和修改请求/响应。
 * 与AMRMProxyService的主要区别在于它实现的是ResourceManager的Web服务协议。
 **/
@Singleton
@Path(RMWSConsts.RM_WEB_SERVICE_PATH)
public class RouterWebServices implements RMWebServiceProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterWebServices.class);
  private final Router router;
  private final Configuration conf;
  private @Context HttpServletResponse response;

  private Map<String, RequestInterceptorChainWrapper> userPipelineMap;

  // -------Default values of QueryParams for RMWebServiceProtocol--------

  public static final String DEFAULT_QUEUE = "default";
  public static final String DEFAULT_RESERVATION_ID = "";
  public static final String DEFAULT_START_TIME = "0";
  public static final String DEFAULT_END_TIME = "-1";
  public static final String DEFAULT_INCLUDE_RESOURCE = "false";

  /**
   * 构造函数，依赖注入Router实例和配置对象.
   * @param router Router服务实例
   * @param conf 配置对象
   */
  @Inject
  public RouterWebServices(final @Named("router") Router router,
      @Named("conf")  Configuration conf) {
    this.router = router;
    this.conf = conf;
    // 从配置读取用户请求拦截流水线缓存最大大小，使用默认值兜底
    int maxCacheSize =
        conf.getInt(YarnConfiguration.ROUTER_PIPELINE_CACHE_MAX_SIZE,
            YarnConfiguration.DEFAULT_ROUTER_PIPELINE_CACHE_MAX_SIZE);
    // 创建线程安全的LRU缓存存储不同用户的拦截流水线
    this.userPipelineMap = Collections.synchronizedMap(new LRUCacheHashMap<>(maxCacheSize, true));
  }

  /**
   * 初始化操作，清空响应ContentType.
   */
  private void init() {
    // clear content type
    response.setContentType(null);
  }

  /**
   * 根据请求获取对应用户的请求拦截流水线.
   * @param hsr HTTP请求对象
   * @return 用户对应的拦截流水线包装对象
   */
  @VisibleForTesting
  protected RequestInterceptorChainWrapper getInterceptorChain(
      final HttpServletRequest hsr) {
    String user = "";
    if (hsr != null) {
      user = hsr.getRemoteUser();
    }
    try {
      // 如果请求中未携带用户信息，则使用当前登录用户
      if (user == null || user.equals("")) {
        // Yarn Router user
        user = UserGroupInformation.getCurrentUser().getUserName();
      }
    } catch (IOException e) {
      LOG.error("Cannot get user: {}", e.getMessage());
    }
    RequestInterceptorChainWrapper chain = userPipelineMap.get(user);
    // 如果缓存中已存在有效流水线，直接返回
    if (chain != null && chain.getRootInterceptor() != null) {
      return chain;
    }
    // 不存在则初始化新流水线并返回
    return initializePipeline(user);
  }

  /**
   * 获取所有用户的请求拦截流水线映射.
   * @return 用户到拦截流水线包装对象的映射
   */
  @VisibleForTesting
  protected Map<String, RequestInterceptorChainWrapper> getPipelines() {
    return this.userPipelineMap;
  }

  /**
   * 创建完整的请求拦截器责任链.
   * @return 责任链的第一个拦截器
   */
  @VisibleForTesting
  protected RESTRequestInterceptor createRequestInterceptorChain() {
    // 根据配置创建拦截器链
    return RouterServerUtil.createRequestInterceptorChain(conf,
        YarnConfiguration.ROUTER_WEBAPP_INTERCEPTOR_CLASS_PIPELINE,
        YarnConfiguration.DEFAULT_ROUTER_WEBAPP_INTERCEPTOR_CLASS,
        RESTRequestInterceptor.class);
  }

  /**
   * 为指定用户初始化请求拦截流水线.
   * @param user 用户名
   * @return 初始化完成的流水线包装对象
   */
  private RequestInterceptorChainWrapper initializePipeline(String user) {
    synchronized (this.userPipelineMap) {
      // 双重检查，避免重复初始化
      if (this.userPipelineMap.containsKey(user)) {
        LOG.info("Request to start an already existing user: {}"
            + " was received, so ignoring.", user);
        return userPipelineMap.get(user);
      }

      RequestInterceptorChainWrapper chainWrapper =
          new RequestInterceptorChainWrapper();
      try {
        // We should init the pipeline instance after it is created and then
        // add to the map, to ensure thread safe.
        LOG.info("Initializing request processing pipeline for user: {}.", user);

        // 创建拦截链并初始化
        RESTRequestInterceptor interceptorChain =
            this.createRequestInterceptorChain();
        interceptorChain.init(user);
        // 给拦截链注入Router客户端代理服务
        RouterClientRMService routerClientRMService = router.getClientRMProxyService();
        interceptorChain.setRouterClientRMService(routerClientRMService);
        chainWrapper.init(interceptorChain);
      } catch (Exception e) {
        LOG.error("Init RESTRequestInterceptor error for user: {}", user, e);
        throw e;
      }
      // 初始化完成后放入缓存
      this.userPipelineMap.put(user, chainWrapper);
      return chainWrapper;
    }
  }

  /**
   * 私有封装类，用于包装请求拦截链和关联的用户信息.
   */
  @Private
  public static class RequestInterceptorChainWrapper {
    private RESTRequestInterceptor rootInterceptor;

    /**
     * 使用指定的根拦截器初始化包装对象.
     * @param interceptor 责任链的根拦截器
     */
    public synchronized void init(RESTRequestInterceptor interceptor) {
      this.rootInterceptor = interceptor;
    }

    /**
     * 获取责任链的根拦截器.
     * @return 根拦截器
     */
    public synchronized RESTRequestInterceptor getRootInterceptor() {
      return rootInterceptor;
    }

    /**
     * 对象销毁时关闭拦截链.
     */
    @Override
    protected void finalize() {
      rootInterceptor.shutdown();
    }
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ClusterInfo get() {
    return getClusterInfo();
  }

  @GET
  @Path(RMWSConsts.INFO)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ClusterInfo getClusterInfo() {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().getClusterInfo();
  }

  @GET
  @Path(RMWSConsts.CLUSTER_USER_INFO)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ClusterUserInfo getClusterUserInfo(@Context HttpServletRequest hsr) {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().getClusterUserInfo(hsr);
  }

  @GET
  @Path(RMWSConsts.METRICS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ClusterMetricsInfo getClusterMetricsInfo() {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().getClusterMetricsInfo();
  }

  @GET
  @Path(RMWSConsts.SCHEDULER)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public SchedulerTypeInfo getSchedulerInfo() {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().getSchedulerInfo();
  }

  @POST
  @Path(RMWSConsts.SCHEDULER_LOGS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public String dumpSchedulerLogs(@FormParam(RMWSConsts.TIME) String time,
      @Context HttpServletRequest hsr) throws IOException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(hsr);
    return pipeline.getRootInterceptor().dumpSchedulerLogs(time, hsr);
  }

  @GET
  @Path(RMWSConsts.NODES)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public NodesInfo getNodes(@QueryParam(RMWSConsts.STATES) String states) {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().getNodes(states);
  }

  @GET
  @Path(RMWSConsts.NODES_NODEID)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public NodeInfo getNode(@PathParam(RMWSConsts.NODEID) String nodeId) {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().getNode(nodeId);
  }

  @POST
  @Path(RMWSConsts.NODE_RESOURCE)
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public ResourceInfo updateNodeResource(
      @Context HttpServletRequest hsr,
      @PathParam(RMWSConsts.NODEID) String nodeId,
      ResourceOptionInfo resourceOption) throws AuthorizationException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().updateNodeResource(
        hsr, nodeId, resourceOption);
  }

  @GET
  @Path(RMWSConsts.APPS)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Override
  public AppsInfo getApps(@Context HttpServletRequest hsr,
      @QueryParam(RMWSConsts.STATE) String stateQuery,
      @QueryParam(RMWSConsts.STATES) Set<String> statesQuery,
      @QueryParam(RMWSConsts.FINAL_STATUS) String finalStatusQuery,
      @QueryParam(RMWSConsts.USER) String userQuery,
      @QueryParam(RMWSConsts.QUEUE) String queueQuery,
      @QueryParam(RMWSConsts.LIMIT) String count,
      @QueryParam(RMWSConsts.STARTED_TIME_BEGIN) String startedBegin,
      @QueryParam(RMWSConsts.STARTED_TIME_END) String startedEnd,
      @QueryParam(RMWSConsts.FINISHED_TIME_BEGIN) String finishBegin,
      @QueryParam(RMWSConsts.FINISHED_TIME_END) String finishEnd,
      @QueryParam(RMWSConsts.APPLICATION_TYPES) Set<String> applicationTypes,
      @QueryParam(RMWSConsts.APPLICATION_TAGS) Set<String> applicationTags,
      @QueryParam(RMWSConsts.NAME) String name,
      @QueryParam(RMWSConsts.DESELECTS) Set<String> unselectedFields) {
    init();
    RequestInterceptorChainWrapper pipeline