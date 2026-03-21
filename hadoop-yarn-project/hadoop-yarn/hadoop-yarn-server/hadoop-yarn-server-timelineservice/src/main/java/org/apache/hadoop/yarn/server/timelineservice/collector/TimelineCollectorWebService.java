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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import javax.inject.Singleton;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationAttemptEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.ClusterEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.ContainerEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.QueueEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.SubApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.UserEntity;
import org.apache.hadoop.yarn.server.timelineservice.metrics.PerNodeAggTimelineCollectorMetrics;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFound;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.IllegalFormatException;

/**
 * 时间线服务写入操作的每个节点REST端点入口，负责将请求路由到对应应用的收集器服务进行处理。
 */
@Private
@Unstable
@Singleton
@Path("/ws/v2/timeline")
public class TimelineCollectorWebService {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineCollectorWebService.class);

  @Context
  private ServletContext context;

  private static final PerNodeAggTimelineCollectorMetrics METRICS =
      PerNodeAggTimelineCollectorMetrics.getInstance();

  /**
   * 时间线收集器服务基本信息封装类，用于REST接口返回服务描述。
   */
  @XmlRootElement(name = "about")
  @XmlAccessorType(XmlAccessType.NONE)
  @Public
  @Unstable
  public static class AboutInfo {

    private String about;

    public AboutInfo() {

    }

    public AboutInfo(String abt) {
      this.about = abt;
    }

    @XmlElement(name = "About")
    public String getAbout() {
      return about;
    }

    public void setAbout(String abt) {
      this.about = abt;
    }

  }

  /**
   * 获取时间线Web服务描述信息。
   *
   * @param req Servlet请求对象
   * @param res Servlet响应对象
   * @return 时间线收集器服务描述信息
   */
  @GET
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public AboutInfo about(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res) {
    init(res);
    return new AboutInfo("Timeline Collector API");
  }

  /**
   * 接收时间线实体写入请求，路由到对应应用收集器处理。
   *
   * @param req Servlet请求对象
   * @param res Servlet响应对象
   * @param async 是否异步写入标识，true表示异步，null/其他表示同步
   * @param isSubAppEntities 是否为子应用实体写入标识
   * @param appId 目标应用ID
   * @param entities 待写入的时间线实体集合
   * @return 带对应HTTP状态码的响应
   */
  @PUT
  @Path("/entities")
  @Consumes({ MediaType.APPLICATION_JSON /* , MediaType.APPLICATION_XML */})
  public Response putEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @QueryParam("async") String async,
      @QueryParam("subappwrite") String isSubAppEntities,
      @QueryParam("appid") String appId,
      TimelineEntities entities) {
    init(res);
    // 获取请求发起用户信息
    UserGroupInformation callerUgi = getUser(req);
    // 解析是否异步写入
    boolean isAsync = async != null && async.trim().equalsIgnoreCase("true");
    // 用户身份校验
    if (callerUgi == null) {
      String msg = "The owner of the posted timeline entities is not set";
      LOG.error(msg);
      throw new ForbiddenException(msg);
    }

    // 记录请求开始时间，用于统计延迟
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    try {
      // 解析应用ID
      ApplicationId appID = parseApplicationId(appId);
      if (appID == null) {
        return Response.status(Response.Status.BAD_REQUEST).build();
      }
      // 从Servlet上下文获取节点级收集器管理器
      NodeTimelineCollectorManager collectorManager =
          (NodeTimelineCollectorManager) context.getAttribute(
              NodeTimelineCollectorManager.COLLECTOR_MANAGER_ATTR_KEY);
      // 获取对应应用的时间线收集器
      TimelineCollector collector = collectorManager.get(appID);
      if (collector == null) {
        LOG.error("Application: {} is not found", appId);
        throw new NotFoundException("Application: "+ appId + " is not found");
      }

      // 根据是否异步选择对应写入方式
      if (isAsync) {
        collector.putEntitiesAsync(processTimelineEntities(entities, appId,
            Boolean.valueOf(isSubAppEntities)), callerUgi);
      } else {
        collector.putEntities(processTimelineEntities(entities, appId,
            Boolean.valueOf(isSubAppEntities)), callerUgi);
      }

      succeeded = true;
      return Response.ok().build();
    } catch (NotFoundException | ForbiddenException e) {
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    } catch (IOException e) {
      LOG.error("Error putting entities", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    } catch (Exception e) {
      LOG.error("Unexpected error while putting entities", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    } finally {
      // 统计请求延迟
      long latency = Time.monotonicNow() - startTime;
      if (isAsync) {
        METRICS.addAsyncPutEntitiesLatency(latency, succeeded);
      } else {
        METRICS.addPutEntitiesLatency(latency, succeeded);
      }
    }
  }

  /**
   * 接收时间线域名写入请求，路由到对应应用收集器处理。
   *
   * @param req Servlet请求对象
   * @param res Servlet响应对象
   * @param domain 待写入的时间线域名
   * @param appId 目标应用ID
   * @return 带对应HTTP状态码的响应
   */
  @PUT
  @Path("/domain")
  @Consumes({ MediaType.APPLICATION_JSON /* , MediaType.APPLICATION_XML */ })
  public Response putDomain(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @QueryParam("appid") String appId,
      TimelineDomain domain) {
    init(res);
    // 获取请求发起用户信息
    UserGroupInformation callerUgi = getUser(req);
    // 用户身份校验
    if (callerUgi == null) {
      String msg = "The owner of the posted timeline entities is not set";
      LOG.error(msg);
      throw new ForbiddenException(msg);
    }

    try {
      // 解析应用ID
      ApplicationId appID = parseApplicationId(appId);
      if (appID == null) {
        return Response.status(Response.Status.BAD_REQUEST).build();
      }
      // 从Servlet上下文获取节点级收集器管理器
      NodeTimelineCollectorManager collectorManager =
          (NodeTimelineCollectorManager) context.getAttribute(
              NodeTimelineCollectorManager.COLLECTOR_MANAGER_ATTR_KEY);
      // 获取对应应用的时间线收集器
      TimelineCollector collector = collectorManager.get(appID);
      if (collector == null) {
        LOG.error("Application: {} is not found", appId);
        throw new NotFoundException("Application: " + appId + " is not found");
      }

      // 设置域名所有者为当前请求用户
      domain.setOwner(callerUgi.getShortUserName());
      // 委托收集器写入域名
      collector.putDomain(domain, callerUgi);

      return Response.ok().build();
    } catch (NotFoundException e) {
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    } catch (IOException e) {
      LOG.error("Error putting entities", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * 解析字符串格式的应用ID，转换为ApplicationId对象。
   * @param appId 字符串格式应用ID
   * @return 解析成功返回ApplicationId，格式错误或为空返回null
   */
  private static ApplicationId parseApplicationId(String appId) {
    try {
      if (appId != null) {
        return ApplicationId.fromString(appId.trim());
      } else {
        return null;
      }
    } catch (IllegalFormatException e) {
      LOG.error("Invalid application ID: {}", appId);
      return null;
    }
  }

  /**
   * 初始化HTTP响应，清空默认ContentType。
   * @param response HTTP响应对象
   */
  private static void init(HttpServletResponse response) {
    response.setContentType(null);
  }

  /**
   * 从HTTP请求中提取远程用户信息，创建对应的UGI对象。
   * @param req HTTP请求对象
   * @return 远程用户UGI对象，未获取到远程用户返回null
   */
  private static UserGroupInformation getUser(HttpServletRequest req) {
    String remoteUser = req.getRemoteUser();
    UserGroupInformation callerUgi = null;
    if (remoteUser != null) {
      callerUgi = UserGroupInformation.createRemoteUser(remoteUser);
    }
    return callerUgi;
  }

  /**
   * 将通用时间线实体转换为具体类型的实体对象，适配后端存储。
   * 保留该处理以便未来扩展聚合等功能。
   * @param entities 输入的通用时间线实体集合
   * @param appId 应用ID，用于子应用实体绑定
   * @param isSubAppWrite 是否为子应用写入
   * @return 处理后的具体类型实体集合
   */
  // The process may not be necessary according to the way we write the backend,
  // but let's keep it for now in case we need to use sub-classes APIs in the
  // future (e.g., aggregation).
  private static TimelineEntities processTimelineEntities(
      TimelineEntities entities, String appId, boolean isSubAppWrite) {
    TimelineEntities entitiesToReturn = new TimelineEntities();
    // 遍历每个实体进行类型转换
    for (TimelineEntity entity : entities.getEntities()) {
      TimelineEntityType type = null;
      try {
        type = TimelineEntityType.valueOf(entity.getType());
      } catch (IllegalArgumentException e) {
        type = null;
      }
      // 根据YARN预定义类型转换为对应具体实体类
      if (type != null) {
        switch (type) {
        case YARN_CLUSTER:
          entitiesToReturn.addEntity(new ClusterEntity(entity));
          break;
        case YARN_FLOW_RUN:
          entitiesToReturn.addEntity(new FlowRunEntity(entity));
          break;
        case YARN_APPLICATION:
          entitiesToReturn.addEntity(new ApplicationEntity(entity));
          break;
        case YARN_APPLICATION_ATTEMPT:
          entitiesToReturn.addEntity(new ApplicationAttemptEntity(entity));
          break;
        case YARN_CONTAINER:
          entitiesToReturn.addEntity(new ContainerEntity(entity));
          break;
        case YARN_QUEUE:
          entitiesToReturn.addEntity(new QueueEntity(entity));
          break;
        case YARN_USER:
          entitiesToReturn.addEntity(new UserEntity(entity));
          break;
        default:
          break;
        }
      } else {
        // 非预定义类型处理：子应用写入转换为SubApplicationEntity并绑定应用ID
        if (isSubAppWrite) {
          SubApplicationEntity se = new SubApplicationEntity(entity);
          se.setApplicationId(appId);
          entitiesToReturn.addEntity(se);
        } else {
          entitiesToReturn.addEntity(entity);
        }
      }
    }
    return entitiesToReturn;
  }
}