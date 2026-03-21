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

package org.apache.hadoop.yarn.server.timeline.webapp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.EntityIdentifier;
import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Timeline时间线服务REST API实现类，提供时间线数据的查询、写入和域名管理的HTTP接口
 */
@Singleton
@Path("/ws/v1/timeline")
//TODO: support XML serialization/deserialization
public class TimelineWebServices {

  private static final Logger LOG = LoggerFactory
      .getLogger(TimelineWebServices.class);

  private TimelineDataManager timelineDataManager;

  /**
   * 构造函数，注入时间线数据管理器实例
   * @param timelineDataManager 时间线数据管理器
   */
  @Inject
  public TimelineWebServices(TimelineDataManager timelineDataManager) {
    this.timelineDataManager = timelineDataManager;
  }

  /**
   * 获取时间线服务API描述信息
   * @param req HTTP请求
   * @param res HTTP响应
   * @return 时间线API描述信息
   */
  @GET
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public TimelineAbout about(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res) {
    init(res);
    return TimelineUtils.createTimelineAbout("Timeline API");
  }

  /**
   * 根据查询条件获取匹配的实体列表
   * @param req HTTP请求
   * @param res HTTP响应
   * @param entityType 实体类型
   * @param primaryFilter 主过滤条件
   * @param secondaryFilter 二级过滤条件
   * @param windowStart 时间窗口起始
   * @param windowEnd 时间窗口结束
   * @param fromId 分页起始实体ID
   * @param fromTs 分页起始时间戳
   * @param limit 返回结果数量限制
   * @param fields 需要返回的字段列表
   * @return 匹配的实体集合
   */
  @GET
  @Path("/{entityType}")
  @Consumes({ MediaType.APPLICATION_JSON })
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public TimelineEntities getEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("entityType") String entityType,
      @QueryParam("primaryFilter") String primaryFilter,
      @QueryParam("secondaryFilter") String secondaryFilter,
      @QueryParam("windowStart") String windowStart,
      @QueryParam("windowEnd") String windowEnd,
      @QueryParam("fromId") String fromId,
      @QueryParam("fromTs") String fromTs,
      @QueryParam("limit") String limit,
      @QueryParam("fields") String fields) {
    init(res);
    try {
      TimelineEntities entities = timelineDataManager.getEntities(
              parseStr(entityType),
              parsePairStr(primaryFilter, ":"),
              parsePairsStr(secondaryFilter, ",", ":"),
              parseLongStr(windowStart),
              parseLongStr(windowEnd),
              parseStr(fromId),
              parseLongStr(fromTs),
              parseLongStr(limit),
              parseFieldsStr(fields, ","),
              getUser(req));
      return entities;
    } catch (NumberFormatException e) {
      throw new BadRequestException(
        "windowStart, windowEnd, fromTs or limit is not a numeric value: " + e);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("requested invalid field: " + e);
    } catch (Exception e) {
      LOG.error("Error getting entities", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * 根据实体类型和ID获取单个实体详情
   * @param req HTTP请求
   * @param res HTTP响应
   * @param entityType 实体类型
   * @param entityId 实体ID
   * @param fields 需要返回的字段列表
   * @return 实体详情
   */
  @GET
  @Path("/{entityType}/{entityId}")
  @Consumes({ MediaType.APPLICATION_JSON /* , MediaType.APPLICATION_XML */})
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public TimelineEntity getEntity(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("entityType") String entityType,
      @PathParam("entityId") String entityId,
      @QueryParam("fields") String fields) {
    init(res);
    TimelineEntity entity = null;
    try {
      entity = timelineDataManager.getEntity(
          parseStr(entityType),
          parseStr(entityId),
          parseFieldsStr(fields, ","),
          getUser(req));
    } catch (YarnException e) {
      // 用户无权限覆盖已存在的域名
      LOG.info(e.getMessage(), e);
      throw new ForbiddenException(e);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    } catch (Exception e) {
      LOG.error("Error getting entity", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    if (entity == null) {
      throw new NotFoundException("Timeline entity "
          + new EntityIdentifier(parseStr(entityId), parseStr(entityType))
          + " is not found");
    }
    return entity;
  }

  /**
   * 根据查询条件获取匹配的事件列表
   * @param req HTTP请求
   * @param res HTTP响应
   * @param entityType 实体类型
   * @param entityId 实体ID列表
   * @param eventType 事件类型列表
   * @param windowStart 时间窗口起始
   * @param windowEnd 时间窗口结束
   * @param limit 返回结果数量限制
   * @return 匹配的事件集合
   */
  @GET
  @Path("/{entityType}/events")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public TimelineEvents getEvents(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("entityType") String entityType,
      @QueryParam("entityId") String entityId,
      @QueryParam("eventType") String eventType,
      @QueryParam("windowStart") String windowStart,
      @QueryParam("windowEnd") String windowEnd,
      @QueryParam("limit") String limit) {
    init(res);
    try {
      return timelineDataManager.getEvents(
          parseStr(entityType),
          parseArrayStr(entityId, ","),
          parseArrayStr(eventType, ","),
          parseLongStr(windowStart),
          parseLongStr(windowEnd),
          parseLongStr(limit),
          getUser(req));
    } catch (NumberFormatException e) {
      throw (BadRequestException)new BadRequestException(
          "windowStart, windowEnd or limit is not a numeric value.")
          .initCause(e);
    } catch (Exception e) {
      LOG.error("Error getting entity timelines", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * 将一批实体写入时间线存储，返回写入过程中发生的错误
   * @param req HTTP请求
   * @param res HTTP响应
   * @param entities 待写入的实体集合
   * @return 写入响应，包含错误信息
   */
  @POST
  @Consumes({ MediaType.APPLICATION_JSON /* , MediaType.APPLICATION_XML */})
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public TimelinePutResponse postEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      TimelineEntities entities) {
    init(res);
    UserGroupInformation callerUGI = getUser(req);
    if (callerUGI == null) {
      String msg = "The owner of the posted timeline entities is not set";
      LOG.error(msg);
      throw new ForbiddenException(msg);
    }
    try {
      return timelineDataManager.postEntities(entities, callerUGI);
    } catch (BadRequestException bre) {
      throw bre;
    } catch (Exception e) {
      LOG.error("Error putting entities", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * 将一个域名写入时间线存储，返回写入过程中发生的错误
   * @param req HTTP请求
   * @param res HTTP响应
   * @param domain 待写入的域名信息
   * @return 写入响应
   */
  @PUT
  @Path("/domain")
  @Consumes({ MediaType.APPLICATION_JSON /* , MediaType.APPLICATION_XML */})
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public TimelinePutResponse putDomain(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      TimelineDomain domain) {
    init(res);
    UserGroupInformation callerUGI = getUser(req);
    if (callerUGI == null) {
      String msg = "The owner of the posted timeline domain is not set";
      LOG.error(msg);
      throw new ForbiddenException(msg);
    }
    domain.setOwner(callerUGI.getShortUserName());
    try {
      timelineDataManager.putDomain(domain, callerUGI);
    } catch (YarnException e) {
      // 用户无权限覆盖已存在的域名
      LOG.error(e.getMessage(), e);
      throw new ForbiddenException(e);
    } catch (RuntimeException e) {
      LOG.error("Error putting domain", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    } catch (IOException e) {
      LOG.error("Error putting domain", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return new TimelinePutResponse();
  }

  /**
   * 根据域名ID获取单个域名详情
   * @param req HTTP请求
   * @param res HTTP响应
   * @param domainId 域名ID
   * @return 域名详情
   */
  @GET
  @Path("/domain/{domainId}")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public TimelineDomain getDomain(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("domainId") String domainId) {
    init(res);
    domainId = parseStr(domainId);
    if (domainId == null || domainId.length() == 0) {
      throw new BadRequestException("Domain ID is not specified.");
    }
    TimelineDomain domain = null;
    try {
      domain = timelineDataManager.getDomain(
          parseStr(domainId), getUser(req));
    } catch (Exception e) {
      LOG.error("Error getting domain", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    if (domain == null) {
      throw new NotFoundException("Timeline domain ["
          + domainId + "] is not found");
    }
    return domain;
  }

  /**
   * 根据所有者获取域名列表
   * @param req HTTP请求
   * @param res HTTP响应
   * @param owner 所有者用户名
   * @return 该所有者拥有的域名集合
   */
  @GET
  @Path("/domain")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8
      /* , MediaType.APPLICATION_XML */})
  public TimelineDomains getDomains(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @QueryParam("owner") String owner) {
    init(res);
    owner = parseStr(owner);
    UserGroupInformation callerUGI = getUser(req);
    if (owner == null || owner.length() == 0) {
      if (callerUGI == null) {
        throw new BadRequestException("Domain owner is not specified.");
      } else {
        // 默认返回当前调用者自己的域名列表
        owner = callerUGI.getShortUserName();
      }
    }
    try {
      return timelineDataManager.getDomains(owner, callerUGI);
    } catch (Exception e) {
      LOG.error("Error getting domains", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * 初始化HTTP响应，清除默认ContentType
   * @param response HTTP响应对象
   */
  private void init(HttpServletResponse response) {
    response.setContentType(null);
  }

  /**
   * 从HTTP请求获取调用用户的UGI信息
   * @param req HTTP请求
   * @return 调用用户的UGI，无远程用户时返回null
   */
  private static UserGroupInformation getUser(HttpServletRequest req) {
    String remoteUser = req.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    return callerUGI;
  }

  /**
   * 将字符串按分隔符拆分解析为有序字符串集合
   * @param str 输入字符串
   * @param delimiter 分隔符
   * @return 拆分后的有序字符串集合
   */
  private static SortedSet<String> parseArrayStr(String str, String delimiter) {
    if (str == null) {
      return null;
    }
    SortedSet<String> strSet = new TreeSet<String>();
    String[] strs = str.split(delimiter);
    for (String aStr : strs) {
      strSet.add(aStr.trim());
    }
    return strSet;
  }

  /**
   * 将字符串按分隔符拆分解析为单个键值对，尝试将值反序列化为对象
   * @param str 输入字符串
   * @param delimiter 分隔符
   * @return 解析后的键值对
   */
  private static NameValuePair parsePairStr(String str, String delimiter) {
    if (str == null) {
      return null;
    }
    String[] strs = str.split(delimiter, 2);
    try {
      return new NameValuePair(strs[0].trim(),