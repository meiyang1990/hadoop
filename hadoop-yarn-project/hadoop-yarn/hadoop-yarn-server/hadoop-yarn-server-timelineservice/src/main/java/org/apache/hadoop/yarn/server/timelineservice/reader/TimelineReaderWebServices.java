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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;

import javax.inject.Singleton;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.api.records.timeline.TimelineHealth;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.metrics.TimelineReaderMetrics;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST 端点，提供时间线读取服务。
 * 位于YARN时间线服务的读取层，对外提供REST API查询时间线数据。
 */
@Private
@Unstable
@Singleton
@Path("/ws/v2/timeline")
public class TimelineReaderWebServices {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineReaderWebServices.class);

  @Context
  private ServletContext ctxt;

  private static final String QUERY_STRING_SEP = "?";
  private static final String RANGE_DELIMITER = "-";
  private static final String DATE_PATTERN = "yyyyMMdd";
  private static final TimelineReaderMetrics METRICS =
      TimelineReaderMetrics.getInstance();

  @VisibleForTesting
  /** 线程本地日期格式化工具，使用GMT时区，yyyyMMdd格式 */
  static final ThreadLocal<DateFormat> DATE_FORMAT =
      new ThreadLocal<DateFormat>() {
      @Override
      protected DateFormat initialValue() {
        SimpleDateFormat format =
            new SimpleDateFormat(DATE_PATTERN, Locale.ENGLISH);
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        format.setLenient(false);
        return format;
      }
    };

  /** 初始化响应，清空Content-Type */
  private void init(HttpServletResponse response) {
    response.setContentType(null);
  }

  /** 日期范围数据结构，存储开始和结束时间戳 */
  private static final class DateRange {
    private Long dateStart;
    private Long dateEnd;
    private DateRange(Long start, Long end) {
      this.dateStart = start;
      this.dateEnd = end;
    }
  }

  /** 解析日期字符串为时间戳 */
  private static long parseDate(String strDate) throws ParseException {
    Date date = DATE_FORMAT.get().parse(strDate);
    return date.getTime();
  }

  /**
   * 解析日期范围字符串，支持单个日期或[startdate]-[enddate]格式，起止日期可缺省。
   * @param dateRange 输入日期范围字符串
   * @return 解析后的DateRange对象
   * @throws IllegalArgumentException 格式错误时抛出
   */
  private static DateRange parseDateRange(String dateRange)
      throws IllegalArgumentException {
    if (dateRange == null || dateRange.isEmpty()) {
      return new DateRange(null, null);
    }
    // 按"-"分割日期范围，获取开始和结束日期两个部分
    String[] dates = dateRange.split(RANGE_DELIMITER, 2);
    Long start = null;
    Long end = null;
    try {
      String startDate = dates[0].trim();
      if (!startDate.isEmpty()) {
        // 开始日期长度不符合yyyyMMdd格式
        if (startDate.length() != DATE_PATTERN.length()) {
          throw new IllegalArgumentException("Invalid date range " + dateRange);
        }
        // 解析"-"前的开始日期，如果没有"-"，则表示单个日期
        start = parseDate(startDate);
      }
      if (dates.length > 1) {
        String endDate = dates[1].trim();
        if (!endDate.isEmpty()) {
          // 结束日期长度不符合yyyyMMdd格式
          if (endDate.length() != DATE_PATTERN.length()) {
            throw new IllegalArgumentException(
                "Invalid date range " + dateRange);
          }
          // 解析"-"后的结束日期
          end = parseDate(endDate);
        }
      } else {
        // 单个日期（不含"-"），结束日期等于开始日期
        end = start;
      }
      if (start != null && end != null) {
        if (start > end) {
          throw new IllegalArgumentException("Invalid date range " + dateRange);
        }
      }
      return new DateRange(start, end);
    } catch (ParseException e) {
      // 日期解析失败
      throw new IllegalArgumentException("Invalid date range " + dateRange);
    }
  }

  /** 从Servlet上下文获取时间线读取管理器实例 */
  private TimelineReaderManager getTimelineReaderManager() {
    return (TimelineReaderManager)
        ctxt.getAttribute(TimelineReaderServer.TIMELINE_READER_MANAGER_ATTR);
  }

  /** 统一处理各类请求异常，分类转换为对应HTTP状态码的异常 */
  private static void handleException(Exception e, String url, long startTime,
      String invalidNumMsg) throws BadRequestException,
      WebApplicationException {
    long endTime = Time.monotonicNow();
    LOG.info("Processed URL {} but encountered exception (Took " +
        "{} ms.)", url, (endTime - startTime));
    if (e instanceof NumberFormatException) {
      throw new BadRequestException(invalidNumMsg + " is not a numeric value.");
    } else if (e instanceof IllegalArgumentException) {
      throw new BadRequestException(e.getMessage() == null ?
          "Requested Invalid Field." : e.getMessage());
    } else if (e instanceof NotFoundException) {
      throw (NotFoundException)e;
    } else if (e instanceof TimelineParseException) {
      throw new BadRequestException(e.getMessage() == null ?
          "Filter Parsing failed." : e.getMessage());
    } else if (e instanceof BadRequestException) {
      throw (BadRequestException)e;
    } else if (e instanceof ForbiddenException) {
      throw (ForbiddenException) e;
    } else {
      LOG.error("Error while processing REST request", e);
      throw new WebApplicationException(e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * 获取时间线读取服务的基本信息。
   * @param req Servlet请求
   * @param res Servlet响应
   * @return 时间线服务关于信息
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public TimelineAbout about(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res) {
    init(res);
    return TimelineUtils.createTimelineAbout("Timeline Reader API");
  }

  /**
   * 健康检查REST端点。
   * @param req Servlet请求
   * @param res Servlet响应
   * @return 健康状态响应，运行中返回200，否则返回500
   */
  @GET
  @Path("/health")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Response health(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res
  ) {
    Response response;
    // 获取时间线读取服务健康状态
    TimelineHealth timelineHealth = this.getTimelineReaderManager().getHealthStatus();
    if (timelineHealth.getHealthStatus()
        .equals(TimelineHealth.TimelineHealthStatus.RUNNING)) {
      // 服务正常运行，返回200OK
      response = Response.ok(timelineHealth).build();
    } else {
       // 服务异常，记录日志并返回500错误
       LOG.info("Timeline services health check: timeline reader reported " +
           "connection failure");
       response = Response.serverError().entity(timelineHealth).build();
    }

    return response;
  }

  /**
   * 根据应用UID查询指定类型的时间线实体集合。
   * @param req Servlet请求
   * @param res Servlet响应
   * @param uId 应用UID，包含集群ID、用户ID、流名称、流运行ID、应用ID
   * @param entityType 实体类型
   * @param limit 返回结果数量限制
   * @param createdTimeStart 创建时间起始过滤
   * @param createdTimeEnd 创建时间结束过滤
   * @param relatesTo 关联实体过滤
   * @param isRelatedTo 被关联实体过滤
   * @param infofilters 信息字段过滤
   * @param conffilters 配置字段过滤
   * @param metricfilters 指标过滤
   * @param eventfilters 事件过滤
   * @param confsToRetrieve 需要返回的配置项
   * @param metricsToRetrieve 需要返回的指标
   * @param fields 需要返回的实体字段
   * @param metricsLimit 返回指标数量限制
   * @param metricsTimeStart 指标时间起始过滤
   * @param metricsTimeEnd 指标时间结束过滤
   * @param fromId 分页起始ID
   * @return 匹配的时间线实体集合
   */
  @GET
  @Path("/app-uid/{uid}/entities/{entitytype}")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Set<TimelineEntity> getEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("uid") String uId,
      @PathParam("entitytype") String entityType,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("relatesto") String relatesTo,
      @QueryParam("isrelatedto") String isRelatedTo,
      @QueryParam("infofilters") String infofilters,
      @QueryParam("conffilters") String conffilters,
      @QueryParam("metricfilters") String metricfilters,
      @QueryParam("eventfilters") String eventfilters,
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("fromid") String fromId) {
    // 构造完整请求URL用于日志记录
    String url = req.getRequestURI() +
        (req.getQueryString() == null ? "" :
            QUERY_STRING_SEP + req.getQueryString());
    // 获取请求用户信息
    UserGroupInformation callerUGI =
        TimelineReaderWebServicesUtils.getUser(req);
    LOG.info("Received URL {} from user {}",
        url, TimelineReaderWebServicesUtils.getUserName(callerUGI));
    long startTime = Time.monotonicNow();
    boolean succeeded = false;
    init(res);
    TimelineReaderManager timelineReaderManager = getTimelineReaderManager();
    Set<TimelineEntity> entities = null;
    try {
      // 解析UID得到查询上下文
      TimelineReaderContext context =
          TimelineUIDConverter.APPLICATION_UID.decodeUID(uId);
      if (context == null) {
        throw new BadRequestException("Incorrect UID " +  uId);
      }
      context.setEntityType(
          TimelineReaderWebServicesUtils.parseStr(entityType));
      context.setGenericEntity(true);
      // 调用读取管理器查询实体
      entities = timelineReaderManager.getEntities(context,
          TimelineReaderWebServicesUtils.createTimelineEntityFilters(
          limit, createdTimeStart, createdTimeEnd, relatesTo, isRelatedTo,
              infofilters, conffilters, metricfilters, eventfilters,
              fromId),
          TimelineReaderWebServicesUtils.createTimelineDataToRetrieve(
          confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
          metricsTimeStart, metricsTimeEnd));
      // 权限检查
      checkAccessForGenericEntities(entities, callerUGI, entityType);
      succeeded = true;
    } catch (Exception e) {
      handleException(e, url, startTime,
          "Either limit or createdtime start/end or metricslimit or metricstime"
              + " start/end or fromid");
    } finally {
      // 记录请求延迟指标
      long latency = Time.monotonicNow() - startTime;
      METRICS.addGetEntitiesLatency(latency, succeeded);
      LOG.info("Processed URL {}" +
          " (Took {} ms.)", url, latency);
    }
    if (entities == null) {
      entities = Collections.emptySet();
    }
    return entities;
  }

  /**
   * 查询指定应用下指定类型的时间线实体集合（使用默认集群ID）。
   */
  @GET
  @Path("/apps/{appid}/entities/{entitytype}")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  @Consumes(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Set<TimelineEntity> getEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,
      @PathParam("appid") String appId,
      @PathParam("entitytype") String entityType,
      @QueryParam("userid") String userId,
      @QueryParam("flowname") String flowName,
      @QueryParam("flowrunid") String flowRunId,
      @QueryParam("limit") String limit,
      @QueryParam("createdtimestart") String createdTimeStart,
      @QueryParam("createdtimeend") String createdTimeEnd,
      @QueryParam("relatesto") String relatesTo,
      @QueryParam("isrelatedto") String isRelatedTo,
      @QueryParam("infofilters") String infofilters,
      @QueryParam("conffilters") String conffilters,
      @QueryParam("metricfilters") String metricfilters,
      @QueryParam("eventfilters") String eventfilters,
      @QueryParam("confstoretrieve") String confsToRetrieve,
      @QueryParam("metricstoretrieve") String metricsToRetrieve,
      @QueryParam("fields") String fields,
      @QueryParam("metricslimit") String metricsLimit,
      @QueryParam("metricstimestart") String metricsTimeStart,
      @QueryParam("metricstimeend") String metricsTimeEnd,
      @QueryParam("fromid") String fromId) {
    return getEntities(req, res, null, appId, entityType, userId, flowName,
        flowRunId, limit, createdTimeStart, createdTimeEnd, relatesTo,
        isRelatedTo, infofilters, conffilters, metricfilters, eventfilters,
        confsToRetrieve, metricsToRetrieve, fields, metricsLimit,
        metricsTimeStart, metricsTimeEnd, fromId, true);
  }

  /**
   * 查询指定集群应用下指定类型的时间线实体集合。
   */
  @GET
  @Path("/clusters/{clusterid}/apps/{appid}/entities/{entitytype}")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  @Consumes(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Set<TimelineEntity> getEntities(
      @Context HttpServletRequest req,
      @Context HttpServletResponse res,