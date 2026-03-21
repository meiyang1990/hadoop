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

package org.apache.hadoop.yarn.server.webapp;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;

import org.apache.commons.lang3.Range;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

/**
 * YARN服务器Web服务核心实现类，提供应用、应用尝试、容器等资源的RESTful查询接口
 * 实现AppInfoProvider接口，为Web UI提供应用信息查询能力
 */
public class WebServices implements AppInfoProvider {

  // 应用基础协议客户端，用于与YARN核心服务交互获取信息
  protected ApplicationBaseProtocol appBaseProt;

  /**
   * 构造函数，初始化Web服务
   * @param appBaseProt 应用基础协议客户端实例
   */
  public WebServices(ApplicationBaseProtocol appBaseProt) {
    this.appBaseProt = appBaseProt;
  }

  /**
   * 获取符合过滤条件的应用列表
   * @param req HTTP请求
   * @param res HTTP响应
   * @param stateQuery 已过时的应用状态查询参数
   * @param statesQuery 应用状态集合查询参数
   * @param finalStatusQuery 最终状态查询参数
   * @param userQuery 提交用户查询参数
   * @param queueQuery 队列查询参数
   * @param count 返回结果数量限制
   * @param startedBegin 应用启动时间起始范围
   * @param startedEnd 应用启动时间结束范围
   * @param finishBegin 应用完成时间起始范围
   * @param finishEnd 应用完成时间结束范围
   * @param nameQuery 应用名称查询参数
   * @param applicationTypes 应用类型集合查询参数
   * @return 符合条件的应用列表信息
   */
  public AppsInfo getApps(HttpServletRequest req, HttpServletResponse res,
      String stateQuery, Set<String> statesQuery, String finalStatusQuery,
      String userQuery, String queueQuery, String count, String startedBegin,
      String startedEnd, String finishBegin, String finishEnd, String nameQuery,
      Set<String> applicationTypes) {
    UserGroupInformation callerUGI = getUser(req);
    boolean checkEnd = false;
    boolean checkAppTypes = false;
    boolean checkAppStates = false;
    long countNum = Long.MAX_VALUE;

    // 未指定范围时设置默认值
    long sBegin = 0;
    long sEnd = Long.MAX_VALUE;
    long fBegin = 0;
    long fEnd = Long.MAX_VALUE;

    // 解析结果数量限制并校验
    if (count != null && !count.isEmpty()) {
      countNum = Long.parseLong(count);
      if (countNum <= 0) {
        throw new BadRequestException("limit value must be greater then 0");
      }
    }

    // 解析启动时间起始值并校验
    if (startedBegin != null && !startedBegin.isEmpty()) {
      sBegin = Long.parseLong(startedBegin);
      if (sBegin < 0) {
        throw new BadRequestException("startedTimeBegin must be greater than 0");
      }
    }
    // 解析启动时间结束值并校验
    if (startedEnd != null && !startedEnd.isEmpty()) {
      sEnd = Long.parseLong(startedEnd);
      if (sEnd < 0) {
        throw new BadRequestException("startedTimeEnd must be greater than 0");
      }
    }
    // 校验启动时间范围合法性
    if (sBegin > sEnd) {
      throw new BadRequestException(
        "startedTimeEnd must be greater than startTimeBegin");
    }

    // 解析完成时间起始值并校验
    if (finishBegin != null && !finishBegin.isEmpty()) {
      checkEnd = true;
      fBegin = Long.parseLong(finishBegin);
      if (fBegin < 0) {
        throw new BadRequestException("finishTimeBegin must be greater than 0");
      }
    }
    // 解析完成时间结束值并校验
    if (finishEnd != null && !finishEnd.isEmpty()) {
      checkEnd = true;
      fEnd = Long.parseLong(finishEnd);
      if (fEnd < 0) {
        throw new BadRequestException("finishTimeEnd must be greater than 0");
      }
    }
    // 校验完成时间范围合法性
    if (fBegin > fEnd) {
      throw new BadRequestException(
        "finishTimeEnd must be greater than finishTimeBegin");
    }

    // 解析应用类型查询参数
    Set<String> appTypes = parseQueries(applicationTypes, false);
    if (!appTypes.isEmpty()) {
      checkAppTypes = true;
    }

    // 处理已过时的stateQuery参数，兼容旧用法
    if (stateQuery != null && !stateQuery.isEmpty()) {
      statesQuery.add(stateQuery);
    }
    // 解析应用状态查询参数
    Set<String> appStates = parseQueries(statesQuery, true);
    if (!appStates.isEmpty()) {
      checkAppStates = true;
    }

    AppsInfo allApps = new AppsInfo();
    Collection<ApplicationReport> appReports = null;
    // 构造获取应用列表请求
    final GetApplicationsRequest request =
        GetApplicationsRequest.newInstance();
    request.setLimit(countNum);
    request.setStartRange(Range.between(sBegin, sEnd));
    try {
      // 根据UGI情况选择是否以特权用户身份执行请求
      if (callerUGI == null) {
        // TODO: the request should take the params like what RMWebServices does
        // in YARN-1819.
        appReports = getApplicationsReport(request);
      } else {
        appReports = callerUGI.doAs(
            new PrivilegedExceptionAction<Collection<ApplicationReport>> () {
          @Override
          public Collection<ApplicationReport> run() throws Exception {
            return getApplicationsReport(request);
          }
        });
      }
    } catch (Exception e) {
      rewrapAndThrowException(e);
    }
    if (appReports == null) {
      return allApps;
    }
    // 遍历所有应用，根据过滤条件筛选
    for (ApplicationReport appReport : appReports) {

      // 按应用状态过滤
      if (checkAppStates &&
          !appStates.contains(StringUtils.toLowerCase(
              appReport.getYarnApplicationState().toString()))) {
        continue;
      }
      // 按最终状态过滤
      if (finalStatusQuery != null && !finalStatusQuery.isEmpty()) {
        FinalApplicationStatus.valueOf(finalStatusQuery);
        if (!appReport.getFinalApplicationStatus().toString()
          .equalsIgnoreCase(finalStatusQuery)) {
          continue;
        }
      }
      // 按提交用户过滤
      if (userQuery != null && !userQuery.isEmpty()) {
        if (!appReport.getUser().equals(userQuery)) {
          continue;
        }
      }
      // 按队列过滤
      if (queueQuery != null && !queueQuery.isEmpty()) {
        if (appReport.getQueue() == null || !appReport.getQueue()
            .equals(queueQuery)) {
          continue;
        }
      }
      // 按应用类型过滤
      if (checkAppTypes &&
          !appTypes.contains(
              StringUtils.toLowerCase(appReport.getApplicationType().trim()))) {
        continue;
      }

      // 按完成时间过滤
      if (checkEnd
          && (appReport.getFinishTime() < fBegin || appReport.getFinishTime() > fEnd)) {
        continue;
      }

      // 按应用名称过滤
      if (nameQuery != null && !nameQuery.equals(appReport.getName())) {
        continue;
      }

      // 转换为DAO对象添加到结果集
      AppInfo app = new AppInfo(appReport);

      allApps.add(app);
    }
    return allApps;
  }

  /**
   * 获取指定应用的信息
   * @param req HTTP请求
   * @param res HTTP响应
   * @param appId 应用ID字符串
   * @return 应用信息
   */
  public AppInfo getApp(HttpServletRequest req,
      HttpServletResponse res, String appId) {
    return getApp(req, appId);
  }

  @Override
  public BasicAppInfo getApp(HttpServletRequest req, String appId,
      String clusterId) {
    return BasicAppInfo.fromAppInfo(getApp(req, appId));
  }

  /**
   * 获取指定应用的信息
   * @param req HTTP请求
   * @param appId 应用ID字符串
   * @return 应用信息
   */
  public AppInfo getApp(HttpServletRequest req, String appId) {
    UserGroupInformation callerUGI = getUser(req);
    final ApplicationId id = parseApplicationId(appId);
    ApplicationReport app = null;
    try {
      // 根据UGI情况选择是否以特权用户身份执行请求
      if (callerUGI == null) {
        GetApplicationReportRequest request =
            GetApplicationReportRequest.newInstance(id);
        app = getApplicationReport(request);
      } else {
        app = callerUGI.doAs(
            new PrivilegedExceptionAction<ApplicationReport> () {
          @Override
          public ApplicationReport run() throws Exception {
            GetApplicationReportRequest request =
                GetApplicationReportRequest.newInstance(id);
            return getApplicationReport(request);
          }
        });
      }
    } catch (Exception e) {
      rewrapAndThrowException(e);
    }
    if (app == null) {
      throw new NotFoundException("app with id: " + appId + " not found");
    }
    return new AppInfo(app);
  }

  /**
   * 获取指定应用的所有尝试列表信息
   * @param req HTTP请求
   * @param res HTTP响应
   * @param appId 应用ID字符串
   * @return 应用尝试列表信息
   */
  public AppAttemptsInfo getAppAttempts(HttpServletRequest req,
      HttpServletResponse res, String appId) {
    UserGroupInformation callerUGI = getUser(req);
    final ApplicationId id = parseApplicationId(appId);
    Collection<ApplicationAttemptReport> appAttemptReports = null;
    try {
      // 根据UGI情况选择是否以特权用户身份执行请求
      if (callerUGI == null) {
        GetApplicationAttemptsRequest request =
            GetApplicationAttemptsRequest.newInstance(id);
        appAttemptReports =
            getApplicationAttemptsReport(request);
      } else {
        appAttemptReports = callerUGI.doAs(
            new PrivilegedExceptionAction<Collection<ApplicationAttemptReport>> () {
          @Override
          public Collection<ApplicationAttemptReport> run() throws Exception {
            GetApplicationAttemptsRequest request =
                GetApplicationAttemptsRequest.newInstance(id);
            return getApplicationAttemptsReport(request);
          }
        });
      }
    } catch (Exception e) {
      rewrapAndThrowException(e);
    }
    AppAttemptsInfo appAttemptsInfo = new AppAttemptsInfo();
    if (appAttemptReports == null) {
      return appAttemptsInfo;
    }
    // 转换为DAO对象添加到结果集
    for (ApplicationAttemptReport appAttemptReport : appAttemptReports) {
      AppAttemptInfo appAttemptInfo = new AppAttemptInfo(appAttemptReport);
      appAttemptsInfo.add(appAttemptInfo);
    }

    return appAttemptsInfo;
  }

  /**
   * 获取指定应用尝试的详细信息
   * @param req HTTP请求
   * @param res HTTP响应
   * @param appId 应用ID字符串
   * @param appAttemptId 应用尝试ID字符串
   * @return 应用尝试信息
   */
  public AppAttemptInfo getAppAttempt(HttpServletRequest req,
      HttpServletResponse res, String appId, String appAttemptId) {
    UserGroupInformation callerUGI = getUser(req);
    ApplicationId aid = parseApplicationId(appId);
    final ApplicationAttemptId aaid = parseApplicationAttemptId(appAttemptId);
    validateIds(aid, aaid, null);
    ApplicationAttemptReport appAttempt = null;
    try {
      // 根据UGI情况选择是否以特权用户身份执行请求
      if (callerUGI == null) {
        GetApplicationAttemptReportRequest request =
            GetApplicationAttemptReportRequest.newInstance(aaid);
        appAttempt =
            getApplicationAttemptReport(request);
      } else {
        appAttempt = callerUGI.doAs(
            new PrivilegedExceptionAction<ApplicationAttemptReport> () {
          @Override
          public ApplicationAttemptReport run() throws Exception {
            GetApplicationAttemptReportRequest request =
                GetApplicationAttemptReportRequest.newInstance(aaid);
            return getApplicationAttemptReport(request);
          }
        });
      }
    } catch (Exception e) {
      rewrapAndThrowException(e);
    }
    if (appAttempt == null) {
      throw new NotFoundException("app attempt with id: " + appAttemptId
          + " not found");
    }
    return new AppAttemptInfo(appAttempt);
  }

  /**
   * 获取指定应用尝试下的所有容器列表信息
   * @param req HTTP请求
   * @param res HTTP响应
   * @param appId 应用ID字符串
   * @param appAttemptId 应用尝试ID字符串
   * @return 容器列表信息
   */
  public ContainersInfo getContainers(HttpServletRequest req,
      HttpServletResponse res, String appId, String appAttemptId) {
    UserGroupInformation callerUGI = getUser(req);
    ApplicationId aid = parseApplicationId(appId);
    final ApplicationAttemptId aaid = parseApplicationAttemptId(appAttemptId);
    validateIds(aid, aaid, null);
    Collection<ContainerReport> containerReports = null;
    try {
      // 根据UGI情况选择是否以特权用户身份执行请求
      if (callerUGI == null) {
        GetContainersRequest request = GetContainersRequest.newInstance(aaid);
        containerReports =
            getContainersReport(request);
      } else {
        containerReports = callerUGI.doAs(
            new PrivilegedExceptionAction<Collection<ContainerReport>> () {
          @Override
          public Collection<ContainerReport> run() throws Exception {
            GetContainersRequest request = GetContainersRequest.newInstance(aaid);
            return getContainersReport(request);
          }
        });
      }
    } catch (Exception e) {
      rewrapAndThrowException(e);
    }
    ContainersInfo containersInfo = new ContainersInfo();
    if (containerReports == null) {
      return containersInfo;
    }
    // 转换为DAO对象添加到结果集
    for (ContainerReport containerReport : containerReports) {
      ContainerInfo containerInfo = new ContainerInfo(containerReport);
      containersInfo.add(containerInfo);
    }
    return containersInfo;