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

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_STATE;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_START_TIME_BEGIN;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_START_TIME_END;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APPS_NUM;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR_VALUE;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.Range;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN Web UI 应用列表展示块，负责根据查询条件获取应用信息，并渲染应用列表表格。
 */
public class AppsBlock extends HtmlBlock {

  private static final Logger LOG = LoggerFactory.getLogger(AppsBlock.class);
  protected ApplicationBaseProtocol appBaseProt;
  protected EnumSet<YarnApplicationState> reqAppStates;
  protected UserGroupInformation callerUGI;
  protected Collection<ApplicationReport> appReports;

  @Inject
  protected AppsBlock(ApplicationBaseProtocol appBaseProt, ViewContext ctx) {
    super(ctx);
    this.appBaseProt = appBaseProt;
  }

  /**
   * 根据请求参数获取应用列表数据，处理参数校验和权限查询。
   * @throws YarnException YARN服务异常
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  protected void fetchData() throws YarnException, IOException,
      InterruptedException {
    reqAppStates = EnumSet.noneOf(YarnApplicationState.class);
    String reqStateString = $(APP_STATE);
    if (reqStateString != null && !reqStateString.isEmpty()) {
      String[] appStateStrings = reqStateString.split(",");
      // 解析多个逗号分隔的应用状态
      for (String stateString : appStateStrings) {
        reqAppStates.add(YarnApplicationState.valueOf(stateString.trim()));
      }
    }
    callerUGI = getCallerUGI();
    // 创建获取应用列表请求对象
    final GetApplicationsRequest request =
        GetApplicationsRequest.newInstance(reqAppStates);
    String appsNumStr = $(APPS_NUM);
    if (appsNumStr != null && !appsNumStr.isEmpty()) {
      long appsNum = Long.parseLong(appsNumStr);
      request.setLimit(appsNum);
    }

    // 解析应用启动起始时间参数
    String appStartedTimeBegainStr = $(APP_START_TIME_BEGIN);
    long appStartedTimeBegain = 0;
    if (appStartedTimeBegainStr != null && !appStartedTimeBegainStr.isEmpty()) {
      appStartedTimeBegain = Long.parseLong(appStartedTimeBegainStr);
      if (appStartedTimeBegain < 0) {
        throw new BadRequestException(
          "app.started-time.begin must be greater than 0");
      }
    }
    // 解析应用启动结束时间参数
    String appStartedTimeEndStr = $(APP_START_TIME_END);
    long appStartedTimeEnd = Long.MAX_VALUE;
    if (appStartedTimeEndStr != null && !appStartedTimeEndStr.isEmpty()) {
      appStartedTimeEnd = Long.parseLong(appStartedTimeEndStr);
      if (appStartedTimeEnd < 0) {
        throw new BadRequestException(
          "app.started-time.end must be greater than 0");
      }
    }
    // 校验时间范围合法性
    if (appStartedTimeBegain > appStartedTimeEnd) {
      throw new BadRequestException(
        "app.started-time.end must be greater than app.started-time.begin");
    }
    // 设置应用启动时间范围查询条件
    request.setStartRange(
        Range.between(appStartedTimeBegain, appStartedTimeEnd));

    // 以请求用户身份查询应用列表
    if (callerUGI == null) {
      appReports = getApplicationReport(request);
    } else {
      appReports =
          callerUGI
            .doAs(new PrivilegedExceptionAction<Collection<ApplicationReport>>() {
              @Override
              public Collection<ApplicationReport> run() throws Exception {
                return getApplicationReport(request);
              }
            });
    }
  }

  /**
   * 调用ApplicationBaseProtocol获取应用列表。
   * @param request 获取应用请求
   * @return 应用报告列表
   * @throws YarnException YARN服务异常
   * @throws IOException IO异常
   */
  protected List<ApplicationReport> getApplicationReport(
      final GetApplicationsRequest request) throws YarnException, IOException {
    return appBaseProt.getApplications(request).getApplicationList();
  }

  @Override
  public void render(Block html) {
    setTitle("Applications");

    try {
      // 获取查询数据
      fetchData();
    } catch (YarnException | IOException | InterruptedException e) {
      String message = "Failed to read the applications.";
      LOG.error(message, e);
      html.p().__(message).__();
      return;
    }
    // 渲染数据到页面
    renderData(html);
  }

  /**
   * 将应用列表数据渲染为前端表格数据。
   * @param html HTML块上下文
   */
  protected void renderData(Block html) {
    // 创建应用列表表格表头
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#apps").thead().tr().th(".id", "ID").th(".user", "User")
          .th(".name", "Name").th(".type", "Application Type")
          .th(".apptag", "Application Tags").th(".queue", "Queue")
          .th(".priority", "Application Priority")
          .th(".starttime", "StartTime")
          .th(".launchtime", "LaunchTime")
          .th(".finishtime", "FinishTime")
          .th(".state", "State").th(".finalstatus", "FinalStatus")
          .th(".progress", "Progress").th(".ui", "Tracking UI").__().__().tbody();

    // 构建前端表格需要的JSON数据
    StringBuilder appsTableData = new StringBuilder("[\n");
    for (ApplicationReport appReport : appReports) {
      // TODO: remove the following condition. It is still here because
      // the history side implementation of ApplicationBaseProtocol
      // hasn't filtering capability (YARN-1819).
      // 服务端未实现过滤能力，本地二次过滤不符合状态条件的应用
      if (!reqAppStates.isEmpty()
          && !reqAppStates.contains(appReport.getYarnApplicationState())) {
        continue;
      }
      AppInfo app = new AppInfo(appReport);
      String percent = StringUtils.format("%.1f", app.getProgress());
      // 拼接每个应用的JSON行，进行HTML转义防止XSS
      appsTableData
        .append("[\"<a href='")
        .append(url("app", app.getAppId()))
        .append("'>")
        .append(app.getAppId())
        .append("</a>\",\"")
        .append(
          StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(app
              .getUser())))
        .append("\",\"")
        .append(
          StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(app
            .getName())))
        .append("\",\"")
        .append(
          StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(app
            .getType())))
          .append("\",\"")
          .append(
              StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(
                  app.getApplicationTags() == null ? "" : app.getApplicationTags())))
        .append("\",\"")
        .append(
          StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(app
            .getQueue()))).append("\",\"").append(String
                .valueOf(app.getPriority()))
        .append("\",\"").append(app.getStartedTime())
        .append("\",\"").append(app.getLaunchTime())
        .append("\",\"").append(app.getFinishedTime())
        .append("\",\"")
        .append(app.getAppState() == null ? UNAVAILABLE : app.getAppState())
        .append("\",\"")
        .append(app.getFinalAppStatus())
        .append("\",\"")
        // 生成进度条HTML
        .append("<br title='").append(percent).append("'> <div class='")
        .append(C_PROGRESSBAR).append("' title='").append(join(percent, '%'))
        .append("'> ").append("<div class='").append(C_PROGRESSBAR_VALUE)
        .append("' style='").append(join("width:", percent, '%'))
        .append("'> </div> </div>").append("\",\"<a ");

      String trackingURL =
          app.getTrackingUrl() == null
              || app.getTrackingUrl().equals(UNAVAILABLE) ? null : app
            .getTrackingUrl();

      // 根据应用状态确定追踪UI名称，已结束应用显示History，运行中显示ApplicationMaster
      String trackingUI =
          app.getTrackingUrl() == null || app.getTrackingUrl().equals(UNAVAILABLE)
              ? "Unassigned" :
              Apps.isApplicationFinalState(app.getAppState())
                  ? "History" : "ApplicationMaster";
      appsTableData.append(trackingURL == null ? "#" : "href='" + trackingURL)
        .append("'>").append(trackingUI).append("</a>\"],\n");

    }
    // 移除最后一行多余的逗号
    if (appsTableData.charAt(appsTableData.length() - 2) == ',') {
      appsTableData.delete(appsTableData.length() - 2,
        appsTableData.length() - 1);
    }
    appsTableData.append("]");
    // 将表格数据注入为前端JavaScript变量
    html.script().$type("text/javascript")
      .__("var appsTableData=" + appsTableData).__();

    tbody.__().__();
  }
}