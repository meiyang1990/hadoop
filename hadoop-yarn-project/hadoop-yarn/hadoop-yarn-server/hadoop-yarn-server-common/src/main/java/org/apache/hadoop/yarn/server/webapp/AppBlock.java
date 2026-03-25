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
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APPLICATION_ID;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.WEB_UI_TYPE;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN Web UI 应用详情页面区块，负责渲染应用基本信息、尝试列表和相关操作按钮
 */
public class AppBlock extends HtmlBlock {

  private static final Logger LOG = LoggerFactory.getLogger(AppBlock.class);
  protected ApplicationBaseProtocol appBaseProt;
  protected Configuration conf;
  protected ApplicationId appID = null;
  private boolean unsecuredUI = true;


  /**
   * 构造函数，通过Guice注入依赖，初始化并检查当前UI是否为非安全认证模式
   */
  @Inject
  protected AppBlock(ApplicationBaseProtocol appBaseProt, ViewContext ctx,
      Configuration conf) {
    super(ctx);
    this.appBaseProt = appBaseProt;
    this.conf = conf;
    // 检查当前UI是否使用非安全认证
    String httpAuth = conf.get(CommonConfigurationKeys.HADOOP_HTTP_AUTHENTICATION_TYPE);
    this.unsecuredUI = (httpAuth != null) && (httpAuth.equals("simple") ||
         httpAuth.equals(PseudoAuthenticationHandler.class.getName()));
  }

  /**
   * 渲染应用详情页面区块的主入口方法
   */
  @Override
  protected void render(Block html) {
    String webUiType = $(WEB_UI_TYPE);
    String aid = $(APPLICATION_ID);
    if (aid.isEmpty()) {
      puts("Bad request: requires Application ID");
      return;
    }

    try {
      // 将字符串ID转换为ApplicationId对象
      appID = Apps.toAppID(aid);
    } catch (Exception e) {
      puts("Invalid Application ID: " + aid);
      return;
    }

    // 获取当前请求用户信息
    UserGroupInformation callerUGI = getCallerUGI();
    ApplicationReport appReport;
    try {
      final GetApplicationReportRequest request =
          GetApplicationReportRequest.newInstance(appID);
      // 根据是否有用户信息选择调用方式，支持特权访问
      if (callerUGI == null) {
        appReport = getApplicationReport(request);
      } else {
        appReport = callerUGI.doAs(
            new PrivilegedExceptionAction<ApplicationReport> () {
          @Override
          public ApplicationReport run() throws Exception {
            return getApplicationReport(request);
          }
        });
      }
    } catch (Exception e) {
      String message = "Failed to read the application " + appID + ".";
      LOG.error(message, e);
      html.p().__(message).__();
      return;
    }

    if (appReport == null) {
      puts("Application not found: " + aid);
      return;
    }

    // 封装应用信息为Web层数据对象
    AppInfo app = new AppInfo(appReport);

    // 设置页面标题
    setTitle(join("Application ", aid));

    // 验证是否有权限读取应用尝试信息，同时会基于ACL验证当前用户是否允许杀死应用
    Collection<ApplicationAttemptReport> attempts;
    try {
      final GetApplicationAttemptsRequest request =
          GetApplicationAttemptsRequest.newInstance(appID);
      if (callerUGI == null) {
        attempts = getApplicationAttemptsReport(request);
      } else {
        attempts = callerUGI.doAs(
          new PrivilegedExceptionAction<Collection<
              ApplicationAttemptReport>>() {
            @Override
            public Collection<ApplicationAttemptReport> run()
                throws Exception {
              return getApplicationAttemptsReport(request);
            }
          });
      }
    } catch (Exception e) {
      String message =
          "Failed to read the attempts of the application " + appID + ".";
      LOG.error(message, e);
      html.p().__(message).__();
      return;
    }

    // YARN-6890: 安全集群开启匿名UI访问时，不显示杀死应用按钮
    boolean unsecuredUIForSecuredCluster = UserGroupInformation.isSecurityEnabled()
        && this.unsecuredUI;

    // 判断是否显示杀死应用按钮：必须是RM WebUI、开启了UI操作、非安全集群匿名访问、应用未结束
    if (webUiType != null
        && webUiType.equals(YarnWebParams.RM_WEB_UI)
        && conf.getBoolean(YarnConfiguration.RM_WEBAPP_UI_ACTIONS_ENABLED,
          YarnConfiguration.DEFAULT_RM_WEBAPP_UI_ACTIONS_ENABLED)
            && !unsecuredUIForSecuredCluster
            && !Apps.isApplicationFinalState(app.getAppState())) {
      // 渲染杀死应用按钮
      html.div()
        .button()
          .$onclick("confirmAction()").b("Kill Application").__()
          .__();

      // 生成杀死应用的JavaScript处理逻辑
      StringBuilder script = new StringBuilder();
      script.append("function confirmAction() {")
          .append(" b = confirm(\"Are you sure?\");")
          .append(" if (b == true) {")
          .append(" $.ajax({")
          .append(" type: 'PUT',")
          .append(" url: '/ws/v1/cluster/apps/").append(aid).append("/state',")
          .append(" contentType: 'application/json',")
          .append(getCSRFHeaderString(conf))
          .append(" data: '{\"state\":\"KILLED\"}',")
          .append(" dataType: 'json'")
          .append(" }).done(function(data){")
          .append(" setTimeout(function(){")
          .append(" location.href = '/cluster/app/").append(aid).append("';")
          .append(" }, 1000);")
          .append(" }).fail(function(data){")
          .append(" console.log(data);")
          .append(" });")
          .append(" }")
          .append("}");

      html.script().$type("text/javascript").__(script.toString()).__();
    }

    // 生成调度器页面链接，跳转到对应队列
    String schedulerPath = WebAppUtils.getResolvedRMWebAppURLWithScheme(conf) +
        "/cluster/scheduler?openQueues=" + app.getQueue();

    // 渲染应用概览信息表格
    generateOverviewTable(app, schedulerPath, webUiType, appReport);

    // 渲染应用指标表格
    createApplicationMetricsTable(html);

    // 渲染通用信息区块
    html.__(InfoBlock.class);

    // 渲染应用尝试列表表格
    generateApplicationTable(html, callerUGI, attempts);

  }

  /**
   * 生成应用概览信息表格，填充应用基本信息
   * @param app 应用Web层信息对象
   * @param schedulerPath 调度队列链接路径
   * @param webUiType UI类型（RM/AHS）
   * @param appReport 应用服务端报告对象
   */
  private void generateOverviewTable(AppInfo app, String schedulerPath,
      String webUiType, ApplicationReport appReport) {
    ResponseInfo overviewTable = info("Application Overview")
        .__("User:", schedulerPath, app.getUser())
        .__("Name:", app.getName())
        .__("Application Type:", app.getType())
        .__("Application Tags:",
            app.getApplicationTags() == null ? "" : app.getApplicationTags())
        .__("Application Priority:", clarifyAppPriority(app.getPriority()))
        .__(
            "YarnApplicationState:",
            app.getAppState() == null ? UNAVAILABLE : clarifyAppState(app
                .getAppState()))
        .__("Queue:", schedulerPath, app.getQueue())
        .__("FinalStatus Reported by AM:",
            clairfyAppFinalStatus(app.getFinalAppStatus()))
        .__("Started:", Times.format(app.getStartedTime()))
        .__("Launched:", Times.format(app.getLaunchTime()))
        .__("Finished:", Times.format(app.getFinishedTime()))
        .__("Elapsed:", StringUtils.formatTime(app.getElapsedTime()))
        .__(
            "Tracking URL:",
            app.getTrackingUrl() == null
                || app.getTrackingUrl().equals(UNAVAILABLE) ? null : root_url(app
                .getTrackingUrl()),
            app.getTrackingUrl() == null
                || app.getTrackingUrl().equals(UNAVAILABLE) ? "Unassigned" :
                Apps.isApplicationFinalState(app.getAppState()) ?
                    "History" : "ApplicationMaster");
    // RM WebUI 需要额外显示日志聚合状态和应用超时信息
    if (webUiType != null
        && webUiType.equals(YarnWebParams.RM_WEB_UI)) {
      LogAggregationStatus status = getLogAggregationStatus();
      if (status == null) {
        overviewTable.__("Log Aggregation Status:", "N/A");
      } else if (status == LogAggregationStatus.DISABLED
          || status == LogAggregationStatus.NOT_START
          || status == LogAggregationStatus.SUCCEEDED) {
        overviewTable.__("Log Aggregation Status:", status.name());
      } else {
        overviewTable.__("Log Aggregation Status:",
            root_url("logaggregationstatus", app.getAppId()), status.name());
      }
      // 获取应用生命周期剩余超时时间
      long timeout = appReport.getApplicationTimeouts()
          .get(ApplicationTimeoutType.LIFETIME).getRemainingTime();
      if (timeout < 0) {
        overviewTable.__("Application Timeout (Remaining Time):", "Unlimited");
      } else {
        overviewTable.__("Application Timeout (Remaining Time):",
            String.format("%d seconds", timeout));
      }
    }
    // 添加诊断信息和标签表达式信息
    overviewTable.__("Diagnostics:",
        app.getDiagnosticsInfo() == null ? "" : app.getDiagnosticsInfo());
    overviewTable.__("Unmanaged Application:", app.isUnmanagedApp());
    overviewTable.__("Application Node Label expression:",
        app.getAppNodeLabelExpression() == null ? "<Not set>"
            : app.getAppNodeLabelExpression());
    overviewTable.__("AM container Node Label expression:",
        app.getAmNodeLabelExpression() == null ? "<Not set>"
            : app.getAmNodeLabelExpression());
  }

  /**
   * 生成应用尝试列表表格，渲染所有应用尝试的基本信息
   * @param html HTML块上下文
   * @param callerUGI 当前请求用户信息
   * @param attempts 应用尝试报告集合
   */
  protected void generateApplicationTable(Block html,
      UserGroupInformation callerUGI,
      Collection<ApplicationAttemptReport> attempts) {
    // 创建应用尝试表格表头
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#attempts").thead().tr().th(".id", "Attempt ID")
          .th(".started", "Started").th(".node", "Node").th(".logs", "Logs")
          .__().__().tbody();

    // 构建前端表格需要的JSON数据
    StringBuilder attemptsTableData = new StringBuilder("[\n");
    for (final ApplicationAttemptReport appAttemptReport : attempts) {
      AppAttemptInfo appAttempt = new AppAttemptInfo(appAttemptReport);
      ContainerReport containerReport;
      try {
        // 获取AM容器信息，用于获取节点地址和日志链接
        final GetContainerReportRequest request =
                GetContainerReportRequest.newInstance(
                      appAttemptReport.getAMContainerId());
        if (callerUGI == null) {
          containerReport =
              getContainerReport(request);
        } else {
          containerReport = callerUGI.doAs(
              new PrivilegedExceptionAction<ContainerReport>() {
            @Override
            public ContainerReport run() throws Exception {
              ContainerReport report = null;
              if (request.getContainerId() != null) {
                  try {
                    report = getContainerReport(request);
                  } catch (ContainerNotFoundException ex) {
                    LOG.warn(ex.getMessage());
                  }
              }
              return report;
            }
          });
        }
      } catch (Exception e) {
        String message =
            "Failed to read the AM container of the application attempt "
                + appAttemptReport.getApplicationAttemptId() + ".";
        LOG.error(message, e);
        html.p().__(message).__();
        return;
      }
      long startTime = 0L;
      String logsLink = null;
      String nodeLink = null;
      // 从容器报告中提取节点地址和日志链接
      if (containerReport != null) {
        ContainerInfo container = new ContainerInfo(containerReport);
        startTime = container.getStartedTime();
        logsLink = containerReport.getLogUrl();
        nodeLink = containerReport.getNodeHttpAddress();
      }
      // 拼接表格行数据
      attemptsTableData
        .append("[\"<a href='")
        .append(url("appattempt", appAttempt.getAppAttemptId()))
        .append("'>")
        .append(appAttempt.getAppAttemptId())
        .append("</a>\",\"")
        .append(startTime)
        .append("\",\"<a ")
        .append(nodeLink == null ? "#" : "href='" + nodeLink)
        .append("'>")
        .append(nodeLink == null ? "N/A" : StringEscapeUtils
            .escapeEcmaScript(StringEscapeUtils.escapeHtml4(nodeLink)))
        .append("</a>\",\"<a ")
        .append(logsLink == null ? "#" : "href='" + logsLink).append("'>")
        .append(logsLink == null ? "N/A" : "Logs").append("</a>\"],\n");
    }
    // 移除最后多余的逗号
    if (attemptsTableData.charAt(attemptsTableData.length() - 2) == ',') {
      attemptsTableData.delete(attemptsTableData.length() - 2,
        attemptsTableData.length() - 1);
    }
    attemptsTableData.append("]");
    // 将JSON数据注入到页面脚本中
    html.script().$type("text/javascript")
      .__("var attemptsTableData=" + attemptsTableData).__();

    tbody.__().__();
  }

  protected ContainerReport getContainerReport(
      final GetContainerReportRequest request)
      throws YarnException, IOException {
    return appBaseProt.getContainerReport(request).getContainerReport();
  }

  protected List<ApplicationAttemptReport> getApplicationAttemptsReport(
      final GetApplicationAttemptsRequest request)
      throws YarnException, IOException {
    return appBaseProt.getApplicationAttempts(request)
        .getApplicationAttemptList();
  }