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
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APPLICATION_ATTEMPT_ID;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.List;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN Web UI 应用尝试详情页面 HTML 块，负责渲染应用尝试的概览信息和容器列表
 */
public class AppAttemptBlock extends HtmlBlock {

  private static final Logger LOG =
      LoggerFactory.getLogger(AppAttemptBlock.class);
  // 应用基础协议客户端，用于向ResourceManager获取信息
  protected ApplicationBaseProtocol appBaseProt;
  // 当前渲染的应用尝试ID
  protected ApplicationAttemptId appAttemptId = null;

  @Inject
  public AppAttemptBlock(ApplicationBaseProtocol appBaseProt, ViewContext ctx) {
    super(ctx);
    this.appBaseProt = appBaseProt;
  }

  @Override
  /**
   * 渲染应用尝试详情页面HTML内容
   */
  protected void render(Block html) {
    // 从请求获取应用尝试ID
    String attemptid = $(APPLICATION_ATTEMPT_ID);
    if (attemptid.isEmpty()) {
      puts("Bad request: requires application attempt ID");
      return;
    }

    try {
      // 解析应用尝试ID
      appAttemptId = ApplicationAttemptId.fromString(attemptid);
    } catch (IllegalArgumentException e) {
      puts("Invalid application attempt ID: " + attemptid);
      return;
    }

    // 获取请求用户UGI，用于特权操作
    UserGroupInformation callerUGI = getCallerUGI();
    ApplicationAttemptReport appAttemptReport;
    try {
      final GetApplicationAttemptReportRequest request =
          GetApplicationAttemptReportRequest.newInstance(appAttemptId);
      if (callerUGI == null) {
        // 无用户信息直接获取应用尝试报告
        appAttemptReport =
            getApplicationAttemptReport(request);
      } else {
        // 以请求用户身份获取应用尝试报告
        appAttemptReport = callerUGI.doAs(
            new PrivilegedExceptionAction<ApplicationAttemptReport> () {
          @Override
          public ApplicationAttemptReport run() throws Exception {
            return getApplicationAttemptReport(request);
          }
        });
      }
    } catch (Exception e) {
      String message =
          "Failed to read the application attempt " + appAttemptId + ".";
      LOG.error(message, e);
      html.p().__(message).__();
      return;
    }

    if (appAttemptReport == null) {
      puts("Application Attempt not found: " + attemptid);
      return;
    }

    boolean exceptionWhenGetContainerReports = false;
    Collection<ContainerReport> containers = null;
    try {
      final GetContainersRequest request =
          GetContainersRequest.newInstance(appAttemptId);
      if (callerUGI == null) {
        // 无用户信息直接获取容器列表
        containers = getContainers(request);
      } else {
        // 以请求用户身份获取容器列表
        containers = callerUGI.doAs(
            new PrivilegedExceptionAction<Collection<ContainerReport>> () {
          @Override
          public Collection<ContainerReport> run() throws Exception {
            return  getContainers(request);
          }
        });
      }
    } catch (RuntimeException e) {
      // 捕获异常标记获取容器失败，抑制findbugs警告
      exceptionWhenGetContainerReports = true;
    } catch (Exception e) {
      exceptionWhenGetContainerReports = true;
    }

    // 包装应用尝试报告为Web层DTO
    AppAttemptInfo appAttempt = new AppAttemptInfo(appAttemptReport);

    // 设置页面标题
    setTitle(join("Application Attempt ", attemptid));

    // 拼接AM节点地址
    String node = "N/A";
    if (appAttempt.getHost() != null && appAttempt.getRpcPort() >= 0
        && appAttempt.getRpcPort() < 65536) {
      node = appAttempt.getHost() + ":" + appAttempt.getRpcPort();
    }
    // 生成概览信息区块
    generateOverview(appAttemptReport, containers, appAttempt, node);

    if (exceptionWhenGetContainerReports) {
      // 获取容器失败，显示错误信息
      html
        .p()
        .__(
          "Sorry, Failed to get containers for application attempt" + attemptid
              + ".").__();
      return;
    }

    // 预留钩子创建资源头空间表格
    createAttemptHeadRoomTable(html);
    // 渲染信息区块
    html.__(InfoBlock.class);

    // 预留钩子创建尝试指标表格
    createTablesForAttemptMetrics(html);

    // 创建容器列表表格表头
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#containers").$style("width:100%")
          .thead().tr().th(".id", "Container ID")
          .th(".node", "Node").th(".exitstatus", "Container Exit Status")
          .th(".logs", "Logs").__().__().tbody();

    // 构建容器表格数据JSON，供前端DataTables使用
    StringBuilder containersTableData = new StringBuilder("[\n");
    for (ContainerReport containerReport : containers) {
      ContainerInfo container = new ContainerInfo(containerReport);
      containersTableData
        .append("[\"<a href='")
        .append(url("container", container.getContainerId()))
        .append("'>")
        .append(container.getContainerId())
        .append("</a>\",\"<a ")
        .append(
          container.getNodeHttpAddress() == null ? "#" : "href='"
              + container.getNodeHttpAddress())
        .append("'>")
        .append(container.getNodeHttpAddress() == null ? "N/A" :
            StringEscapeUtils.escapeEcmaScript(StringEscapeUtils
                .escapeHtml4(container.getNodeHttpAddress())))
        .append("</a>\",\"")
        .append(container.getContainerExitStatus()).append("\",\"<a href='")
        .append(container.getLogUrl() == null ?
            "#" : container.getLogUrl()).append("'>")
        .append(container.getLogUrl() == null ?
            "N/A" : "Logs").append("</a>\"],\n");
    }
    // 移除最后一个多余的逗号
    if (containersTableData.charAt(containersTableData.length() - 2) == ',') {
      containersTableData.delete(containersTableData.length() - 2,
        containersTableData.length() - 1);
    }
    containersTableData.append("]");
    // 输出JSON数据到页面脚本
    html.script().$type("text/javascript")
      .__("var containersTableData=" + containersTableData).__();

    // 结束表格标签
    tbody.__().__();
  }

  /**
   * 从ResourceManager获取当前应用尝试的容器列表
   */
  protected List<ContainerReport> getContainers(
      final GetContainersRequest request) throws YarnException, IOException {
    return appBaseProt.getContainers(request).getContainerList();
  }

  /**
   * 从ResourceManager获取应用尝试报告
   */
  protected ApplicationAttemptReport getApplicationAttemptReport(
      final GetApplicationAttemptReportRequest request)
      throws YarnException, IOException {
    return appBaseProt.getApplicationAttemptReport(request)
        .getApplicationAttemptReport();
  }

  /**
   * 生成应用尝试概览信息面板
   */
  protected void generateOverview(ApplicationAttemptReport appAttemptReport,
      Collection<ContainerReport> containers, AppAttemptInfo appAttempt,
      String node) {
    String amContainerId = appAttempt.getAmContainerId();
    info("Application Attempt Overview")
      .__(
        "Application Attempt State:",
        appAttempt.getAppAttemptState() == null ? UNAVAILABLE : appAttempt
          .getAppAttemptState())
      .__("AM Container:",
          amContainerId == null
              || containers == null
              || !hasAMContainer(appAttemptReport.getAMContainerId(),
                  containers) ? null : root_url("container", amContainerId),
          amContainerId == null ? "N/A" : amContainerId)
      .__("Node:", node)
      .__(
        "Tracking URL:",
        appAttempt.getTrackingUrl() == null
            || appAttempt.getTrackingUrl().equals(UNAVAILABLE) ? null
            : root_url(appAttempt.getTrackingUrl()),
        appAttempt.getTrackingUrl() == null
            || appAttempt.getTrackingUrl().equals(UNAVAILABLE)
            ? "Unassigned"
            : appAttempt.getAppAttemptState() == YarnApplicationAttemptState.FINISHED
                || appAttempt.getAppAttemptState() == YarnApplicationAttemptState.FAILED
                || appAttempt.getAppAttemptState() == YarnApplicationAttemptState.KILLED
                ? "History" : "ApplicationMaster")
      .__(
        "Diagnostics Info:",
        appAttempt.getDiagnosticsInfo() == null ? "" : appAttempt
          .getDiagnosticsInfo());
  }

  /**
   * 在容器列表中检查AM容器是否存在
   */
  protected boolean hasAMContainer(ContainerId containerId,
      Collection<ContainerReport> containers) {
    for (ContainerReport container : containers) {
      if (containerId.equals(container.getContainerId())) {
        return true;
      }
    }
    return false;
  }

  /**
   * 扩展钩子：创建尝试资源头空间表格
   */
  protected void createAttemptHeadRoomTable(Block html) {
    
  }

  /**
   * 扩展钩子：创建尝试指标表格
   */
  protected void createTablesForAttemptMetrics(Block html) {

  }
}