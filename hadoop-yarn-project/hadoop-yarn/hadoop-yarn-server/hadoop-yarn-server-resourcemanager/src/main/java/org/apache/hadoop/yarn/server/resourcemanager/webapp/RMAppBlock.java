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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.AppBlock;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

/**
 * RM Web UI 应用详情页面区块，扩展通用AppBlock实现ResourceManager侧特定的应用信息渲染逻辑
 */
public class RMAppBlock extends AppBlock{

  private final ResourceManager rm;
  private final Configuration conf;


  @Inject
  RMAppBlock(ViewContext ctx, Configuration conf, ResourceManager rm) {
    super(null, ctx, conf);
    this.conf = conf;
    this.rm = rm;
  }

  @Override
  protected void render(Block html) {
    // 调用父类渲染逻辑
    super.render(html);
  }

  @Override
  protected void createApplicationMetricsTable(Block html){
    // 从RM上下文获取当前应用对象
    RMApp rmApp = this.rm.getRMContext().getRMApps().get(appID);
    // 获取应用整体指标
    RMAppMetrics appMetrics = rmApp == null ? null : rmApp.getRMAppMetrics();
    // Get attempt metrics and fields, it is possible currentAttempt of RMApp is
    // null. In that case, we will assume resource preempted and number of Non
    // AM container preempted on that attempt is 0
    RMAppAttemptMetrics attemptMetrics;
    // 尝试获取当前应用尝试的指标
    if (rmApp == null || null == rmApp.getCurrentAppAttempt()) {
      attemptMetrics = null;
    } else {
      attemptMetrics = rmApp.getCurrentAppAttempt().getRMAppAttemptMetrics();
    }
    // 获取当前尝试被抢占的资源
    Resource attemptResourcePreempted =
        attemptMetrics == null ? Resources.none() : attemptMetrics
          .getResourcePreempted();
    // 获取当前尝试被抢占的非AM容器数量
    int attemptNumNonAMContainerPreempted =
        attemptMetrics == null ? 0 : attemptMetrics
          .getNumNonAMContainersPreempted();
    // 创建信息块容器div
    DIV<Hamlet> pdiv = html.
        __(InfoBlock.class).
        div(_INFO_WRAP);
    // 清空原有信息，设置标题
    info("Application Overview").clear();
    // 构建应用指标表格，依次添加各类抢占资源和聚合分配信息
    info("Application Metrics")
        .__("Total Resource Preempted:",
          appMetrics == null ? "N/A" : appMetrics.getResourcePreempted())
        .__("Total Number of Non-AM Containers Preempted:",
          appMetrics == null ? "N/A"
              : appMetrics.getNumNonAMContainersPreempted())
        .__("Total Number of AM Containers Preempted:",
          appMetrics == null ? "N/A"
              : appMetrics.getNumAMContainersPreempted())
        .__("Resource Preempted from Current Attempt:",
          attemptResourcePreempted)
        .__("Number of Non-AM Containers Preempted from Current Attempt:",
          attemptNumNonAMContainerPreempted)
        .__("Aggregate Resource Allocation:", appMetrics == null ? "N/A" :
            StringHelper
                .getResourceSecondsString(appMetrics.getResourceSecondsMap()))
        .__("Aggregate Preempted Resource Allocation:",
            appMetrics == null ? "N/A" : StringHelper.getResourceSecondsString(
                appMetrics.getPreemptedResourceSecondsMap()));

    // 闭合div标签
    pdiv.__();
  }

  @Override
  protected void generateApplicationTable(Block html,
      UserGroupInformation callerUGI,
      Collection<ApplicationAttemptReport> attempts) {
    // 创建应用尝试列表表格表头
    Hamlet.TBODY<Hamlet.TABLE<Hamlet>> tbody =
        html.table("#attempts").thead().tr().th(".id", "Attempt ID")
            .th(".started", "Started").th(".node", "Node").th(".logs", "Logs")
            .th(".appBlacklistednodes", "Nodes blacklisted by the application",
                "Nodes blacklisted by the app")
            .th(".rmBlacklistednodes", "Nodes blacklisted by the RM for the"
                + " app", "Nodes blacklisted by the system").__().__().tbody();

    // 从RM上下文获取当前应用
    RMApp rmApp = this.rm.getRMContext().getRMApps().get(this.appID);
    if (rmApp == null) {
      return;
    }
    // 初始化前端表格需要的JSON数据
    StringBuilder attemptsTableData = new StringBuilder("[\n");
    // 遍历所有应用尝试报告
    for (final ApplicationAttemptReport appAttemptReport : attempts) {
      // 从RM应用获取对应尝试对象
      RMAppAttempt rmAppAttempt =
          rmApp.getRMAppAttempt(appAttemptReport.getApplicationAttemptId());
      if (rmAppAttempt == null) {
        continue;
      }
      // 构建应用尝试信息对象
      AppAttemptInfo attemptInfo =
          new AppAttemptInfo(this.rm, rmAppAttempt, true, rmApp.getUser(),
              WebAppUtils.getHttpSchemePrefix(conf));
      // 获取应用拉黑的节点列表
      Set<String> nodes = rmAppAttempt.getBlacklistedNodes();
      // nodes which are blacklisted by the application
      String appBlacklistedNodesCount = String.valueOf(nodes.size());
      // nodes which are blacklisted by the RM for AM launches
      // 获取RM拉黑的节点数量
      String rmBlacklistedNodesCount =
          String.valueOf(rmAppAttempt.getAMBlacklistManager()
            .getBlacklistUpdates().getBlacklistAdditions().size());
      // 获取节点HTTP地址
      String nodeLink = attemptInfo.getNodeHttpAddress();
      // 补全HTTP协议前缀
      if (nodeLink != null) {
        nodeLink = WebAppUtils.getHttpSchemePrefix(conf) + nodeLink;
      }
      // 获取日志链接
      String logsLink = attemptInfo.getLogsLink();
      // 拼接当前行JSON数据，添加链接和转义处理
      attemptsTableData
          .append("[\"<a href='")
          .append(url("appattempt", rmAppAttempt.getAppAttemptId().toString()))
          .append("'>")
          .append(String.valueOf(rmAppAttempt.getAppAttemptId()))
          .append("</a>\",\"")
          .append(attemptInfo.getStartTime())
          .append("\",\"<a ")
          .append(nodeLink == null ? "#" : "href='" + nodeLink)
          .append("'>")
          .append(nodeLink == null ? "N/A" : StringEscapeUtils
              .escapeEcmaScript(StringEscapeUtils.escapeHtml4(nodeLink)))
          .append("</a>\",\"<a ")
          .append(logsLink == null ? "#" : "href='" + logsLink).append("'>")
          .append(logsLink == null ? "N/A" : "Logs").append("</a>\",")
          .append("\"").append(appBlacklistedNodesCount).append("\",")
          .append("\"").append(rmBlacklistedNodesCount).append("\"],\n");
    }
    // 移除最后一行多余的逗号
    if (attemptsTableData.charAt(attemptsTableData.length() - 2) == ',') {
      attemptsTableData.delete(attemptsTableData.length() - 2,
          attemptsTableData.length() - 1);
    }
    // 闭合JSON数组
    attemptsTableData.append("]");
    // 将JSON数据注入页面脚本供前端表格使用
    html.script().$type("text/javascript")
        .__("var attemptsTableData=" + attemptsTableData).__();

    // 闭合表格标签
    tbody.__().__();
  }

  @Override
  protected LogAggregationStatus getLogAggregationStatus() {
    // 从RM获取应用日志聚合状态
    RMApp rmApp = this.rm.getRMContext().getRMApps().get(appID);
    if (rmApp == null) {
      return null;
    }
    return rmApp.getLogAggregationStatusForAppReport();
  }

  @Override
  protected ContainerReport getContainerReport(
      final GetContainerReportRequest request)
      throws YarnException, IOException {
    // 调用RM客户端服务获取容器报告
    return rm.getClientRMService().getContainerReport(request)
        .getContainerReport();
  }

  @Override
  protected List<ApplicationAttemptReport> getApplicationAttemptsReport(
      final GetApplicationAttemptsRequest request)
      throws YarnException, IOException {
    // 调用RM客户端服务获取应用尝试列表报告
    return rm.getClientRMService().getApplicationAttempts(request)
        .getApplicationAttemptList();
  }

  @Override
  protected ApplicationReport getApplicationReport(
      final GetApplicationReportRequest request)
      throws YarnException, IOException {
    // 调用RM客户端服务获取应用报告
    return rm.getClientRMService().getApplicationReport(request)
        .getApplicationReport();
  }

}