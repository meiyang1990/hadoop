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

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_PROGRESSBAR_VALUE;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.webapp.AppsBlock;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.THEAD;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TR;

import com.google.inject.Inject;

/**
 * RM Web UI 应用列表页面内容块，负责渲染ResourceManager侧的所有应用列表表格
 */
public class RMAppsBlock extends AppsBlock {

  private ResourceManager rm;

  /** Columns for the Apps RM page. */
  static final ColumnHeader[] COLUMNS = {
      new ColumnHeader(".id", "ID"),
      new ColumnHeader(".user", "User"),
      new ColumnHeader(".name", "Name"),
      new ColumnHeader(".type", "Application Type"),
      new ColumnHeader(".apptag", "Application Tags"),
      new ColumnHeader(".queue", "Queue"),
      new ColumnHeader(".priority", "Application Priority"),
      new ColumnHeader(".starttime", "StartTime"),
      new ColumnHeader(".IDlaunchtime", "LaunchTime"),
      new ColumnHeader(".finishtime", "FinishTime"),
      new ColumnHeader(".state", "State"),
      new ColumnHeader(".finalstatus", "FinalStatus"),
      new ColumnHeader(".runningcontainer", "Running Containers"),
      new ColumnHeader(".allocatedCpu", "Allocated CPU VCores"),
      new ColumnHeader(".allocatedMemory", "Allocated Memory MB"),
      new ColumnHeader(".allocatedGpu", "Allocated GPUs"),
      new ColumnHeader(".reservedCpu", "Reserved CPU VCores"),
      new ColumnHeader(".reservedMemory", "Reserved Memory MB"),
      new ColumnHeader(".reservedGpu", "Reserved GPUs"),
      new ColumnHeader(".queuePercentage", "% of Queue"),
      new ColumnHeader(".clusterPercentage", "% of Cluster"),
      new ColumnHeader(".progress", "Progress"),
      new ColumnHeader(".ui", "Tracking UI"),
      new ColumnHeader(".blacklisted", "Blacklisted Nodes"),
  };

  @Inject
  RMAppsBlock(ResourceManager rm, View.ViewContext ctx) {
    super(null, ctx);
    this.rm = rm;
  }

  @Override
  protected void renderData(Block html) {
    // 创建表格表头行
    TR<THEAD<TABLE<Hamlet>>> tr = html.table("#apps").thead().tr();
    // 遍历所有列定义生成表头单元格
    for (ColumnHeader col : COLUMNS) {
      tr = tr.th(col.getSelector(), col.getCData());
    }
    // 结束表头，创建表体
    TBODY<TABLE<Hamlet>> tbody = tr.__().__().tbody();

    // 构建前端表格需要的JSON数据
    StringBuilder appsTableData = new StringBuilder("[\n");
    // 遍历所有符合条件的应用报告
    for (ApplicationReport appReport : appReports) {
      // TODO: remove the following condition. It is still here because
      // the history side implementation of ApplicationBaseProtocol
      // hasn't filtering capability (YARN-1819).
      // 过滤不符合请求状态的应用
      if (!reqAppStates.isEmpty()
          && !reqAppStates.contains(appReport.getYarnApplicationState())) {
        continue;
      }

      // 包装应用报告为Web层AppInfo对象
      AppInfo app = new AppInfo(appReport);
      // 解析当前应用尝试ID
      ApplicationAttemptId appAttemptId = ApplicationAttemptId.fromString(
          app.getCurrentAppAttemptId());
      // 初始化占比数据
      String queuePercent = "N/A";
      String clusterPercent = "N/A";
      // 如果存在资源使用报告，计算队列和集群占比
      if(appReport.getApplicationResourceUsageReport() != null) {
        queuePercent = String.format("%.1f",
            appReport.getApplicationResourceUsageReport()
                .getQueueUsagePercentage());
        clusterPercent = String.format("%.1f",
            appReport.getApplicationResourceUsageReport().getClusterUsagePercentage());
      }

      // 初始化黑名单节点数量
      String blacklistedNodesCount = "N/A";
      // 从RM上下文获取应用实例
      RMApp rmApp = rm.getRMContext().getRMApps()
          .get(appAttemptId.getApplicationId());
      // 标记应用是否已完成
      boolean isAppInCompletedState = false;
      // 如果应用存在，获取黑名单节点数量
      if (rmApp != null) {
        RMAppAttempt appAttempt = rmApp.getRMAppAttempt(appAttemptId);
        Set<String> nodes =
            null == appAttempt ? null : appAttempt.getBlacklistedNodes();
        if (nodes != null) {
          blacklistedNodesCount = String.valueOf(nodes.size());
        }
        isAppInCompletedState = rmApp.isAppInCompletedStates();
      }
      // 格式化应用进度百分比
      String percent = StringUtils.format("%.1f", app.getProgress());
      // 拼接应用ID链接
      appsTableData
        .append("[\"<a href='")
        .append(url("app", app.getAppId()))
        .append("'>")
        .append(app.getAppId())
        .append("</a>\",\"")
        // 转义用户名字符串，避免XSS和JSON解析错误
        .append(
          StringEscapeUtils.escapeEcmaScript(
              StringEscapeUtils.escapeHtml4(app.getUser())))
        .append("\",\"")
        // 转义应用名
        .append(
          StringEscapeUtils.escapeEcmaScript(
              StringEscapeUtils.escapeHtml4(app.getName())))
        .append("\",\"")
        // 转义应用类型
        .append(
          StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(app
            .getType())))
        .append("\",\"")
        // 转义应用标签
        .append(
          StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(
            app.getApplicationTags() == null ? "" : app.getApplicationTags())))
        .append("\",\"")
        // 转义队列名
        .append(
          StringEscapeUtils.escapeEcmaScript(StringEscapeUtils.escapeHtml4(app
             .getQueue()))).append("\",\"").append(String
             .valueOf(app.getPriority()))
        .append("\",\"").append(app.getStartedTime())
        .append("\",\"").append(app.getLaunchTime())
        .append("\",\"").append(app.getFinishedTime())
        .append("\",\"")
        // 处理应用状态空值
        .append(app.getAppState() == null ? UNAVAILABLE : app.getAppState())
        .append("\",\"")
        .append(app.getFinalAppStatus())
        .append("\",\"")
        // 处理运行容器数量空值
        .append(app.getRunningContainers() == -1 ? "N/A" : String
            .valueOf(app.getRunningContainers()))
        .append("\",\"")
        // 处理已分配CPU空值
        .append(app.getAllocatedCpuVcores() == -1 ? "N/A" : String
            .valueOf(app.getAllocatedCpuVcores()))
        .append("\",\"")
        // 处理已分配内存空值
        .append(app.getAllocatedMemoryMB() == -1 ? "N/A" :
            String.valueOf(app.getAllocatedMemoryMB()))
        .append("\",\"")
        // 处理已分配GPU空值，已完成应用无GPU显示不可用
        .append((isAppInCompletedState && app.getAllocatedGpus() <= 0)
            ? UNAVAILABLE : String.valueOf(app.getAllocatedGpus()))
        .append("\",\"")
        // 处理预留CPU空值
        .append(app.getReservedCpuVcores() == -1 ? "N/A" : String
            .valueOf(app.getReservedCpuVcores()))
        .append("\",\"")
        // 处理预留内存空值
        .append(app.getReservedMemoryMB() == -1 ? "N/A" :
            String.valueOf(app.getReservedMemoryMB()))
        .append("\",\"")
        // 处理预留GPU空值，已完成应用无预留GPU显示不可用
        .append((isAppInCompletedState && app.getReservedGpus() <= 0)
            ? UNAVAILABLE : String.valueOf(app.getReservedGpus()))
        .append("\",\"")
        .append(queuePercent)
        .append("\",\"")
        .append(clusterPercent)
        .append("\",\"")
        // 生成进度条HTML
          .append("<br title='").append(percent).append("'> <div class='")
        .append(C_PROGRESSBAR).append("' title='").append(join(percent, '%'))
        .append("'> ").append("<div class='").append(C_PROGRESSBAR_VALUE)
        .append("' style='").append(join("width:", percent, '%'))
        .append("'> </div> </div>").append("\",\"<a ");

      // 获取跟踪URL，新建应用无有效URL
      String trackingURL =
          app.getTrackingUrl() == null
              || app.getTrackingUrl().equals(UNAVAILABLE)
              || app.getAppState() == YarnApplicationState.NEW ? null : app
              .getTrackingUrl();

      // 根据应用状态确定跟踪UI类型（历史/ApplicationMaster/未分配）
      String trackingUI =
          app.getTrackingUrl() == null
              || app.getTrackingUrl().equals(UNAVAILABLE)
              || app.getAppState() == YarnApplicationState.NEW ? "Unassigned"
              : Apps.isApplicationFinalState(app.getAppState()) ?
              "History" : "ApplicationMaster";
      // 拼接跟踪UI链接
      appsTableData.append(trackingURL == null ? "#" : "href='" + trackingURL)
        .append("'>").append(trackingUI).append("</a>\",").append("\"")
        .append(blacklistedNodesCount).append("\"],\n");

    }
    // 移除最后一个元素多余的逗号
    if (appsTableData.charAt(appsTableData.length() - 2) == ',') {
      appsTableData.delete(appsTableData.length() - 2,
        appsTableData.length() - 1);
    }
    // 闭合JSON数组
    appsTableData.append("]");
    // 将JSON数据注入页面脚本
    html.script().$type("text/javascript")
      .__("var appsTableData=" + appsTableData).__();

    // 闭合表格标签
    tbody.__().__();
  }

  @Override
  /**
   * 从ResourceManager客户端服务获取符合条件的应用报告列表
   * @param request 获取应用请求，包含过滤条件
   * @return 符合条件的应用报告列表
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  protected List<ApplicationReport> getApplicationReport(
      final GetApplicationsRequest request) throws YarnException, IOException {
    return rm.getClientRMService().getApplications(request)
        .getApplicationList();
  }
}