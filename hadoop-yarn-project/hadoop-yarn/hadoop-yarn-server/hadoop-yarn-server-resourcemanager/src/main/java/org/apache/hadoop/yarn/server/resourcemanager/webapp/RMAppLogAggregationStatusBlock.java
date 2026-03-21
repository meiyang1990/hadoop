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
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APPLICATION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * YARN ResourceManager Web UI 应用日志聚合状态展示块，负责渲染应用级日志聚合状态页面
 */
public class RMAppLogAggregationStatusBlock extends HtmlBlock {

  private static final Logger LOG = LoggerFactory
      .getLogger(RMAppLogAggregationStatusBlock.class);
  private final ResourceManager rm;
  private final Configuration conf;

  @Inject
  RMAppLogAggregationStatusBlock(ViewContext ctx, ResourceManager rm,
      Configuration conf) {
    super(ctx);
    this.rm = rm;
    this.conf = conf;
  }

  @Override
  /**
   * 渲染日志聚合状态HTML页面
   * @param html HTML块输出对象
   */
  protected void render(Block html) {
    // 从请求获取应用ID
    String aid = $(APPLICATION_ID);
    if (aid.isEmpty()) {
      puts("Bad request: requires Application ID");
      return;
    }

    ApplicationId appId;
    try {
      // 将字符串转换为应用ID对象
      appId = Apps.toAppID(aid);
    } catch (Exception e) {
      puts("Invalid Application ID: " + aid);
      return;
    }

    setTitle(join("Application ", aid));

    // 添加日志聚合状态说明表，解释不同状态含义
    DIV<Hamlet> div_description = html.div(_INFO_WRAP);
    TABLE<DIV<Hamlet>> table_description =
        div_description.table("#LogAggregationStatusDecription");
    table_description.
      tr().
        th(_TH, "Log Aggregation Status").
        th(_TH, "Description").
        __();
    table_description.tr().td(LogAggregationStatus.DISABLED.name())
      .td("Log Aggregation is Disabled.").__();
    table_description.tr().td(LogAggregationStatus.NOT_START.name())
      .td("Log Aggregation does not Start.").__();
    table_description.tr().td(LogAggregationStatus.RUNNING.name())
      .td("Log Aggregation is Running.").__();
    table_description.tr().td(LogAggregationStatus.RUNNING_WITH_FAILURE.name())
      .td("Log Aggregation is Running, but has failures "
          + "in previous cycles").__();
    table_description.tr().td(LogAggregationStatus.SUCCEEDED.name())
      .td("Log Aggregation is Succeeded. All of the logs have been "
          + "aggregated successfully.").__();
    table_description.tr().td(LogAggregationStatus.FAILED.name())
      .td("Log Aggregation is Failed. At least one of the logs "
          + "have not been aggregated.").__();
    table_description.tr().td(LogAggregationStatus.TIME_OUT.name())
      .td("The application is finished, but the log aggregation status is "
          + "not updated for a long time. Not sure whether the log aggregation "
          + "is finished or not.").__();
    table_description.__();
    div_description.__();

    // 获取对应应用对象
    RMApp rmApp = rm.getRMContext().getRMApps().get(appId);
    // 创建应用日志聚合状态详情表
    DIV<Hamlet> div = html.div(_INFO_WRAP);
    TABLE<DIV<Hamlet>> table =
        div.h3(
          "Log Aggregation: "
              + (rmApp == null ? "N/A" : rmApp
                .getLogAggregationStatusForAppReport() == null ? "N/A" : rmApp
                .getLogAggregationStatusForAppReport().name())).table(
          "#LogAggregationStatus");

    // 从配置获取内存中保留的最大日志聚合诊断信息条数
    int maxLogAggregationDiagnosticsInMemory = conf.getInt(
      YarnConfiguration.RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY,
      YarnConfiguration.DEFAULT_RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY);
    table
      .tr()
      .th(_TH, "NodeId")
      .th(_TH, "Log Aggregation Status")
      .th(_TH, "Last "
          + maxLogAggregationDiagnosticsInMemory + " Diagnostic Messages")
      .th(_TH, "Last "
          + maxLogAggregationDiagnosticsInMemory + " Failure Messages").__();

    // 如果应用存在，遍历各节点日志聚合报告
    if (rmApp != null) {
      Map<NodeId, LogAggregationReport> logAggregationReports =
          rmApp.getLogAggregationReportsForApp();
      if (logAggregationReports != null && !logAggregationReports.isEmpty()) {
        for (Entry<NodeId, LogAggregationReport> report :
            logAggregationReports.entrySet()) {
          // 获取当前节点日志聚合状态
          LogAggregationStatus status =
              report.getValue() == null ? null : report.getValue()
                .getLogAggregationStatus();
          // 获取诊断信息
          String message =
              report.getValue() == null ? null : report.getValue()
                .getDiagnosticMessage();
          // 获取失败信息
          String failureMessage =
              report.getValue() == null ? null : ((RMAppImpl)rmApp)
                  .getLogAggregationFailureMessagesForNM(report.getKey());
          // 添加表格行输出当前节点日志聚合信息
          table.tr()
            .td(report.getKey().toString())
            .td(status == null ? "N/A" : status.toString())
            .td(message == null ? "N/A" : message)
            .td(failureMessage == null ? "N/A" : failureMessage).__();
        }
      }
    }
    table.__();
    div.__();
  }
}