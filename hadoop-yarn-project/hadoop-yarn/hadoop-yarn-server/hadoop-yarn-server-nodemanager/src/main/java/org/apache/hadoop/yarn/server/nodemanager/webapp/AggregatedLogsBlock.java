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
package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogInfo;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogRow;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationFileData;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.PerNodeFileLogMeta;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AggregatedLogsInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

/**
 * 聚合日志页面HTML块，在NodeManager WebUI中渲染容器聚合日志展示内容
 */
public class AggregatedLogsBlock extends HtmlBlock {
  
  final Context nmContext;
  final AggregatedLogsInfo logsInfo;
  final ContainerInfo containerInfo;

  /**
   * 构造函数，通过Guice注入依赖初始化聚合日志渲染块
   * @param nmContext NodeManager上下文对象，保存节点运行时状态信息
   * @param logsInfo 聚合日志信息数据访问对象
   * @param containerInfo 容器信息数据访问对象
   */
  @Inject
  public AggregatedLogsBlock(Context nmContext, AggregatedLogsInfo logsInfo,
      ContainerInfo containerInfo) {
    this.nmContext = nmContext;
    this.logsInfo = logsInfo;
    this.containerInfo = containerInfo;
  }

  @Override
  protected void render(Block html) {
    // 解析获取容器ID
    ContainerId containerId = this.containerInfo.getContainerID();
    // 解析获取应用ID
    ApplicationId appId = containerId.getApplicationId();
    // 获取分页参数
    int startRow = this.logsInfo.getStartRow();
    int endRow = this.logsInfo.getEndRow();
    // 从日志存储中获取当前容器的聚合日志元数据
    ContainerLogMeta logMeta = this.logsInfo.getLogMeta();
    if (logMeta == null) {
      // 未找到日志元数据，输出提示信息
      html.p().__("No logs for container " + containerId.toString() +
          " of application " + appId.toString() + " found.").__();
      return;
    }
    // 渲染日志元信息
    renderLogMeta(html, containerId, logMeta);
    // 初始化分页日志数据
    List<AggregatedLogRow> pagedLogs = new ArrayList<>();
    // 获取分页前的所有日志行
    List<AggregatedLogRow> allLogs = getAllLogs(logMeta);
    int totalRows = allLogs.size();
    // 校验分页参数边界
    if (endRow > totalRows) {
      endRow = totalRows;
    }
    // 截取当前页需要展示的日志行
    pagedLogs = allLogs.subList(startRow, endRow);
    // 渲染分页导航栏
    renderPagination(html, startRow, endRow, totalRows);
    // 渲染实际日志内容
    renderLogContent(html, pagedLogs);
  }

  /**
   * 渲染所有聚合日志内容为按行分割的列表
   * @param logMeta 容器聚合日志元数据
   * @return 所有日志行的列表
   */
  private List<AggregatedLogRow> getAllLogs(ContainerLogMeta logMeta) {
    List<AggregatedLogRow> allLogs = new ArrayList<>();
    // 遍历容器在各节点上的日志元数据
    for (PerNodeFileLogMeta nodeLogMeta : logMeta.getPerNodeFileLogMetas()) {
      // 遍历该节点上每个日志文件的数据
      for (ContainerLogAggregationFileData fileData : 
          nodeLogMeta.getContainerLogAggregationFileData()) {
        // 如果当前文件有日志内容，读取所有日志行加入结果列表
        if (fileData.hasLogs()) {
          try {
            fileData.readLogContent();
            allLogs.addAll(fileData.getLogContent());
          } catch (IOException e) {
            // 读取日志失败，添加错误信息行
            allLogs.add(new AggregatedLogRow(allLogs.size() + 1,
                "Error getting log content: " + e.getMessage()));
          }
        }
      }
    }
    return allLogs;
  }

  /**
   * 渲染日志元数据信息块，展示容器、节点、日志文件基本信息
   * @param html HTML块输出对象
   * @param containerId 当前容器ID
   * @param logMeta 聚合日志元数据
   */
  private void renderLogMeta(Block html, ContainerId containerId, 
      ContainerLogMeta logMeta) {
    // 开始输出信息块
    InfoBlock info = new InfoBlock();
    info.row("Container ID", containerId.toString());
    info.row("User", logMeta.getUserName());
    // 遍历所有日志文件元数据，生成跳转链接
    StringBuilder fileNames = new StringBuilder();
    for (PerNodeFileLogMeta nodeFile : logMeta.getPerNodeFileLogMetas()) {
      String nodeId = nodeFile.getNodeId().toString();
      for (AggregatedLogInfo log : this.logsInfo.getLogFiles(nodeFile)) {
        // 生成单个日志文件链接
        fileNames.append(log.getLink()).append(" on node ").append(nodeId);
        fileNames.append("<br />");
      }
    }
    info.row("Log Files", fileNames.toString());
    // 将信息块添加到HTML输出
    info.render(html);
  }

  /**
   * 渲染分页导航控件，提供上下页跳转和当前页码信息
   * @param html HTML块输出对象
   * @param startRow 当前页起始行号
   * @param endRow 当前页结束行号
   * @param totalRows 日志总行数
   */
  private void renderPagination(Block html, int startRow, int endRow, 
      int totalRows) {
    html.p();
    // 如果不是第一页，显示上一页链接
    if (startRow > 0) {
      int prevStart = startRow - (endRow - startRow);
      int prevEnd = startRow;
      if (prevStart < 0) {
        prevStart = 0;
      }
      html.a().href("?start-row=" + prevStart + "&end-row=" + prevEnd)
          .__("Previous").__();
      html.span().__(" | ");
    }
    // 输出当前页范围和总行数信息
    html.span().__("Showing lines " + (startRow + 1) + " to " + endRow + 
        " of " + totalRows).__();
    // 如果还有下一页，显示下一页链接
    if (endRow < totalRows) {
      html.span().__(" | ");
      int nextStart = endRow;
      int nextEnd = endRow + (endRow - startRow);
      html.a().href("?start-row=" + nextStart + "&end-row=" + nextEnd)
          .__("Next").__();
    }
    html.__();
    html.hr();
  }

  /**
   * 渲染日志内容，将分页后的日志行按格式输出到页面
   * @param html HTML块输出对象
   * @param pagedLogs 当前页需要展示的日志行列表
   */
  private void renderLogContent(Block html, List<AggregatedLogRow> pagedLogs) {
    // 创建预格式化块保留日志原始格式
    html.pre();
    for (AggregatedLogRow row : pagedLogs) {
      // 输出单条日志内容，自动转义HTML特殊字符
      html.__(escapeHtml(row.getLog()) + "\n");
    }
    html.__();
  }

  /**
   * 转义HTML特殊字符，避免XSS攻击和页面渲染错位
   * @param input 原始日志字符串
   * @return 转义后的安全字符串
   */
  private String escapeHtml(String input) {
    if (input == null) {
      return null;
    }
    // 替换HTML特殊字符为实体编码
    StringBuilder escaped = new StringBuilder();
    for (char c : input.toCharArray()) {
      switch (c) {
        case '<':
          escaped.append("&lt;");
          break;
        case '>':
          escaped.append("&gt;");
          break;
        case '"':
          escaped.append("&quot;");
          break;
        case '&':
          escaped.append("&amp;");
          break;
        default:
          escaped.append(c);
      }
    }
    return escaped.toString();
  }
}