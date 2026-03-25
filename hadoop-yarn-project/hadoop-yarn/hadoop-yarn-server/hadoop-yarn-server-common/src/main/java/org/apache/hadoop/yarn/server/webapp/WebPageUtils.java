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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

/**
 * YARN服务端Web页面工具类，提供DataTables表格初始化配置和工具区域生成功能。
 */
public class WebPageUtils {

  /**
   * 生成应用列表表格的初始化JavaScript配置，默认参数。
   * @return DataTables初始化配置字符串
   */
  public static String appsTableInit() {
    return appsTableInit(false, true);
  }

  /**
   * 生成应用列表表格的初始化JavaScript配置，指定是否为RM页面。
   * @param isResourceManager 是否是ResourceManager页面
   * @return DataTables初始化配置字符串
   */
  public static String appsTableInit(boolean isResourceManager) {
    return appsTableInit(false, isResourceManager);
  }

  /**
   * 生成应用列表表格的初始化JavaScript配置，支持自定义页面类型。
   * @param isFairSchedulerPage 是否是公平调度器页面
   * @param isResourceManager 是否是ResourceManager页面
   * @return DataTables初始化配置字符串
   */
  public static String appsTableInit(
      boolean isFairSchedulerPage, boolean isResourceManager) {
    // id, user, name, app type, app tags, queue, priority,
    // starttime, launchtime, finishtime, state, status, progress, ui
    // FairSchedulerPage's table is a bit different
    // This is define in RMAppsBlock.COLUMNS for the RM
    return tableInit()
      .append(", 'aaData': appsTableData")
      .append(", bDeferRender: true")
      .append(", bProcessing: true")
      .append("\n, aoColumnDefs: ")
      .append(getAppsTableColumnDefs(isFairSchedulerPage, isResourceManager))
      // Sort by id upon page load
      .append(", aaSorting: [[0, 'desc']]}").toString();
  }

  /**
   * 根据页面类型生成应用列表表格的列定义配置。
   * @param isFairSchedulerPage 是否是公平调度器页面
   * @param isResourceManager 是否是ResourceManager页面
   * @return DataTables列定义JSON字符串
   */
  private static String getAppsTableColumnDefs(
      boolean isFairSchedulerPage, boolean isResourceManager) {
    // default progress column index is 11
    String progressIndex = "[11]";
    StringBuilder sb = new StringBuilder();
    sb.append("[\n")
      .append("{'sType':'natural', 'aTargets': [0], ")
      .append("'mRender': parseHadoopID },\n")
      .append("{'sType':'num-ignore-str', 'aTargets': [7, 8, 9], ")
      .append("'mRender': renderHadoopDate },\n");
    if (isResourceManager) {
      // Update following line if any column added in RM page before column 11
      sb.append("{'sType':'num-ignore-str', ")
        .append("'aTargets': [12, 13, 14, 15, 16] },\n");
      // set progress column index to 21
      progressIndex = "[21]";
    } else if (isFairSchedulerPage) {
      // Update following line if any column added in scheduler page before column 11
      sb.append("{'sType':'num-ignore-str', ")
        .append("'aTargets': [11, 12, 13, 14, 15] },\n");
      // set progress column index to 16
      progressIndex = "[16]";
    }
    sb.append("{'sType':'numeric', bSearchable:false, 'aTargets':")
      .append(progressIndex)
      .append(", 'mRender': parseHadoopProgress }\n]");
    return sb.toString();
  }

  /**
   * 生成尝试列表表格的初始化JavaScript配置。
   * @return DataTables初始化配置字符串
   */
  public static String attemptsTableInit() {
    return tableInit().append(", 'aaData': attemptsTableData")
      .append(", bDeferRender: true").append(", bProcessing: true")
      .append("\n, aoColumnDefs: ").append(getAttemptsTableColumnDefs())
      // Sort by id upon page load
      .append(", aaSorting: [[0, 'desc']]}").toString();
  }

  /**
   * 生成尝试列表表格的列定义配置。
   * @return DataTables列定义JSON字符串
   */
  private static String getAttemptsTableColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb.append("[\n").append("{'sType':'natural', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }")
      .append("\n, {'sType':'numeric', 'aTargets': [1]")
      .append(", 'mRender': renderHadoopDate }]").toString();
  }

  /**
   * 生成容器列表表格的初始化JavaScript配置。
   * @return DataTables初始化配置字符串
   */
  public static String containersTableInit() {
    return tableInit().append(", 'aaData': containersTableData")
      .append(", bDeferRender: true").append(", bProcessing: true")
      .append("\n, aoColumnDefs: ").append(getContainersTableColumnDefs())
      // Sort by id upon page load
      .append(", aaSorting: [[0, 'desc']]}").toString();
  }

  /**
   * 生成容器列表表格的列定义配置。
   * @return DataTables列定义JSON字符串
   */
  private static String getContainersTableColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb.append("[\n").append("{'sType':'natural', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }]").toString();
  }

  /**
   * 生成资源请求列表表格的初始化JavaScript配置。
   * @return DataTables初始化配置字符串
   */
  public static String resourceRequestsTableInit() {
    return tableInit().append(", 'aaData': resourceRequestsTableData")
        .append(", bDeferRender: true").append(", bProcessing: true}")
        .toString();
  }

  /**
   * 在现有HTML区块后添加工具链接区域，根据配置决定是否启用工具。
   * 如果启用工具则添加配置、日志、栈信息、metrics等常用工具链接；
   * 如果未启用则仅显示禁用提示。
   * @param section 已关闭的HTML div区块
   * @param conf YARN配置对象
   * @return 启用时返回工具区域的无序列表对象，未启用时返回null
   */
  public static Hamlet.UL<Hamlet.DIV<Hamlet>> appendToolSection(
      Hamlet.DIV<Hamlet> section, Configuration conf) {
    boolean isToolsEnabled = conf.getBoolean(
        YarnConfiguration.YARN_WEBAPP_UI1_ENABLE_TOOLS, true);

    Hamlet.DIV<Hamlet> tools = null;
    Hamlet.UL<Hamlet.DIV<Hamlet>> enabledTools = null;

    if (isToolsEnabled) {
      tools = section.h3("Tools");
      enabledTools = tools.ul().li().a("/conf", "Configuration").__().
          li().a("/logs", "Local logs").__().
          li().a("/stacks", "Server stacks").__().
          li().a("/jmx?qry=Hadoop:*", "Server metrics").__();
    } else {
      section.h4("Tools (DISABLED)").__();
    }

    return enabledTools;
  }

}