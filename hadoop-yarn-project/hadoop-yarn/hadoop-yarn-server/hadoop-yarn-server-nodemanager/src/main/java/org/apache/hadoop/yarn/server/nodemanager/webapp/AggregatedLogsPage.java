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

import static org.apache.hadoop.yarn.webapp.YarnWebConstants.APP_ID;

import java.io.IOException;
import java.io.PrintWriter;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.AggregatedLogRemover;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.util.YarnWebServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.base.Strings;

/**
 * 聚合日志页面控制器，NodeManager WebUI中用于处理聚合日志的查看请求
 */
@Singleton
public class AggregatedLogsPage extends Controller {

  private static final Logger LOG = LoggerFactory.getLogger(AggregatedLogsPage.class);

  private final NodeManager nm;
  private final ApplicationACLsManager aclsManager;
  private final AggregatedLogRemover aggregatedLogRemover;
  
  @Inject
  /**
   * 构造聚合日志页面控制器
   * @param nm NodeManager实例
   * @param context NodeManager上下文
   * @param aclsManager 应用访问权限管理器
   */
  public AggregatedLogsPage(NodeManager nm, Context context, ApplicationACLsManager aclsManager) {
    this.nm = nm;
    this.aclsManager = aclsManager;
    this.aggregatedLogMetadatRemover = new AggregatedLogRemover(context);
  }
  
  @Override
  /**
   * 处理GET请求，渲染聚合日志页面
   */
  public void index() {
    String appIdStr = $(APP_ID);
    if (appIdStr == null || appIdStr.isEmpty()) {
      setStatus(400);
      redirect("Bad request: missing application ID");
      return;
    }
    ApplicationId appId = null;
    try {
      // 解析应用ID字符串
      appId = ApplicationId.fromString(appIdStr);
    } catch (Exception ex) {
      setStatus(400);
      redirect("Bad request: invalid application ID");
      return;
    }
    // 检查用户访问权限
    if (!YarnWebServiceUtils.hasAccess(request, applicationId.getClusterTimestamp(),
        aclsManager, appId)) {
      setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      redirect("Unauthorized: You don't have permission for this application");
      return;
    }
    // 获取容器ID参数
    String containerIdStr = $(Constants.CONTAINER_ID);
    if (Strings.isNullOrEmpty(containerIdStr)) {
      setStatus(400);
      redirect("Bad request: missing container ID");
      return;
    }
    ContainerId containerId = null;
    try {
      // 解析容器ID字符串
      containerId = ContainerId.fromString(containerIdStr);
    } catch (Exception ex) {
      setStatus(400);
      redirect("Bad request: invalid container ID");
      return;
    }
    // 获取日志类型参数
    String logType = $(Constants.LOG_TYPE);
    if (Strings.isNullOrEmpty(logType)) {
      setStatus(400);
      redirect("Bad request: missing log type");
      return;
    }
    // 获取起始字节参数
    String startStr = $(Constants.START);
    long start = 0;
    if (!Strings.isNullOrEmpty(startStr)) {
      try {
        start = Long.parseLong(startStr);
      } catch (NumberFormatException ex) {
        setStatus(400);
        redirect("Bad request: invalid start value");
        return;
      }
    }
    // 获取结束字节参数
    String endStr = $(Constants.END);
    long end = -1;
    if (!Strings.isNullOrEmpty(endStr)) {
      try {
        end = Long.parseLong(endStr);
      } catch (NumberFormatException ex) {
        setStatus(400);
        redirect("Bad request: invalid end value");
        return;
      }
    }
    setContentType("text/plain");
    PrintWriter out = writer();
    // 获取本地目录处理器
    LocalDirsHandlerService dirHandler = nm.getNodeStatusChecker().getDirsHandler();
    try {
      // 读取指定范围的聚合日志并写入响应
      aggregatedLogRemover.readAggregatedLogs(dirHandler, appId, containerId, logType, start, end, out);
    } catch (IOException ex) {
      LOG.error("Error reading aggregated logs for container {}", containerId, ex);
      out.println("Error reading aggregated logs: " + ex.getMessage());
    }
  }
  
  /**
   * 跳转到错误页面，输出错误信息
   * @param message 错误信息
   */
  private void redirect(String message) {
    try {
      PrintWriter out = writer();
      out.println(message);
    } catch (Exception ex) {
      LOG.error("Error writing error response", ex);
    }
  }
}