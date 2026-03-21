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

import org.apache.hadoop.classification.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_ID;

/**
 * YARN Web UI 容器详情页面块，负责渲染容器基本信息页面
 */
public class ContainerBlock extends HtmlBlock {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerBlock.class);
  protected ApplicationBaseProtocol appBaseProt;

  /**
   * 构造方法，注入应用基础协议和视图上下文
   * @param appBaseProt 应用基础协议客户端，用于获取容器信息
   * @param ctx 视图上下文
   */
  @Inject
  public ContainerBlock(ApplicationBaseProtocol appBaseProt, ViewContext ctx) {
    super(ctx);
    this.appBaseProt = appBaseProt;
  }

  @Override
  protected void render(Block html) {
    // 从请求参数获取容器ID
    String containerid = $(CONTAINER_ID);
    if (containerid.isEmpty()) {
      puts("Bad request: requires container ID");
      return;
    }

    ContainerId containerId = null;
    try {
      // 解析容器ID字符串
      containerId = ContainerId.fromString(containerid);
    } catch (IllegalArgumentException e) {
      puts("Invalid container ID: " + containerid);
      return;
    }

    // 获取当前请求用户信息
    UserGroupInformation callerUGI = getCallerUGI();
    ContainerReport containerReport = null;
    try {
      // 创建获取容器报告请求
      final GetContainerReportRequest request =
          GetContainerReportRequest.newInstance(containerId);
      if (callerUGI == null) {
        containerReport = getContainerReport(request);
      } else {
        // 以请求用户身份获取容器报告
        containerReport = callerUGI.doAs(
            new PrivilegedExceptionAction<ContainerReport> () {
          @Override
          public ContainerReport run() throws Exception {
            return getContainerReport(request);
          }
        });
      }
    } catch (Exception e) {
      String message = "Failed to read the container " + containerid + ".";
      LOG.error(message, e);
      html.p().__(message).__();
      return;
    }

    if (containerReport == null) {
      puts("Container not found: " + containerid);
      return;
    }

    // 包装容器报告为DAO对象
    ContainerInfo container = new ContainerInfo(containerReport);
    // 设置页面标题
    setTitle(join("Container ", containerid));

    // 构建容器概览信息块
    info("Container Overview")
      .__(
        "Container State:",
        container.getContainerState() == null ? UNAVAILABLE : container
          .getContainerState())
      .__("Exit Status:", container.getContainerExitStatus())
      .__(
        "Node:",
        container.getNodeHttpAddress() == null ? "#" : container
          .getNodeHttpAddress(),
        container.getNodeHttpAddress() == null ? "N/A" : container
          .getNodeHttpAddress())
      .__("Priority:", container.getPriority())
      .__("Started:", Times.format(container.getStartedTime()))
      .__(
        "Elapsed:",
        StringUtils.formatTime(Times.elapsed(container.getStartedTime(),
          container.getFinishedTime())))
      .__(
        "Resource:", getResources(container))
      .__("Logs:", container.getLogUrl() == null ? "#" : container.getLogUrl(),
          container.getLogUrl() == null ? "N/A" : "Logs")
      .__("Diagnostics:", container.getDiagnosticsInfo() == null ?
          "" : container.getDiagnosticsInfo());

    // 渲染信息块到页面
    html.__(InfoBlock.class);
  }

  /**
   * 格式化容器分配资源为字符串，内存和vCore始终排在最前，自定义资源在后
   * @param container 容器信息对象
   * @return 格式化后的资源字符串
   */
  @VisibleForTesting
  String getResources(ContainerInfo container) {
    Map<String, Long> allocatedResources = container.getAllocatedResources();

    StringBuilder sb = new StringBuilder();
    // 先添加内存资源
    sb.append(getResourceAsString(ResourceInformation.MEMORY_URI,
        allocatedResources.get(ResourceInformation.MEMORY_URI))).append(", ");
    // 再添加CPU核心资源
    sb.append(getResourceAsString(ResourceInformation.VCORES_URI,
        allocatedResources.get(ResourceInformation.VCORES_URI)));

    // 添加自定义资源
    if (container.hasCustomResources()) {
      container.getAllocatedResources().forEach((key, value) -> {
        // 跳过已添加的内存和vCore
        if (!key.equals(ResourceInformation.MEMORY_URI) &&
            !key.equals(ResourceInformation.VCORES_URI)) {
          sb.append(", ");
          sb.append(getResourceAsString(key, value));
        }
      });
    }

    return sb.toString();
  }

  /**
   * 格式化单个资源为字符串，转换内置资源的显示名称
   * @param resourceName 资源标识符
   * @param value 资源值
   * @return 格式化后的资源字符串
   */
  private String getResourceAsString(String resourceName, long value) {
    final String translatedResourceName;
    switch (resourceName) {
    case ResourceInformation.MEMORY_URI:
      translatedResourceName = "Memory";
      break;
    case ResourceInformation.VCORES_URI:
      translatedResourceName = "VCores";
      break;
    default:
      translatedResourceName = resourceName;
      break;
    }
    return String.valueOf(value) + " " + translatedResourceName;
  }

  /**
   * 调用API获取容器报告
   * @param request 获取容器报告请求
   * @return 容器报告
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  protected ContainerReport getContainerReport(
      final GetContainerReportRequest request)
      throws YarnException, IOException {
    return appBaseProt.getContainerReport(request).getContainerReport();
  }
}