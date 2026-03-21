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
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.LogAggregationMetaCollector;
import org.apache.hadoop.yarn.logaggregation.ExtendedLogMetaRequest;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.RemoteLogPathEntry;
import org.apache.hadoop.yarn.server.webapp.dao.RemoteLogPaths;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 聚合日志提取与相关信息查询服务，被AHS、ATS等多个Web服务共享使用
 */
public class LogServlet extends Configured {

  private static final Logger LOG = LoggerFactory
      .getLogger(LogServlet.class);

  private static final Joiner JOINER = Joiner.on("");
  private static final String NM_DOWNLOAD_URI_STR = "/ws/v1/node/containers";

  private LogAggregationFileControllerFactory factoryInstance = null;
  private final AppInfoProvider appInfoProvider;

  /**
   * 构造日志服务Servlet，传入配置和应用信息提供者
   * @param conf 配置对象
   * @param appInfoProvider 应用信息提供者
   */
  public LogServlet(Configuration conf, AppInfoProvider appInfoProvider) {
    super(conf);
    this.appInfoProvider = appInfoProvider;
  }

  /**
   * 获取或创建日志聚合文件控制器工厂（懒加载单例模式）
   * @return 日志聚合文件控制器工厂实例
   */
  private LogAggregationFileControllerFactory getOrCreateFactory() {
    if (factoryInstance != null) {
      return factoryInstance;
    } else {
      factoryInstance = new LogAggregationFileControllerFactory(getConf());
      return factoryInstance;
    }
  }

  @VisibleForTesting
  public String getNMWebAddressFromRM(String nodeId)
      throws JSONException {
    return LogWebServiceUtils.getNMWebAddressFromRM(getConf(), nodeId);
  }

  /**
   * 将容器日志元数据列表转换为Web服务返回的容器日志信息列表
   * @param containerLogMetas 容器日志元数据列表
   * @param emptyLocalContainerLogMeta 是否需要添加空的本地日志元数据条目
   * @return 转换后的容器日志信息列表
   */
  private static List<ContainerLogsInfo> convertToContainerLogsInfo(
      List<ContainerLogMeta> containerLogMetas,
      boolean emptyLocalContainerLogMeta) {
    List<ContainerLogsInfo> containersLogsInfo = new ArrayList<>();
    for (ContainerLogMeta meta : containerLogMetas) {
      ContainerLogsInfo logInfo =
          new ContainerLogsInfo(meta, ContainerLogAggregationType.AGGREGATED);
      containersLogsInfo.add(logInfo);

      if (emptyLocalContainerLogMeta) {
        ContainerLogMeta emptyMeta =
            new ContainerLogMeta(logInfo.getContainerId(),
                logInfo.getNodeId() == null ? "N/A" : logInfo.getNodeId());
        ContainerLogsInfo empty =
            new ContainerLogsInfo(emptyMeta, ContainerLogAggregationType.LOCAL);
        containersLogsInfo.add(empty);
      }
    }
    return containersLogsInfo;
  }

  /**
   * 生成包含容器日志元数据的REST响应
   * @param request 封装后的日志元数据请求
   * @param emptyLocalContainerLogMeta 是否添加空本地日志条目
   * @return 包含日志元数据的REST响应
   */
  private static Response getContainerLogMeta(
      WrappedLogMetaRequest request, boolean emptyLocalContainerLogMeta) {
    try {
      List<ContainerLogMeta> containerLogMeta = request.getContainerLogMetas();
      if (containerLogMeta.isEmpty()) {
        throw new NotFoundException("Can not get log meta for request.");
      }
      List<ContainerLogsInfo> containersLogsInfo = convertToContainerLogsInfo(
          containerLogMeta, emptyLocalContainerLogMeta);

      GenericEntity<List<ContainerLogsInfo>> meta =
          new GenericEntity<List<ContainerLogsInfo>>(containersLogsInfo) {
          };
      Response.ResponseBuilder response = Response.ok(meta);
      // 阻止浏览器对内容类型进行MIME嗅探，避免安全问题
      response.header("X-Content-Type-Options", "nosniff");
      return response.build();
    } catch (Exception ex) {
      LOG.debug("Exception during request", ex);
      throw new WebApplicationException(ex);
    }
  }

  /**
   * 验证用户查询参数合法性：至少指定一个ID，且各ID之间所属关系正确
   * @param applicationId 应用ID
   * @param applicationAttemptId 应用尝试ID
   * @param containerId 容器ID
   */
  private void validateUserInput(ApplicationId applicationId,
      ApplicationAttemptId applicationAttemptId, ContainerId containerId) {
    // 至少指定一个查询参数
    if (applicationId == null && applicationAttemptId == null &&
        containerId == null) {
      throw new IllegalArgumentException("Should set application id, " +
          "application attempt id or container id.");
    }

    // 验证容器ID是否匹配提供的应用尝试ID和应用ID
    if (containerId != null) {
      if (applicationAttemptId != null && !applicationAttemptId.equals(
          containerId.getApplicationAttemptId())) {
        throw new IllegalArgumentException(
            String.format(
                "Container %s does not belong to application attempt %s!",
                containerId, applicationAttemptId));
      }
      if (applicationId != null && !applicationId.equals(
          containerId.getApplicationAttemptId().getApplicationId())) {
        throw new IllegalArgumentException(
            String.format(
                "Container %s does not belong to application %s!",
                containerId, applicationId));
      }
    }

    // 验证应用尝试ID是否匹配提供的应用ID
    if (applicationAttemptId != null && applicationId != null &&
        !applicationId.equals(applicationAttemptId.getApplicationId())) {
      throw new IllegalArgumentException(
          String.format(
                "Application attempt %s does not belong to application %s!",
                applicationAttemptId, applicationId));
    }
  }

  /**
   * 获取每个配置的日志聚合文件控制器对应的远程日志目录路径
   * @param user 远程用户名
   * @param applicationId 应用ID字符串
   * @return 包含远程日志路径的REST响应
   * @throws IOException IO异常
   */
  public Response getRemoteLogDirPath(String user, String applicationId)
      throws IOException {
    String remoteUser = user;
    ApplicationId appId = applicationId != null ?
        ApplicationIdPBImpl.fromString(applicationId) : null;

    // 未指定用户时使用当前登录用户
    if (remoteUser == null) {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      remoteUser = ugi.getUserName();
    }

    List<LogAggregationFileController> fileControllers =
        getOrCreateFactory().getConfiguredLogAggregationFileControllerList();
    List<RemoteLogPathEntry> paths = new ArrayList<>();

    // 遍历所有已配置的文件控制器收集路径
    for (LogAggregationFileController fileController : fileControllers) {
      String path;
      if (appId != null) {
        path = fileController.getRemoteAppLogDir(appId, remoteUser).toString();
      } else {
        path = LogAggregationUtils.getRemoteLogSuffixedDir(
            fileController.getRemoteRootLogDir(),
            remoteUser, fileController.getRemoteRootLogDirSuffix()).toString();
      }

      paths.add(new RemoteLogPathEntry(fileController.getFileControllerName(),
          path));
    }

    RemoteLogPaths result = new RemoteLogPaths(paths);
    Response.ResponseBuilder response = Response.ok().entity(result);
    response.header("X-Content-Type-Options", "nosniff");
    return response.build();
  }

  /**
   * 根据路径参数获取日志元数据信息
   * @param hsr HTTP请求对象
   * @param appIdStr 应用ID字符串
   * @param appAttemptIdStr 应用尝试ID字符串
   * @param containerIdStr 容器ID字符串
   * @param nmId NodeManager ID
   * @param redirectedFromNode 是否从节点重定向而来
   * @param manualRedirection 是否手动重定向（返回Location头而非自动跳转）
   * @return 包含日志信息的REST响应
   */
  public Response getLogsInfo(HttpServletRequest hsr, String appIdStr,
      String appAttemptIdStr, String containerIdStr, String nmId,
      boolean redirectedFromNode, boolean manualRedirection) {
    ApplicationId appId = null;
    if (appIdStr != null) {
      try {
        appId = ApplicationId.fromString(appIdStr);
      } catch (IllegalArgumentException iae) {
        throw new BadRequestException(iae);
      }
    }

    ApplicationAttemptId appAttemptId = null;
    if (appAttemptIdStr != null) {
      try {
        appAttemptId = ApplicationAttemptId.fromString(appAttemptIdStr);
      } catch (IllegalArgumentException iae) {
        throw new BadRequestException(iae);
      }
    }

    ContainerId containerId = null;
    if (containerIdStr != null) {
      try {
        containerId = ContainerId.fromString(containerIdStr);
      } catch (IllegalArgumentException iae) {
        throw new BadRequestException(iae);
      }
    }

    // 验证输入参数合法性
    validateUserInput(appId, appAttemptId, containerId);

    WrappedLogMetaRequest.Builder logMetaRequestBuilder =
        WrappedLogMetaRequest.builder()
            .setApplicationId(appId)
            .setApplicationAttemptId(appAttemptId)
            .setContainerId(containerIdStr);

    // 继续处理请求获取容器日志信息
    return getContainerLogsInfo(hsr, logMetaRequestBuilder, nmId,
        redirectedFromNode, null, manualRedirection);
  }

  /**
   * 根据扩展日志元请求获取批量容器日志信息
   * @param req HTTP请求对象
   * @param logsRequest 扩展日志元请求构建器
   * @return 包含批量日志信息的REST响应
   * @throws IOException IO异常
   */
  public Response getContainerLogsInfo(
      HttpServletRequest req,
      ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder logsRequest)
      throws IOException {
    List<ContainerLogMeta> logs = new ArrayList<>();

    // 未指定用户时使用当前登录用户
    if (!logsRequest.isUserSet()) {
      logsRequest.setUser(UserGroupInformation.getCurrentUser().getUserName());
    }
    LogAggregationMetaCollector collector = new LogAggregationMetaCollector(
        logsRequest.build(), getConf());

    // 从所有已配置的文件控制器收集日志元数据
    for (LogAggregationFileController fc : getOrCreateFactory()
        .getConfiguredLogAggregationFileControllerList()) {
      logs.addAll(collector.collect(fc));
    }

    List<ContainerLogsInfo> containersLogsInfo = convertToContainerLogsInfo(
        logs, false);
    GenericEntity<List<ContainerLogsInfo>> meta =
        new GenericEntity<List<ContainerLogsInfo>>(containersLogsInfo) {
        };
    Response.ResponseBuilder response = Response.ok(meta);
    // 阻止浏览器对内容类型进行MIME嗅探，避免安全问题
    response.header("X-Content-Type-Options", "nosniff");
    return response.build();
  }


  /**
   * 获取指定容器的日志信息，运行中容器会重定向到对应NodeManager获取本地日志
   * @param req HTTP请求对象
   * @param builder 日志元请求构建器
   * @param nmId NodeManager ID
   * @param redirectedFromNode 是否从节点重定向而来
   * @param clusterId 集群ID
   * @param manualRedirection 是否手动重定向（返回Location头而非自动跳转）
   * @return 包含日志信息或重定向指令的REST响应
   */
  public Response getContainerLogsInfo(HttpServletRequest req,
      WrappedLogMetaRequest.Builder builder,
      String nmId, boolean redirectedFromNode,
      String clusterId, boolean manualRedirection) {

    builder.setFactory(getOrCreateFactory());

    BasicAppInfo appInfo;
    try {
      // 从信息提供者获取应用基本信息
      appInfo = appInfoProvider.getApp(req, builder.getAppId(), clusterId);
    } catch (Exception ex) {
      LOG.warn("Could not obtain appInfo object from provider.", ex);
      // 获取应用信息失败，直接从HDFS读取聚合日志返回
      return getContainerLogMeta(builder.build(), false);
    }
    // 应用已完成，直接从HDFS读取聚合日志返回
    if (Apps.isApplicationFinalState(appInfo.getAppState())) {
      return getContainerLogMeta(builder.build(), false);
    }
    // 应用正在运行，尝试重定向到对应NodeManager获取本地日志
    if (LogWebServiceUtils.isRunningState(appInfo.getAppState())) {
      String appOwner = appInfo.getUser();
      builder.setAppOwner(appOwner);
      WrappedLogMetaRequest request = builder.build();

      String nodeHttpAddress = null;
      // 已提供NM ID，尝试从RM获取NM的Web地址
      if (nmId != null && !nmId.isEmpty()) {
        try {
          nodeHttpAddress = getNMWebAddressFromRM(nmId);
        } catch (Exception ex) {
          LOG.info("Exception during getting NM web address.", ex);
        }
      }
      // 未获取到NM地址，尝试通过应用信息提供者查询
      if (nodeHttpAddress == null || nodeHttpAddress.isEmpty()) {
        if (request.getContainerId() != null) {
          try {
            nodeHttpAddress = appInfoProvider.getNodeHttpAddress(
                req, request.getAppId(), request.getAppAttemptId(),
                request.getContainerId().toString(), clusterId);
          } catch (Exception ex) {
            LOG.warn("Could not obtain node HTTP address from provider.", ex);
            // 获取NM地址失败，返回聚合日志元数据，并添加空本地日志条目
            return getContainerLogMeta(request, true);
          }
        }
        // 仍未获取到NM地址 或 请求本身就是从NM重定向来的，直接返回聚合日志
        if (nodeHttpAddress == null || nodeHttpAddress.isEmpty()
            || redirectedFromNode) {
          // 返回聚合日志元数据，如果存在的话，并添加空本地日志条目
          return getContainerLogMeta(request, true);
        }
      }
      ContainerId containerId = request.getContainerId();
      // 没有指定容器ID，无法重定向到单个NM
      if (containerId == null) {
        throw new WebApplicationException(
            new Exception("Could not redirect to node, as app attempt or " +
                "application logs are requested."));
      }
      // 拼接NM端日志下载地址
      String uri = "/" + containerId.toString() + "/logs";
      String resURI = JOINER.join(
          LogWebServiceUtils.getAbsoluteNMWebAddress(getConf(),
              nodeHttpAddress),
          NM_DOWNLOAD_URI_STR, uri);
      String query = req.getQueryString();
      if (query != null && !query.isEmpty()) {
        resURI += "?" + query;
      }
      // 手动重定向模式返回Location头，自动重定向模式返回302跳转响应
      if (manualRedirection) {
        return createLocationResponse(res