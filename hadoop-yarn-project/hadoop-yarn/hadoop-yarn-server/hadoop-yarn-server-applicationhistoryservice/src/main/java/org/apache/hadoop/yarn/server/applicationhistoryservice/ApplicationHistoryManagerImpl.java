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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerHistoryData;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 应用历史管理器的具体实现类，负责管理应用历史数据的读写和转换。
 * 
 * 该类通过底层的 ApplicationHistoryStore 持久化历史数据，
 * 并将历史数据转换为 YARN API 定义的标准报告格式供客户端查询。
 */
public class ApplicationHistoryManagerImpl extends AbstractService implements
    ApplicationHistoryManager {
  private static final Logger LOG =
          LoggerFactory.getLogger(ApplicationHistoryManagerImpl.class);
  private static final String UNAVAILABLE = "N/A";

  // 历史数据存储后端
  private ApplicationHistoryStore historyStore;
  // 服务 HTTP 地址，用于构建日志 URL
  private String serverHttpAddress;

  public ApplicationHistoryManagerImpl() {
    super(ApplicationHistoryManagerImpl.class.getName());
  }

  /**
   * 初始化服务：创建并初始化历史存储后端
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    LOG.info("ApplicationHistory Init");
    historyStore = createApplicationHistoryStore(conf);
    historyStore.init(conf);
    // 构建服务器 HTTP 地址，用于生成聚合日志 URL
    serverHttpAddress = WebAppUtils.getHttpSchemePrefix(conf) +
        WebAppUtils.getAHSWebAppURLWithoutScheme(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting ApplicationHistory");
    historyStore.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping ApplicationHistory");
    historyStore.stop();
    super.serviceStop();
  }

  /**
   * 根据配置创建历史存储实例，默认使用文件系统存储
   */
  protected ApplicationHistoryStore createApplicationHistoryStore(
      Configuration conf) {
    return ReflectionUtils.newInstance(conf.getClass(
      YarnConfiguration.APPLICATION_HISTORY_STORE,
      FileSystemApplicationHistoryStore.class,
      ApplicationHistoryStore.class), conf);
  }

  /**
   * 获取指定应用尝试的 AM 容器报告
   */
  @Override
  public ContainerReport getAMContainer(ApplicationAttemptId appAttemptId)
      throws IOException {
    ApplicationReport app =
        getApplication(appAttemptId.getApplicationId());
    return convertToContainerReport(historyStore.getAMContainer(appAttemptId),
        app == null ? null : app.getUser());
  }

  /**
   * 获取指定时间范围内的应用列表
   */
  @Override
  public Map<ApplicationId, ApplicationReport> getApplications(long appsNum,
      long appStartedTimeBegin, long appStartedTimeEnd) throws IOException {
    Map<ApplicationId, ApplicationHistoryData> histData =
        historyStore.getAllApplications();
    HashMap<ApplicationId, ApplicationReport> applicationsReport =
        new HashMap<ApplicationId, ApplicationReport>();
    int count = 0;
    // 遍历所有应用，按启动时间过滤并限制返回数量
    for (Entry<ApplicationId, ApplicationHistoryData> entry : histData
      .entrySet()) {
      if (count == appsNum) {
        break;
      }
      long appStartTime = entry.getValue().getStartTime();
      // 跳过不在时间范围内的应用
      if (appStartTime < appStartedTimeBegin
          || appStartTime > appStartedTimeEnd) {
        continue;
      }
      applicationsReport.put(entry.getKey(),
        convertToApplicationReport(entry.getValue()));
      count++;
    }
    return applicationsReport;
  }

  /**
   * 获取指定应用的报告
   */
  @Override
  public ApplicationReport getApplication(ApplicationId appId)
      throws IOException {
    return convertToApplicationReport(historyStore.getApplication(appId));
  }

  /**
   * 将应用历史数据转换为应用报告格式
   */
  private ApplicationReport convertToApplicationReport(
      ApplicationHistoryData appHistory) throws IOException {
    ApplicationAttemptId currentApplicationAttemptId = null;
    String trackingUrl = UNAVAILABLE;
    String host = UNAVAILABLE;
    int rpcPort = -1;

    // 获取最后一次尝试的信息，用于填充当前尝试 ID、跟踪 URL 等
    ApplicationAttemptHistoryData lastAttempt =
        getLastAttempt(appHistory.getApplicationId());
    if (lastAttempt != null) {
      currentApplicationAttemptId = lastAttempt.getApplicationAttemptId();
      trackingUrl = lastAttempt.getTrackingURL();
      host = lastAttempt.getHost();
      rpcPort = lastAttempt.getRPCPort();
    }
    return ApplicationReport.newInstance(appHistory.getApplicationId(),
      currentApplicationAttemptId, appHistory.getUser(), appHistory.getQueue(),
      appHistory.getApplicationName(), host, rpcPort, null,
      appHistory.getYarnApplicationState(), appHistory.getDiagnosticsInfo(),
      trackingUrl, appHistory.getStartTime(), appHistory.getSubmitTime(), 0,
      appHistory.getFinishTime(), appHistory.getFinalApplicationStatus(),
      null, "", 100, appHistory.getApplicationType(), null);
  }

  /**
   * 获取指定应用的最后一次尝试记录
   */
  private ApplicationAttemptHistoryData getLastAttempt(ApplicationId appId)
      throws IOException {
    Map<ApplicationAttemptId, ApplicationAttemptHistoryData> attempts =
        historyStore.getApplicationAttempts(appId);
    ApplicationAttemptId prevMaxAttemptId = null;
    // 找出尝试 ID 最大的记录（即最后一次尝试）
    for (ApplicationAttemptId attemptId : attempts.keySet()) {
      if (prevMaxAttemptId == null) {
        prevMaxAttemptId = attemptId;
      } else {
        if (prevMaxAttemptId.getAttemptId() < attemptId.getAttemptId()) {
          prevMaxAttemptId = attemptId;
        }
      }
    }
    return attempts.get(prevMaxAttemptId);
  }

  /**
   * 将应用尝试历史数据转换为尝试报告格式
   */
  private ApplicationAttemptReport convertToApplicationAttemptReport(
      ApplicationAttemptHistoryData appAttemptHistory) {
    return ApplicationAttemptReport.newInstance(
      appAttemptHistory.getApplicationAttemptId(), appAttemptHistory.getHost(),
      appAttemptHistory.getRPCPort(), appAttemptHistory.getTrackingURL(), null,
      appAttemptHistory.getDiagnosticsInfo(),
      appAttemptHistory.getYarnApplicationAttemptState(),
      appAttemptHistory.getMasterContainerId());
  }

  /**
   * 获取指定应用尝试的报告
   */
  @Override
  public ApplicationAttemptReport getApplicationAttempt(
      ApplicationAttemptId appAttemptId) throws IOException {
    return convertToApplicationAttemptReport(historyStore
      .getApplicationAttempt(appAttemptId));
  }

  /**
   * 获取指定应用的所有尝试列表
   */
  @Override
  public Map<ApplicationAttemptId, ApplicationAttemptReport>
      getApplicationAttempts(ApplicationId appId) throws IOException {
    Map<ApplicationAttemptId, ApplicationAttemptHistoryData> histData =
        historyStore.getApplicationAttempts(appId);
    HashMap<ApplicationAttemptId, ApplicationAttemptReport> applicationAttemptsReport =
        new HashMap<ApplicationAttemptId, ApplicationAttemptReport>();
    for (Entry<ApplicationAttemptId, ApplicationAttemptHistoryData> entry : histData
      .entrySet()) {
      applicationAttemptsReport.put(entry.getKey(),
        convertToApplicationAttemptReport(entry.getValue()));
    }
    return applicationAttemptsReport;
  }

  /**
   * 获取指定容器的报告
   */
  @Override
  public ContainerReport getContainer(ContainerId containerId)
      throws IOException {
    ApplicationReport app =
        getApplication(containerId.getApplicationAttemptId().getApplicationId());
    return convertToContainerReport(historyStore.getContainer(containerId),
        app == null ? null: app.getUser());
  }

  /**
   * 将容器历史数据转换为容器报告格式
   * 同时生成聚合日志的访问 URL
   */
  private ContainerReport convertToContainerReport(
      ContainerHistoryData containerHistory, String user) {
    // 如果容器有聚合日志，添加服务器根 URL
    String logUrl = WebAppUtils.getAggregatedLogURL(
        serverHttpAddress,
        containerHistory.getAssignedNode().toString(),
        containerHistory.getContainerId().toString(),
        containerHistory.getContainerId().toString(),
        user);
    ContainerReport container = ContainerReport.newInstance(
        containerHistory.getContainerId(),
        containerHistory.getAllocatedResource(),
        containerHistory.getAssignedNode(), containerHistory.getPriority(),
        containerHistory.getStartTime(), containerHistory.getFinishTime(),
        containerHistory.getDiagnosticsInfo(), logUrl,
        containerHistory.getContainerExitStatus(),
        containerHistory.getContainerState(), null);
    container.setExposedPorts(containerHistory.getExposedPorts());
    return container;
  }

  /**
   * 获取指定应用尝试的所有容器列表
   */
  @Override
  public Map<ContainerId, ContainerReport> getContainers(
      ApplicationAttemptId appAttemptId) throws IOException {
    ApplicationReport app =
        getApplication(appAttemptId.getApplicationId());
    Map<ContainerId, ContainerHistoryData> histData =
        historyStore.getContainers(appAttemptId);
    HashMap<ContainerId, ContainerReport> containersReport =
        new HashMap<ContainerId, ContainerReport>();
    for (Entry<ContainerId, ContainerHistoryData> entry : histData.entrySet()) {
      containersReport.put(entry.getKey(),
        convertToContainerReport(entry.getValue(),
            app == null ? null : app.getUser()));
    }
    return containersReport;
  }

  @Private
  @VisibleForTesting
  public ApplicationHistoryStore getHistoryStore() {
    return this.historyStore;
  }
}
