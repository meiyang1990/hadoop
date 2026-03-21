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

package org.apache.hadoop.yarn.server.applicationhistoryservice.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;

/**
 * 应用尝试运行历史数据实体，存储RMAppAttempt需要持久化保存的所有字段数据
 * 供应用历史服务持久化存储、查询应用尝试运行历史信息使用
 */
@Public
@Unstable
public class ApplicationAttemptHistoryData {

  private ApplicationAttemptId applicationAttemptId;

  private String host;

  private int rpcPort;

  private String trackingURL;

  private String diagnosticsInfo;

  private FinalApplicationStatus finalApplicationStatus;

  private ContainerId masterContainerId;

  private YarnApplicationAttemptState yarnApplicationAttemptState;

  /**
   * 创建应用尝试运行历史数据实例，使用所有必填字段初始化
   * @param appAttemptId 应用尝试运行ID
   * @param host AM所在节点主机名
   * @param rpcPort AM RPC服务端口
   * @param masterContainerId AM主容器ID
   * @param diagnosticsInfo 诊断信息
   * @param trackingURL 追踪页面URL
   * @param finalApplicationStatus 应用最终状态
   * @param yarnApplicationAttemptState YARN应用尝试运行状态
   * @return 初始化完成的应用尝试运行历史数据实例
   */
  @Public
  @Unstable
  public static ApplicationAttemptHistoryData newInstance(
      ApplicationAttemptId appAttemptId, String host, int rpcPort,
      ContainerId masterContainerId, String diagnosticsInfo,
      String trackingURL, FinalApplicationStatus finalApplicationStatus,
      YarnApplicationAttemptState yarnApplicationAttemptState) {
    ApplicationAttemptHistoryData appAttemptHD =
        new ApplicationAttemptHistoryData();
    appAttemptHD.setApplicationAttemptId(appAttemptId);
    appAttemptHD.setHost(host);
    appAttemptHD.setRPCPort(rpcPort);
    appAttemptHD.setMasterContainerId(masterContainerId);
    appAttemptHD.setDiagnosticsInfo(diagnosticsInfo);
    appAttemptHD.setTrackingURL(trackingURL);
    appAttemptHD.setFinalApplicationStatus(finalApplicationStatus);
    appAttemptHD.setYarnApplicationAttemptState(yarnApplicationAttemptState);
    return appAttemptHD;
  }

  @Public
  @Unstable
  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  @Public
  @Unstable
  public void
      setApplicationAttemptId(ApplicationAttemptId applicationAttemptId) {
    this.applicationAttemptId = applicationAttemptId;
  }

  @Public
  @Unstable
  public String getHost() {
    return host;
  }

  @Public
  @Unstable
  public void setHost(String host) {
    this.host = host;
  }

  @Public
  @Unstable
  public int getRPCPort() {
    return rpcPort;
  }

  @Public
  @Unstable
  public void setRPCPort(int rpcPort) {
    this.rpcPort = rpcPort;
  }

  @Public
  @Unstable
  public String getTrackingURL() {
    return trackingURL;
  }

  @Public
  @Unstable
  public void setTrackingURL(String trackingURL) {
    this.trackingURL = trackingURL;
  }

  @Public
  @Unstable
  public String getDiagnosticsInfo() {
    return diagnosticsInfo;
  }

  @Public
  @Unstable
  public void setDiagnosticsInfo(String diagnosticsInfo) {
    this.diagnosticsInfo = diagnosticsInfo;
  }

  @Public
  @Unstable
  public FinalApplicationStatus getFinalApplicationStatus() {
    return finalApplicationStatus;
  }

  @Public
  @Unstable
  public void setFinalApplicationStatus(
      FinalApplicationStatus finalApplicationStatus) {
    this.finalApplicationStatus = finalApplicationStatus;
  }

  @Public
  @Unstable
  public ContainerId getMasterContainerId() {
    return masterContainerId;
  }

  @Public
  @Unstable
  public void setMasterContainerId(ContainerId masterContainerId) {
    this.masterContainerId = masterContainerId;
  }

  @Public
  @Unstable
  public YarnApplicationAttemptState getYarnApplicationAttemptState() {
    return yarnApplicationAttemptState;
  }

  @Public
  @Unstable
  public void setYarnApplicationAttemptState(
      YarnApplicationAttemptState yarnApplicationAttemptState) {
    this.yarnApplicationAttemptState = yarnApplicationAttemptState;
  }

}