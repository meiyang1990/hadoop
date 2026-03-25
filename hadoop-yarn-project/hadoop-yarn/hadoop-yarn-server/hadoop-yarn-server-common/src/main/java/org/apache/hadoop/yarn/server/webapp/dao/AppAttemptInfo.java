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

package org.apache.hadoop.yarn.server.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;

/**
 * YARN Web UI 应用尝试信息数据访问对象
 * 封装应用尝试的基础信息，用于Web服务返回JSON/XML格式数据
 */
@Public
@Evolving
@XmlRootElement(name = "appAttempt")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppAttemptInfo {

  // 应用尝试ID
  protected String appAttemptId;
  // ApplicationMaster所在节点主机名
  protected String host;
  // ApplicationMaster RPC服务端口
  protected int rpcPort;
  // 应用追踪页面URL
  protected String trackingUrl;
  // 原始应用追踪页面URL
  protected String originalTrackingUrl;
  // 诊断信息，用于错误排查
  protected String diagnosticsInfo;
  // 应用尝试当前状态
  protected YarnApplicationAttemptState appAttemptState;
  // ApplicationMaster所在容器ID
  protected String amContainerId;
  // 应用尝试启动时间戳
  protected long startedTime;
  // 应用尝试完成时间戳
  protected long finishedTime;

  /**
   * JAXB要求的无参构造方法
   */
  public AppAttemptInfo() {
    // JAXB needs this
  }

  /**
   * 根据应用尝试报告构造AppAttemptInfo对象
   * @param appAttempt 应用尝试报告
   */
  public AppAttemptInfo(ApplicationAttemptReport appAttempt) {
    appAttemptId = appAttempt.getApplicationAttemptId().toString();
    host = appAttempt.getHost();
    rpcPort = appAttempt.getRpcPort();
    trackingUrl = appAttempt.getTrackingUrl();
    originalTrackingUrl = appAttempt.getOriginalTrackingUrl();
    diagnosticsInfo = appAttempt.getDiagnostics();
    appAttemptState = appAttempt.getYarnApplicationAttemptState();
    if (appAttempt.getAMContainerId() != null) {
      amContainerId = appAttempt.getAMContainerId().toString();
    }
    startedTime = appAttempt.getStartTime();
    finishedTime = appAttempt.getFinishTime();
  }

  public String getAppAttemptId() {
    return appAttemptId;
  }

  public String getHost() {
    return host;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public String getTrackingUrl() {
    return trackingUrl;
  }

  public String getOriginalTrackingUrl() {
    return originalTrackingUrl;
  }

  public String getDiagnosticsInfo() {
    return diagnosticsInfo;
  }

  public YarnApplicationAttemptState getAppAttemptState() {
    return appAttemptState;
  }

  public String getAmContainerId() {
    return amContainerId;
  }

  public long getStartedTime() {
    return startedTime;
  }

  public long getFinishedTime() {
    return finishedTime;
  }

}