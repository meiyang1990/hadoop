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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import static org.apache.hadoop.yarn.util.StringHelper.PATH_JOINER;

/**
 * YARN ResourceManager Web UI 应用尝试信息数据访问对象，封装应用尝试的基础信息用于Web展示
 */
@XmlRootElement(name = "appAttempt")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppAttemptInfo {

  protected int id;
  protected long startTime;
  protected long finishedTime;
  protected String containerId;
  protected String nodeHttpAddress;
  protected String nodeId;
  protected String logsLink;
  protected String blacklistedNodes;
  private String nodesBlacklistedBySystem;
  protected String appAttemptId;
  private String exportPorts;
  private RMAppAttemptState appAttemptState;

  public AppAttemptInfo() {
  }

  /**
   * 从RM应用尝试对象构造应用尝试信息，填充各类展示字段
   * @param rm ResourceManager实例
   * @param attempt RM应用尝试对象
   * @param hasAccess 用户是否有权限访问该应用
   * @param user 用户名，用于日志链接构造
   * @param schemePrefix HTTP/HTTPS协议前缀，用于日志链接构造
   */
  public AppAttemptInfo(ResourceManager rm, RMAppAttempt attempt,
      Boolean hasAccess, String user, String schemePrefix) {
    this.startTime = 0;
    this.containerId = "";
    this.nodeHttpAddress = "";
    this.nodeId = "";
    this.logsLink = "";
    this.blacklistedNodes = "";
    this.exportPorts = "";
    if (attempt != null) {
      // 填充应用尝试基础标识信息
      this.id = attempt.getAppAttemptId().getAttemptId();
      this.startTime = attempt.getStartTime();
      this.finishedTime = attempt.getFinishTime();
      this.appAttemptState = attempt.getAppAttemptState();
      this.appAttemptId = attempt.getAppAttemptId().toString();
      Container masterContainer = attempt.getMasterContainer();
      if (masterContainer != null && hasAccess) {
        // 填充AM容器所在节点信息
        this.containerId = masterContainer.getId().toString();
        this.nodeHttpAddress = masterContainer.getNodeHttpAddress();
        this.nodeId = masterContainer.getNodeId().toString();

        // 获取日志服务器配置，构造日志链接
        Configuration conf = rm.getRMContext().getYarnConfiguration();
        String logServerUrl = conf.get(YarnConfiguration.YARN_LOG_SERVER_URL);
        // 已完成的应用尝试使用聚合日志服务器链接
        if ((this.appAttemptState == RMAppAttemptState.FAILED ||
            this.appAttemptState == RMAppAttemptState.FINISHED ||
            this.appAttemptState == RMAppAttemptState.KILLED) &&
            logServerUrl != null) {
          this.logsLink = PATH_JOINER.join(logServerUrl,
               masterContainer.getNodeId().toString(),
               masterContainer.getId().toString(),
               masterContainer.getId().toString(), user);
        } else {
          // 运行中应用直接链接到NodeManager的运行日志
          this.logsLink = WebAppUtils.getRunningLogURL(schemePrefix
               + masterContainer.getNodeHttpAddress(),
               masterContainer.getId().toString(), user);
        }
        // 序列化导出端口信息为JSON
        Gson gson = new Gson();
        this.exportPorts = gson.toJson(masterContainer.getExposedPorts());

        // 收集系统拉黑的节点列表
        nodesBlacklistedBySystem =
            StringUtils.join(attempt.getAMBlacklistManager()
              .getBlacklistUpdates().getBlacklistAdditions(), ", ");
        // 收集调度层拉黑的节点列表
        if (rm.getResourceScheduler() instanceof AbstractYarnScheduler) {
          AbstractYarnScheduler ayScheduler =
              (AbstractYarnScheduler) rm.getResourceScheduler();
          SchedulerApplicationAttempt sattempt =
              ayScheduler.getApplicationAttempt(attempt.getAppAttemptId());
          if (sattempt != null) {
            blacklistedNodes =
                StringUtils.join(sattempt.getBlacklistedNodes(), ", ");
          }
        }
      }
    }
  }

  public int getAttemptId() {
    return this.id;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public long getFinishedTime() {
    return this.finishedTime;
  }

  public String getNodeHttpAddress() {
    return this.nodeHttpAddress;
  }

  public String getLogsLink() {
    return this.logsLink;
  }

  public String getAppAttemptId() {
    return this.appAttemptId;
  }

  public RMAppAttemptState getAppAttemptState() {
    return this.appAttemptState;
  }
}