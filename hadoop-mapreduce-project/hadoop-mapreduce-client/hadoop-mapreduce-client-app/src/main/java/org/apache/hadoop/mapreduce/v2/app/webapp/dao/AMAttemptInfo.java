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
package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.util.StringHelper.ujoin;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

/**
 * MR ApplicationMaster尝试信息数据访问对象，用于Web UI序列化展示AM尝试的基本信息。
 * 封装了AM尝试运行所在节点、容器、启动时间等信息，提供给MapReduce应用Web界面展示。
 */
@XmlRootElement(name = "jobAttempt")
@XmlAccessorType(XmlAccessType.FIELD)
public class AMAttemptInfo {

  protected String nodeHttpAddress;
  protected String nodeId;
  protected int id;
  protected long startTime;
  protected String containerId;
  protected String logsLink;

  /**
   * 默认无参构造函数，供JAXB序列化使用。
   */
  public AMAttemptInfo() {
  }

  /**
   * 根据AM原始信息构造Web端可展示的AM尝试信息，同时生成日志链接。
   * @param amInfo AM原始信息对象
   * @param jobId 所属作业ID
   * @param user 作业提交用户
   */
  public AMAttemptInfo(AMInfo amInfo, String jobId, String user) {

    this.nodeHttpAddress = "";
    this.nodeId = "";
    // 获取NodeManager的网络信息
    String nmHost = amInfo.getNodeManagerHost();
    int nmHttpPort = amInfo.getNodeManagerHttpPort();
    int nmPort = amInfo.getNodeManagerPort();
    if (nmHost != null) {
      // 拼接NodeManagerHTTP访问地址
      this.nodeHttpAddress = nmHost + ":" + nmHttpPort;
      // 构造节点ID并序列化
      NodeId nodeId = NodeId.newInstance(nmHost, nmPort);
      this.nodeId = nodeId.toString();
    }

    // 获取AM尝试编号
    this.id = amInfo.getAppAttemptId().getAttemptId();
    // 获取启动时间
    this.startTime = amInfo.getStartTime();
    this.containerId = "";
    this.logsLink = "";
    // 获取AM运行容器ID
    ContainerId containerId = amInfo.getContainerId();
    if (containerId != null) {
      // 保存容器ID字符串
      this.containerId = containerId.toString();
      // 拼接YARN日志访问链接
      this.logsLink = join(MRWebAppUtil.getYARNWebappScheme() + nodeHttpAddress,
          ujoin("node", "containerlogs", this.containerId, user));
    }
  }

  public String getNodeHttpAddress() {
    return this.nodeHttpAddress;
  }

  public String getNodeId() {
    return this.nodeId;
  }

  public int getAttemptId() {
    return this.id;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public String getContainerId() {
    return this.containerId;
  }

  public String getLogsLink() {
    return this.logsLink;
  }

}