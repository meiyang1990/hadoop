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
package org.apache.hadoop.mapreduce.v2.hs.webapp.dao;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.util.StringHelper.ujoin;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

/**
 * MR ApplicationMaster尝试运行信息数据访问对象，用于历史服务器Web界面展示AM尝试信息。
 * 封装了AM尝试的节点信息、容器信息、日志链接等基础元数据，支持XML/JSON序列化返回给前端。
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

  @XmlTransient
  protected String shortLogsLink;

  /**
   * 默认无参构造函数，用于JAXB反序列化。
   */
  public AMAttemptInfo() {
  }

  /**
   * 根据AM信息构造历史服务器Web可用的AM尝试信息对象。
   * @param amInfo 原始AM信息对象
   * @param jobId 作业ID
   * @param user 作业提交用户
   * @param host RM/HS服务主机地址
   * @param pathPrefix 路径前缀
   */
  public AMAttemptInfo(AMInfo amInfo, String jobId, String user, String host,
      String pathPrefix) {
    this.nodeHttpAddress = "";
    this.nodeId = "";
    // 获取NodeManager地址信息
    String nmHost = amInfo.getNodeManagerHost();
    int nmHttpPort = amInfo.getNodeManagerHttpPort();
    int nmPort = amInfo.getNodeManagerPort();
    // 拼接节点HTTP地址和节点ID字符串
    if (nmHost != null) {
      this.nodeHttpAddress = nmHost + ":" + nmHttpPort;
      NodeId nodeId = NodeId.newInstance(nmHost, nmPort);
      this.nodeId = nodeId.toString();
    }

    this.id = amInfo.getAppAttemptId().getAttemptId();
    this.startTime = amInfo.getStartTime();
    this.containerId = "";
    this.logsLink = "";
    this.shortLogsLink = "";
    // 获取容器ID并生成完整和短格式日志链接
    ContainerId containerId = amInfo.getContainerId();
    if (containerId != null) {
      this.containerId = containerId.toString();
      this.logsLink = join(host, pathPrefix,
          ujoin("logs", this.nodeId, this.containerId, jobId, user));
      this.shortLogsLink = ujoin("logs", this.nodeHttpAddress, this.containerId,
          jobId, user);
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

  public String getShortLogsLink() {
    return this.shortLogsLink;
  }

}