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

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.ContainerLogFileInfo;

/**
 * YARN Web REST API 容器日志信息数据访问对象，封装容器日志元数据信息
 * <p>
 * 包含的核心信息：
 * <ul>
 *   <li>容器日志文件信息列表</li>
 *   <li>容器ID</li>
 *   <li>容器所在NodeManager节点ID</li>
 *   <li>日志聚合类型（本地日志/聚合日志）</li>
 * </ul>
 */

@XmlRootElement(name = "containerLogsInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class ContainerLogsInfo {

  @XmlElement(name = "containerLogInfo")
  protected List<ContainerLogFileInfo> containerLogsInfo;

  @XmlElement(name = "logAggregationType")
  protected String logType;

  @XmlElement(name = "containerId")
  protected String containerId;

  @XmlElement(name = "nodeId")
  protected String nodeId;

  // JAXB需要无参构造函数用于XML/JSON反序列化
  public ContainerLogsInfo() {}

  /**
   * 从ContainerLogMeta构造容器日志信息对象
   * @param logMeta 容器日志元数据
   * @param logType 日志聚合类型
   */
  public ContainerLogsInfo(ContainerLogMeta logMeta,
      ContainerLogAggregationType logType) {
    this.containerLogsInfo = new ArrayList<>(logMeta.getContainerLogMeta());
    this.logType = logType.toString();
    this.containerId = logMeta.getContainerId();
    this.nodeId = logMeta.getNodeId();
  }

  /**
   * 测试用构造函数，接受字符串类型的日志类型
   * @param logMeta 容器日志元数据
   * @param logType 字符串格式的日志类型
   */
  @VisibleForTesting
  public ContainerLogsInfo(ContainerLogMeta logMeta, String logType) {
    this.containerLogsInfo = new ArrayList<>(logMeta.getContainerLogMeta());
    this.logType = logType;
    this.containerId = logMeta.getContainerId();
    this.nodeId = logMeta.getNodeId();
  }

  public List<ContainerLogFileInfo> getContainerLogsInfo() {
    return this.containerLogsInfo;
  }

  public String getLogType() {
    return this.logType;
  }

  public String getContainerId() {
    return this.containerId;
  }

  public String getNodeId() {
    return this.nodeId;
  }

  public void setContainerLogsInfo(List<ContainerLogFileInfo> containerLogsInfo) {
    this.containerLogsInfo = containerLogsInfo;
  }

  public void setLogType(String logType) {
    this.logType = logType;
  }

  public void setContainerId(String containerId) {
    this.containerId = containerId;
  }

  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }
}