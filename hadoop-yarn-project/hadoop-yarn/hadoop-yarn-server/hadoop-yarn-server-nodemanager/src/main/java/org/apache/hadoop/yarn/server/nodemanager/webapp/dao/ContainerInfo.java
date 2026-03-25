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

package org.apache.hadoop.yarn.server.nodemanager.webapp.dao;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.util.StringHelper.ujoin;

import javax.xml.bind.annotation.*;

import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.webapp.ContainerLogsUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * NodeManager Web UI容器信息数据访问对象，封装容器基本信息、资源信息和日志信息，用于Web接口返回JSON/XML格式数据
 */
@XmlRootElement(name = "container")
@XmlAccessorType(XmlAccessType.FIELD)
public class ContainerInfo {

  protected String id;
  protected String state;
  protected int exitCode;
  protected String diagnostics;
  protected String user;
  protected long totalMemoryNeededMB;
  protected long totalVCoresNeeded;
  private String executionType;
  protected String containerLogsLink;
  protected String nodeId;
  @XmlTransient
  protected String containerLogsShortLink;
  @XmlTransient
  protected String exitStatus;

  protected List<String> containerLogFiles;

  /**
   * JAXB要求的无参构造函数，用于序列化/反序列化
   */
  public ContainerInfo() {
  } // JAXB needs this

  /**
   * 构造函数，基于NM上下文和容器对象创建ContainerInfo
   * @param nmContext NodeManager上下文
   * @param container 容器对象
   */
  public ContainerInfo(final Context nmContext, final Container container) {
    this(nmContext, container, "", "", "");
  }

  /**
   * 完整构造函数，基于NM上下文、容器对象和请求信息构建完整容器信息
   * @param nmContext NodeManager上下文
   * @param container 容器对象
   * @param requestUri 请求URI
   * @param pathPrefix 路径前缀
   * @param remoteUser 远程访问用户
   */
  public ContainerInfo(final Context nmContext, final Container container,
       String requestUri, String pathPrefix, String remoteUser) {

    // 设置容器ID和当前节点ID
    this.id = container.getContainerId().toString();
    this.nodeId = nmContext.getNodeId().toString();
    // 获取容器状态副本
    ContainerStatus containerData = container.cloneAndGetContainerStatus();
    // 设置退出码
    this.exitCode = containerData.getExitStatus();
    // 处理退出状态显示，INVALID状态显示N/A
    this.exitStatus =
        (this.exitCode == ContainerExitStatus.INVALID) ?
            "N/A" : String.valueOf(exitCode);
    // 设置容器状态字符串
    this.state = container.getContainerState().toString();
    // 设置诊断信息，空值转空字符串
    this.diagnostics = containerData.getDiagnostics();
    if (this.diagnostics == null || this.diagnostics.isEmpty()) {
      this.diagnostics = "";
    }

    // 设置容器所属用户
    this.user = container.getUser();
    // 获取容器申请资源
    Resource res = container.getResource();
    if (res != null) {
      // 提取内存和vcore信息
      this.totalMemoryNeededMB = res.getMemorySize();
      this.totalVCoresNeeded = res.getVirtualCores();
    }
    // 获取容器执行类型
    this.executionType =
        container.getContainerTokenIdentifier().getExecutionType().name();
    // 生成日志短链接
    this.containerLogsShortLink = ujoin("containerlogs", this.id,
        container.getUser());

    // 空值处理
    if (requestUri == null) {
      requestUri = "";
    }
    if (pathPrefix == null) {
      pathPrefix = "";
    }
    // 拼接完整日志链接
    this.containerLogsLink = join(requestUri, pathPrefix,
        this.containerLogsShortLink);
    // 获取容器所有日志文件名列表
    this.containerLogFiles =
        getContainerLogFiles(container.getContainerId(), remoteUser, nmContext);
  }

  public String getId() {
    return this.id;
  }

  public String getNodeId() {
    return this.nodeId;
  }

  public String getState() {
    return this.state;
  }

  public int getExitCode() {
    return this.exitCode;
  }

  public String getExitStatus() {
    return this.exitStatus;
  }

  public String getDiagnostics() {
    return this.diagnostics;
  }

  public String getUser() {
    return this.user;
  }

  public String getShortLogLink() {
    return this.containerLogsShortLink;
  }

  public String getLogLink() {
    return this.containerLogsLink;
  }

  public long getMemoryNeeded() {
    return this.totalMemoryNeededMB;
  }

  public long getVCoresNeeded() {
    return this.totalVCoresNeeded;
  }

  public String getExecutionType() {
    return this.executionType;
  }

  public List<String> getContainerLogFiles() {
    return this.containerLogFiles;
  }

  /**
   * 从本地磁盘获取容器所有日志文件名称列表
   * @param id 容器ID
   * @param remoteUser 远程访问用户
   * @param nmContext NodeManager上下文
   * @return 日志文件名列表
   */
  private List<String> getContainerLogFiles(ContainerId id, String remoteUser,
      Context nmContext) {
    List<String> logFiles = new ArrayList<>();
    try {
      // 获取容器所有日志目录
      List<File> logDirs =
          ContainerLogsUtils.getContainerLogDirs(id, remoteUser, nmContext);
      // 遍历每个日志目录
      for (File containerLogsDir : logDirs) {
        File[] logs = containerLogsDir.listFiles();
        if (logs != null) {
          // 遍历目录中文件，只添加普通文件名称
          for (File log : logs) {
            if (log.isFile()) {
              logFiles.add(log.getName());
            }
          }
        }
      }
    } catch (Exception ye) {
      // 异常返回已收集的日志文件列表
      return logFiles;
    }
    return logFiles;
  }

}