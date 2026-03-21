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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.ContainerLogFileInfo;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.webapp.ContainerLogsUtils;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfo;
import org.apache.hadoop.yarn.util.Times;

/**
 * NodeManager本地存储容器日志元数据数据访问对象，继承自通用ContainerLogsInfo
 * 用于Web API返回NM本地日志目录中容器日志的元信息
 */
@XmlRootElement(name = "containerLogsInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class NMContainerLogsInfo extends ContainerLogsInfo {

  //JAXB needs this
  public NMContainerLogsInfo() {}

  /**
   * 构造NM容器日志元信息对象，从NodeManager本地加载日志元数据
   * @param nmContext NodeManager上下文对象
   * @param containerId 目标容器ID
   * @param remoteUser 远程请求用户，用于权限检查
   * @param logType 日志聚合类型
   * @throws YarnException 加载日志元数据时抛出异常
   */
  public NMContainerLogsInfo(final Context nmContext,
      final ContainerId containerId, String remoteUser,
      ContainerLogAggregationType logType) throws YarnException {
    this.logType = logType.toString();
    this.containerId = containerId.toString();
    this.nodeId = nmContext.getNodeId().toString();
    this.containerLogsInfo = getContainerLogsInfo(
        containerId, remoteUser, nmContext);
  }

  /**
   * 从NodeManager本地文件系统获取容器所有日志文件的元信息
   * @param id 容器ID
   * @param remoteUser 远程请求用户
   * @param nmContext NodeManager上下文
   * @return 容器日志文件元信息列表
   * @throws YarnException 获取日志目录失败时抛出异常
   */
  private static List<ContainerLogFileInfo> getContainerLogsInfo(
      ContainerId id, String remoteUser, Context nmContext)
      throws YarnException {
    List<ContainerLogFileInfo> logFiles = new ArrayList<>();
    // 获取容器所有日志目录
    List<File> logDirs = ContainerLogsUtils.getContainerLogDirs(
        id, remoteUser, nmContext);
    // 遍历每个日志目录
    for (File containerLogsDir : logDirs) {
      // 列出目录下所有文件
      File[] logs = containerLogsDir.listFiles();
      if (logs != null) {
        // 遍历每个日志文件
        for (File log : logs) {
          // 只处理普通文件，跳过子目录
          if (log.isFile()) {
            // 构造日志文件元信息：文件名、文件大小、最后修改时间
            ContainerLogFileInfo logMeta = new ContainerLogFileInfo(
                log.getName(), Long.toString(log.length()),
                Times.format(log.lastModified()));
            // 添加到结果列表
            logFiles.add(logMeta);
          }
        }
      }
    }
    return logFiles;
  }
}