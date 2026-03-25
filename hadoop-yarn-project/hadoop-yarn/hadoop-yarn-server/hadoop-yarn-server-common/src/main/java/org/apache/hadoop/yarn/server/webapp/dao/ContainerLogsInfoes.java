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

import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * YARN Web REST API 容器日志信息列表封装实体，用于序列化返回给前端的多容器日志信息集合
 */
@XmlRootElement
public class ContainerLogsInfoes {
  // 容器日志信息列表
  private List<ContainerLogsInfo> containerLogsInfo;

  /**
   * 带参数构造方法，初始化容器日志信息列表
   * @param containerLogsInfo 容器日志信息列表
   */
  public ContainerLogsInfoes(List<ContainerLogsInfo> containerLogsInfo) {
    this.containerLogsInfo = containerLogsInfo;
  }

  /**
   * 默认无参构造方法，供JAXB反序列化使用
   */
  public ContainerLogsInfoes() {
  }

  /**
   * 获取容器日志信息列表
   * @return 容器日志信息列表
   */
  public List<ContainerLogsInfo> getContainerLogsInfo() {
    return containerLogsInfo;
  }

  /**
   * 设置容器日志信息列表
   * @param containerLogsInfo 待设置的容器日志信息列表
   */
  public void setContainerLogsInfo(List<ContainerLogsInfo> containerLogsInfo) {
    this.containerLogsInfo = containerLogsInfo;
  }
}