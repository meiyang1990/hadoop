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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.ApplicationAccessType;

/**
 * 容器启动上下文信息数据访问对象，用于接收通过REST API提交应用时传入的容器启动参数，
 * 构造应用提交上下文所需的ContainerLaunchContext对象
 */
@XmlRootElement(name = "container-launch-context-info")
@XmlAccessorType(XmlAccessType.FIELD)
public class ContainerLaunchContextInfo {

  @XmlElementWrapper(name = "local-resources")
  // 容器本地化资源映射表，键为资源名称，值为资源信息对象
  HashMap<String, LocalResourceInfo> local_resources;
  // 容器环境变量映射表
  HashMap<String, String> environment;

  @XmlElementWrapper(name = "commands")
  @XmlElement(name = "command", type = String.class)
  // 容器启动命令列表
  List<String> commands;

  @XmlElementWrapper(name = "service-data")
  // 辅助服务数据映射表
  HashMap<String, String> servicedata;

  @XmlElement(name = "credentials")
  // 容器凭证信息
  CredentialsInfo credentials;

  @XmlElementWrapper(name = "application-acls")
  // 应用访问控制权限映射表
  HashMap<ApplicationAccessType, String> acls;

  /**
   * 默认构造函数，初始化所有字段容器
   */
  public ContainerLaunchContextInfo() {
    local_resources = new HashMap<String, LocalResourceInfo>();
    environment = new HashMap<String, String>();
    commands = new ArrayList<String>();
    servicedata = new HashMap<String, String>();
    credentials = new CredentialsInfo();
    acls = new HashMap<ApplicationAccessType, String>();
  }

  /**
   * 获取本地化资源映射表
   * @return 本地化资源映射表
   */
  public Map<String, LocalResourceInfo> getResources() {
    return local_resources;
  }

  /**
   * 获取环境变量映射表
   * @return 环境变量映射表
   */
  public Map<String, String> getEnvironment() {
    return environment;
  }

  /**
   * 获取启动命令列表
   * @return 启动命令列表
   */
  public List<String> getCommands() {
    return commands;
  }

  /**
   * 获取辅助服务数据映射表
   * @return 辅助服务数据映射表
   */
  public Map<String, String> getAuxillaryServiceData() {
    return servicedata;
  }

  /**
   * 获取凭证信息
   * @return 凭证信息对象
   */
  public CredentialsInfo getCredentials() {
    return credentials;
  }

  /**
   * 获取应用访问控制权限映射表
   * @return 访问控制权限映射表
   */
  public Map<ApplicationAccessType, String> getAcls() {
    return acls;
  }

  /**
   * 设置本地化资源映射表
   * @param resources 本地化资源映射表
   */
  public void setResources(HashMap<String, LocalResourceInfo> resources) {
    this.local_resources = resources;
  }

  /**
   * 设置环境变量映射表
   * @param environment 环境变量映射表
   */
  public void setEnvironment(HashMap<String, String> environment) {
    this.environment = environment;
  }

  /**
   * 设置启动命令列表
   * @param commands 启动命令列表
   */
  public void setCommands(List<String> commands) {
    this.commands = commands;
  }

  /**
   * 设置辅助服务数据映射表
   * @param serviceData 辅助服务数据映射表
   */
  public void setAuxillaryServiceData(HashMap<String, String> serviceData) {
    this.servicedata = serviceData;
  }

  /**
   * 设置凭证信息
   * @param credentials 凭证信息对象
   */
  public void setCredentials(CredentialsInfo credentials) {
    this.credentials = credentials;
  }

  /**
   * 设置应用访问控制权限映射表
   * @param acls 访问控制权限映射表
   */
  public void setAcls(HashMap<ApplicationAccessType, String> acls) {
    this.acls = acls;
  }
}