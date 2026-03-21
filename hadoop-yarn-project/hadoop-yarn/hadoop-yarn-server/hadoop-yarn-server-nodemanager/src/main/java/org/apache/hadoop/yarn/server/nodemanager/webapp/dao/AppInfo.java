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

import java.util.ArrayList;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * NodeManager Web界面应用信息数据访问对象，封装当前Node节点上运行的应用信息，用于JSON/XML序列化返回给前端。
 */
@XmlRootElement(name = "app")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppInfo {

  protected String id;
  protected String state;
  protected String user;
  protected ArrayList<String> containerids;

  /**
   * 无参构造函数，供JAXB序列化框架使用。
   */
  public AppInfo() {
  } // JAXB needs this

  /**
   * 根据NM端的Application对象构造AppInfo，提取并转换需要展示给Web界面的信息。
   * @param app NM端的应用对象
   */
  public AppInfo(final Application app) {
    this.id = app.getAppId().toString();
    this.state = app.getApplicationState().toString();
    this.user = app.getUser();

    this.containerids = new ArrayList<String>();
    Map<ContainerId, Container> appContainers = app.getContainers();
    // 遍历所有容器，将容器ID转为字符串存储
    for (ContainerId containerId : appContainers.keySet()) {
      String containerIdStr = containerId.toString();
      containerids.add(containerIdStr);
    }
  }

  public String getId() {
    return this.id;
  }

  public String getUser() {
    return this.user;
  }

  public String getState() {
    return this.state;
  }

  public ArrayList<String> getContainers() {
    return this.containerids;
  }

}