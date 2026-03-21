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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * NodeManager Web UI 应用列表信息数据访问对象，用于封装当前节点上所有运行应用的信息，支持XML/JSON序列化输出。
 */
@XmlRootElement(name = "apps")
@XmlAccessorType(XmlAccessType.FIELD)
@SuppressWarnings("checkstyle:VisibilityModifier")
public class AppsInfo {

  /** 存储单个应用信息的列表 */
  protected ArrayList<AppInfo> app = new ArrayList<>();

  /** 无参构造器，供JAXB序列化框架使用 */
  public AppsInfo() {
  } // JAXB needs this

  /**
   * 添加一个应用信息到列表中
   * @param appInfo 单个应用信息对象
   */
  public void add(AppInfo appInfo) {
    app.add(appInfo);
  }

  /**
   * 获取所有应用信息列表
   * @return 当前节点所有应用信息集合
   */
  public ArrayList<AppInfo> getApps() {
    return app;
  }

}