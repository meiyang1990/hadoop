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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN ResourceManager Web DAO类，封装多个应用信息，用于REST接口返回应用列表数据
 */
@XmlRootElement(name = "apps")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppsInfo {

  private ArrayList<AppInfo> app = new ArrayList<>();

  /**
   * 默认无参构造函数，供JAXB序列化/反序列化使用
   */
  public AppsInfo() {
  } // JAXB needs this

  /**
   * 添加单个应用信息到应用列表
   * @param appinfo 要添加的单个应用信息对象
   */
  public void add(AppInfo appinfo) {
    app.add(appinfo);
  }

  /**
   * 获取所有应用信息列表
   * @return 存储所有应用信息的ArrayList
   */
  public ArrayList<AppInfo> getApps() {
    return app;
  }

  /**
   * 批量添加多个应用信息到应用列表
   * @param appsInfo 要添加的应用信息列表
   */
  public void addAll(ArrayList<AppInfo> appsInfo) {
    app.addAll(appsInfo);
  }

}