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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN Web REST API 应用列表信息数据访问对象，封装多个应用的基本信息，用于序列化返回给前端
 */
@Public
@Evolving
@XmlRootElement(name = "apps")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppsInfo {

  // 存储单个应用信息的列表
  protected ArrayList<AppInfo> app = new ArrayList<>();

  /**
   * JAXB反序列化需要的无参构造函数
   */
  public AppsInfo() {
    // JAXB needs this
  }

  /**
   * 添加单个应用信息到列表
   * @param appinfo 单个应用信息对象
   */
  public void add(AppInfo appinfo) {
    app.add(appinfo);
  }

  /**
   * 获取所有应用信息列表
   * @return 所有应用信息的ArrayList
   */
  public ArrayList<AppInfo> getApps() {
    return app;
  }

}