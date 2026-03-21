// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by joblicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 应用程序所有尝试信息的数据访问对象，封装YARN RM WebUI所需的应用尝试列表信息
 */
@XmlRootElement(name = "appAttempts")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppAttemptsInfo {

  @XmlElement(name = "appAttempt")
  protected ArrayList<AppAttemptInfo> attempt = new ArrayList<AppAttemptInfo>();

  /**
   * 默认无参构造函数，供JAXB序列化/反序列化使用
   */
  public AppAttemptsInfo() {
  } // JAXB needs this

  /**
   * 添加单个应用尝试信息到列表
   * @param info 单个应用尝试信息对象
   */
  public void add(AppAttemptInfo info) {
    this.attempt.add(info);
  }

  /**
   * 获取所有应用尝试信息列表
   * @return 所有应用尝试信息集合
   */
  public ArrayList<AppAttemptInfo> getAttempts() {
    return this.attempt;
  }

}