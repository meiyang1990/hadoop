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
 * Unless required by applicable joblicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.webapp.dao;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * YARN Web REST API 应用尝试列表数据访问对象，封装应用所有尝试的信息，用于序列化返回给前端
 */
@Public
@Evolving
@XmlRootElement(name = "appAttempts")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppAttemptsInfo {

  // XML/JSON序列化对应的应用尝试列表
  @XmlElement(name = "appAttempt")
  protected ArrayList<AppAttemptInfo> attempt = new ArrayList<AppAttemptInfo>();

  /**
   * JAXB反序列化需要的无参构造函数
   */
  public AppAttemptsInfo() {
    // JAXB needs this
  }

  /**
   * 添加单个应用尝试信息到列表
   * @param info 单个应用尝试信息
   */
  public void add(AppAttemptInfo info) {
    this.attempt.add(info);
  }

  /**
   * 获取所有应用尝试信息列表
   * @return 应用尝试信息列表
   */
  public ArrayList<AppAttemptInfo> getAttempts() {
    return this.attempt;
  }

}