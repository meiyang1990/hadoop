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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN RM Web API 应用状态信息数据访问对象，用于序列化返回应用状态和诊断信息。
 */
@XmlRootElement(name = "appstate")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppState {

  String state;
  private String diagnostics;

  /**
   * JAXB要求的无参构造函数，用于反序列化XML/JSON对象。
   */
  public AppState() {
  }

  /**
   * 构造仅包含应用状态的AppState对象。
   * @param state 应用当前状态
   */
  public AppState(String state) {
    this.state = state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getState() {
    return this.state;
  }

  public String getDiagnostics() {
    return diagnostics;
  }

  public void setDiagnostics(String diagnostics) {
    this.diagnostics = diagnostics;
  }
}