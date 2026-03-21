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
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN RM Web API 应用超时配置信息集合数据访问对象，用于封装多个应用超时配置信息，供Web服务序列化输出。
 */
@XmlRootElement(name = "timeouts")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppTimeoutsInfo {

  @XmlElement(name = "timeout")
  private ArrayList<AppTimeoutInfo> timeouts = new ArrayList<AppTimeoutInfo>();

  /**
   * JAXB反序列化需要的无参构造方法。
   */
  public AppTimeoutsInfo() {
  } // JAXB needs this

  /**
   * 添加单个应用超时配置信息到集合中。
   * @param timeoutInfo 单个应用超时配置信息对象
   */
  public void add(AppTimeoutInfo timeoutInfo) {
    timeouts.add(timeoutInfo);
  }

  /**
   * 获取所有应用超时配置信息集合。
   * @return 所有应用超时配置信息列表
   */
  public ArrayList<AppTimeoutInfo> getAppTimeouts() {
    return timeouts;
  }
}