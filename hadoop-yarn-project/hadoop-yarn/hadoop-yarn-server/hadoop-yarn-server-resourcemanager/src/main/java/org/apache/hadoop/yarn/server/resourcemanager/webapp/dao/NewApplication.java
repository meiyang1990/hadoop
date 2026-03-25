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
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN RM Web API 新建应用响应数据对象，封装新分配应用的基础信息
 */
@XmlRootElement(name="NewApplication")
@XmlAccessorType(XmlAccessType.FIELD)
public class NewApplication {

  @XmlElement(name="application-id")
  String applicationId;

  @XmlElement(name="maximum-resource-capability")
  ResourceInfo maximumResourceCapability;

  /**
   * 默认构造函数，初始化空应用ID和最大资源能力
   */
  public NewApplication() {
    applicationId = "";
    maximumResourceCapability = new ResourceInfo();
  }

  /**
   * 带参数构造函数，使用指定应用ID和最大资源能力创建对象
   * @param appId 新分配的应用ID
   * @param maxResources 应用可使用的最大资源能力
   */
  public NewApplication(String appId, ResourceInfo maxResources) {
    applicationId = appId;
    maximumResourceCapability = maxResources;
  }

  /**
   * 获取新建应用的应用ID
   * @return 应用ID字符串
   */
  public String getApplicationId() {
    return applicationId;
  }

  /**
   * 获取新建应用允许使用的最大资源能力
   * @return 资源能力信息对象
   */
  public ResourceInfo getMaximumResourceCapability() {
    return maximumResourceCapability;
  }

}