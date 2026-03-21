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
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.ResourceInformation;

/**
 * YARN RM Web API 资源信息列表封装类
 * 用于将多个资源信息对象序列化为XML/JSON格式返回给前端
 */
@XmlRootElement(name = "resourceInformations")
@XmlAccessorType(XmlAccessType.FIELD)
public class ResourceInformationsInfo {

  // 存储单个资源信息列表
  @XmlElement(name = "resourceInformation")
  protected ArrayList<ResourceInformation> resourceInformation =
      new ArrayList<ResourceInformation>();

  /**
   * 默认无参构造函数，供JAXB序列化使用
   */
  public ResourceInformationsInfo() {
  } // JAXB needs this

  /**
   * 获取所有资源信息列表
   * @return 资源信息列表
   */
  public ArrayList<ResourceInformation> getApps() {
    return resourceInformation;
  }

  /**
   * 批量添加资源信息
   * @param resourcesInformationsInfo 待添加的资源信息列表
   */
  public void addAll(List<ResourceInformation> resourcesInformationsInfo) {
    resourceInformation.addAll(resourcesInformationsInfo);
  }
}