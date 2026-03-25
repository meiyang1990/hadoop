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
 * 队列容量向量单个条目信息，用于YARN ResourceManager Web UI展示队列资源容量信息
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class QueueCapacityVectorEntryInfo {
  private String resourceName;
  private String resourceValue;

  /**
   * 默认无参构造函数，供JAXB序列化使用
   */
  public QueueCapacityVectorEntryInfo() {
  }

  /**
   * 构造函数，创建队列容量向量条目
   * @param resourceName 资源名称
   * @param resourceValue 资源容量值
   */
  public QueueCapacityVectorEntryInfo(String resourceName, String resourceValue) {
    this.resourceName = resourceName;
    this.resourceValue = resourceValue;
  }

  /**
   * 获取资源名称
   * @return 资源名称
   */
  public String getResourceName() {
    return this.resourceName;
  }

  /**
   * 设置资源名称
   * @param resourceName 资源名称
   */
  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  /**
   * 获取资源容量值
   * @return 资源容量值
   */
  public String getResourceValue() {
    return this.resourceValue;
  }

  /**
   * 设置资源容量值
   * @param resourceValue 资源容量值
   */
  public void setResourceValue(String resourceValue) {
    this.resourceValue = resourceValue;
  }
}