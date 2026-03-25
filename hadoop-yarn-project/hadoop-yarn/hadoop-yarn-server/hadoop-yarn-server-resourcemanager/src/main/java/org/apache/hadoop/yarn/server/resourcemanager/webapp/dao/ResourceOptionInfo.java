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

import org.apache.hadoop.yarn.api.records.ResourceOption;

/**
 * ResourceOption的数据访问对象，用于YARN ResourceManager Web UI中资源选项信息的序列化传输。
 * 将{@link ResourceOption}封装为JAXB可识别的格式，供Web接口返回XML/JSON数据。
 */
@XmlRootElement(name = "resourceOption")
@XmlAccessorType(XmlAccessType.NONE)
public class ResourceOptionInfo {

  @XmlElement(name = "resource")
  private ResourceInfo resource = new ResourceInfo();
  @XmlElement(name = "overCommitTimeout")
  private int overCommitTimeout;

  /** 缓存原始ResourceOption对象，避免重复构建 */
  private ResourceOption resourceOption;


  public ResourceOptionInfo() {
  } // JAXB needs this

  /**
   * 从ResourceOption构造ResourceOptionInfo对象，转换为Web DAO格式。
   * @param resourceOption 原始资源选项对象
   */
  public ResourceOptionInfo(ResourceOption resourceOption) {
    if (resourceOption != null) {
      this.resource = new ResourceInfo(resourceOption.getResource());
      this.overCommitTimeout = resourceOption.getOverCommitTimeout();
    }
  }

  /**
   * 获取或懒构建原始ResourceOption对象。
   * @return 转换后的原始ResourceOption实例
   */
  public ResourceOption getResourceOption() {
    if (resourceOption == null) {
      resourceOption = ResourceOption.newInstance(
          resource.getResource(), overCommitTimeout);
    }
    return resourceOption;
  }

  @Override
  public String toString() {
    return getResourceOption().toString();
  }
}