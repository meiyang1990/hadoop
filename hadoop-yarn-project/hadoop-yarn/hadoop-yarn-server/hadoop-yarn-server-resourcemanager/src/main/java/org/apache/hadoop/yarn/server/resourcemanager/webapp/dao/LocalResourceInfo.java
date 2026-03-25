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

import java.net.URI;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

/**
 * YARN RM Web API 本地资源信息数据访问对象
 * 用于封装应用提交的本地资源信息，支持XML/JSON序列化输出到Web界面
 */
@XmlRootElement(name = "localresources")
@XmlAccessorType(XmlAccessType.FIELD)
public class LocalResourceInfo {

  @XmlElement(name = "resource")
  URI url;
  LocalResourceType type;
  LocalResourceVisibility visibility;
  long size;
  long timestamp;
  String pattern;

  /** 获取资源访问URI */
  public URI getUrl() {
    return url;
  }

  /** 获取资源类型 */
  public LocalResourceType getType() {
    return type;
  }

  /** 获取资源可见性 */
  public LocalResourceVisibility getVisibility() {
    return visibility;
  }

  /** 获取资源大小（字节） */
  public long getSize() {
    return size;
  }

  /** 获取资源时间戳 */
  public long getTimestamp() {
    return timestamp;
  }

  /** 获取资源通配符匹配模式 */
  public String getPattern() {
    return pattern;
  }

  /** 设置资源访问URI */
  public void setUrl(URI url) {
    this.url = url;
  }

  /** 设置资源类型 */
  public void setType(LocalResourceType type) {
    this.type = type;
  }

  /** 设置资源可见性 */
  public void setVisibility(LocalResourceVisibility visibility) {
    this.visibility = visibility;
  }

  /** 
   * 设置资源大小
   * @param size 资源大小（字节），必须大于0
   */
  public void setSize(long size) {
    if (size <= 0) {
      throw new IllegalArgumentException("size must be greater than 0");
    }
    this.size = size;
  }

  /**
   * 设置资源时间戳
   * @param timestamp 资源时间戳，必须大于0
   */
  public void setTimestamp(long timestamp) {
    if (timestamp <= 0) {
      throw new IllegalArgumentException("timestamp must be greater than 0");
    }
    this.timestamp = timestamp;
  }

  /** 设置资源通配符匹配模式 */
  public void setPattern(String pattern) {
    this.pattern = pattern;
  }
}