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
 * 调度器配置版本信息数据访问对象，用于在RM Web UI中暴露调度器配置版本信息
 */
@XmlRootElement(name = "configversion")
@XmlAccessorType(XmlAccessType.FIELD)
public class ConfigVersionInfo {

  /** 配置版本号 */
  private long versionID;

  /**
   * JAXB要求的无参构造方法
   */
  public ConfigVersionInfo() {
  } // JAXB needs this

  /**
   * 构造配置版本信息对象
   * @param version 配置版本号
   */
  public ConfigVersionInfo(long version) {
    this.versionID = version;
  }

  /**
   * 获取配置版本号
   * @return 当前配置版本号
   */
  public long getVersionID() {
    return this.versionID;
  }

}