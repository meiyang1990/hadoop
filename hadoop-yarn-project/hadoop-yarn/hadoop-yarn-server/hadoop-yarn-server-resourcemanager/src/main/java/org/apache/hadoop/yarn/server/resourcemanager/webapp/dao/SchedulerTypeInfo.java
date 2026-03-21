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
 * Unless required by applicable law or agreed to writing, software
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
 * YARN ResourceManager Web UI 调度器信息数据访问对象，
 * 用于封装调度器基本信息并支持XML/JSON序列化返回给前端。
 */
@XmlRootElement(name = "scheduler")
@XmlAccessorType(XmlAccessType.FIELD)
public class SchedulerTypeInfo {
  // 调度器详细信息对象
  private SchedulerInfo schedulerInfo;
  // 联邦场景下对应的子集群ID
  private String subClusterId;

  /**
   * 无参构造函数，供JAXB序列化框架使用。
   */
  public SchedulerTypeInfo() {
  } // JAXB needs this

  /**
   * 构造函数，使用已有调度器信息创建对象。
   * @param scheduler 调度器详细信息
   */
  public SchedulerTypeInfo(final SchedulerInfo scheduler) {
    this.schedulerInfo = scheduler;
  }

  /**
   * 获取调度器详细信息。
   * @return 调度器信息对象
   */
  public SchedulerInfo getSchedulerInfo() {
    return schedulerInfo;
  }

  /**
   * 获取子集群ID（联邦场景）。
   * @return 子集群ID
   */
  public String getSubClusterId() {
    return subClusterId;
  }

  /**
   * 设置子集群ID（联邦场景）。
   * @param subClusterId 子集群ID
   */
  public void setSubClusterId(String subClusterId) {
    this.subClusterId = subClusterId;
  }
}