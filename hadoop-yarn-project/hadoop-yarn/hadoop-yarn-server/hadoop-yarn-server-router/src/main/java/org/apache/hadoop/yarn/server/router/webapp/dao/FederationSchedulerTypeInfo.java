// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.router.webapp.dao;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * YARN Router联邦场景下，聚合多个子集群调度器类型信息的DAO类.
 * 用于REST API返回序列化，继承RM原生SchedulerTypeInfo扩展支持多子集群场景
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FederationSchedulerTypeInfo extends SchedulerTypeInfo {
  // 存储所有子集群的调度器类型信息列表
  @XmlElement(name = "subCluster")
  private List<SchedulerTypeInfo> list = new ArrayList<>();

  /**
   * JAXB序列化需要的无参构造方法.
   */
  public FederationSchedulerTypeInfo() {
  } // JAXB needs this

  /**
   * 用传入的子集群调度器信息列表构造对象.
   * @param list 所有子集群调度器类型信息列表
   */
  public FederationSchedulerTypeInfo(ArrayList<SchedulerTypeInfo> list) {
    this.list = list;
  }

  /**
   * 获取所有子集群的调度器类型信息列表.
   * @return 子集群调度器信息列表
   */
  public List<SchedulerTypeInfo> getList() {
    return list;
  }

  /**
   * 设置子集群调度器类型信息列表.
   * @param list 子集群调度器信息列表
   */
  public void setList(List<SchedulerTypeInfo> list) {
    this.list = list;
  }
}