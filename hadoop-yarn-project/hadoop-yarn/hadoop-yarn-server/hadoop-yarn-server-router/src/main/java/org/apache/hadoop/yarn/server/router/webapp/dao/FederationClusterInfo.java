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

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * 联邦集群信息数据访问对象，用于Router聚合多个子集群信息，提供REST API返回联邦整体集群信息。
 * 继承自单集群ClusterInfo，添加了所有子集群的信息列表。
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FederationClusterInfo extends ClusterInfo {

  // 存储所有子集群的基础集群信息列表
  @XmlElement(name = "subCluster")
  private List<ClusterInfo> list = new ArrayList<>();

  /**
   * 默认无参构造函数，供JAXB序列化/反序列化使用。
   */
  public FederationClusterInfo() {
  } // JAXB needs this

  /**
   * 构造方法，使用指定子集群列表初始化联邦集群信息。
   * @param list 所有子集群的信息列表
   */
  public FederationClusterInfo(ArrayList<ClusterInfo> list) {
    this.list = list;
  }

  /**
   * 获取所有子集群的信息列表。
   * @return 子集群信息列表
   */
  public List<ClusterInfo> getList() {
    return list;
  }

  /**
   * 设置子集群信息列表。
   * @param list 待设置的子集群信息列表
   */
  public void setList(List<ClusterInfo> list) {
    this.list = list;
  }
}