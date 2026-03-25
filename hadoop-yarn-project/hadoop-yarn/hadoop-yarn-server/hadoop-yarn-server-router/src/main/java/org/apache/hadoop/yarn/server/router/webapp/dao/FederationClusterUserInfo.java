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

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterUserInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * 联邦集群用户信息数据访问对象，聚合多个子集群的用户信息，
 * 用于Router联邦模式下Web UI展示全集群维度的用户资源统计信息。
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FederationClusterUserInfo extends ClusterUserInfo {
  // 所有子集群的用户信息列表
  @XmlElement(name = "subCluster")
  private List<ClusterUserInfo> list = new ArrayList<>();

  /**
   * JAXB反序列化需要的无参构造方法。
   */
  public FederationClusterUserInfo() {
  } // JAXB needs this

  /**
   * 构造方法，使用指定子集群用户信息列表初始化联邦用户信息。
   * @param list 子集群用户信息列表
   */
  public FederationClusterUserInfo(ArrayList<ClusterUserInfo> list) {
    this.list = list;
  }

  /**
   * 获取所有子集群的用户信息列表。
   * @return 子集群用户信息列表
   */
  public List<ClusterUserInfo> getList() {
    return list;
  }

  /**
   * 设置子集群用户信息列表。
   * @param list 子集群用户信息列表
   */
  public void setList(List<ClusterUserInfo> list) {
    this.list = list;
  }
}