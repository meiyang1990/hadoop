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

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.RMQueueAclInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * 联邦YARN环境下多子集群队列ACL信息DTO，聚合多个子集群的队列权限信息，供Router Web服务返回前端使用。
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FederationRMQueueAclInfo extends RMQueueAclInfo {

  // 存储各子集群的队列ACL信息列表
  @XmlElement(name = "subCluster")
  private List<RMQueueAclInfo> list = new ArrayList<>();

  /**
   * JAXB反序列化需要的无参构造函数。
   */
  public FederationRMQueueAclInfo() {
  } // JAXB needs this

  /**
   * 构造函数，使用传入的子集群队列ACL列表初始化对象。
   * @param list 各子集群队列ACL信息列表
   */
  public FederationRMQueueAclInfo(ArrayList<RMQueueAclInfo> list) {
    this.list = list;
  }

  /**
   * 获取所有子集群的队列ACL信息列表。
   * @return 子集群队列ACL信息列表
   */
  public List<RMQueueAclInfo> getList() {
    return list;
  }

  /**
   * 设置子集群队列ACL信息列表。
   * @param list 子集群队列ACL信息列表
   */
  public void setList(List<RMQueueAclInfo> list) {
    this.list = list;
  }
}