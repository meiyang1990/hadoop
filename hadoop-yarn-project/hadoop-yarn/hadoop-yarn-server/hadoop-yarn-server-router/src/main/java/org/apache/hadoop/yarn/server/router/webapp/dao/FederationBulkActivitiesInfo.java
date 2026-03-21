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

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.BulkActivitiesInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;

/**
 * YARN联邦路由层批量活动信息数据访问对象，聚合多个子集群的批量活动信息。
 * 用于REST API响应序列化，承载跨子集群的批量操作结果汇总。
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FederationBulkActivitiesInfo extends BulkActivitiesInfo {

  /** 存储各子集群的批量活动信息列表 */
  @XmlElement(name = "subCluster")
  private ArrayList<BulkActivitiesInfo> list = new ArrayList<>();

  /** JAXB反序列化需要的无参构造函数 */
  public FederationBulkActivitiesInfo() {
  } // JAXB needs this

  /**
   * 构造函数，通过子集群批量活动信息列表初始化对象
   * @param list 各子集群批量活动信息列表
   */
  public FederationBulkActivitiesInfo(ArrayList<BulkActivitiesInfo> list) {
    this.list = list;
  }

  /**
   * 获取子集群批量活动信息列表
   * @return 各子集群批量活动信息列表
   */
  public ArrayList<BulkActivitiesInfo> getList() {
    return list;
  }

  /**
   * 设置子集群批量活动信息列表
   * @param list 各子集群批量活动信息列表
   */
  public void setList(ArrayList<BulkActivitiesInfo> list) {
    this.list = list;
  }
}