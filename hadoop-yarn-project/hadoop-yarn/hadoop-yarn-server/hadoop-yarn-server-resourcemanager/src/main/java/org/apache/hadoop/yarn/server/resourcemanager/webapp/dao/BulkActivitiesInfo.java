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
import java.util.List;
import java.util.ArrayList;

/**
 * YARN RM WebAPI 批量分配活动信息数据访问对象，用于封装批量活动信息并序列化为XML/JSON返回前端
 */
@XmlRootElement(name = "bulkActivities")
@XmlAccessorType(XmlAccessType.FIELD)
public class BulkActivitiesInfo {

  // 存储所有活动信息列表
  private ArrayList<ActivitiesInfo> activities = new ArrayList<>();

  // 关联的子集群ID（联邦场景使用）
  private String subClusterId;

  /**
   * JAXB反序列化需要的无参构造函数
   */
  public BulkActivitiesInfo() {
    // JAXB needs this
  }

  /**
   * 添加单个活动信息到批量列表
   * @param activitiesInfo 待添加的活动信息对象
   */
  public void add(ActivitiesInfo activitiesInfo) {
    activities.add(activitiesInfo);
  }

  /**
   * 获取所有活动信息列表
   * @return 活动信息列表
   */
  public ArrayList<ActivitiesInfo> getActivities() {
    return activities;
  }

  /**
   * 批量添加多个活动信息到列表
   * @param activitiesInfoList 待添加的活动信息列表
   */
  public void addAll(List<ActivitiesInfo> activitiesInfoList) {
    activities.addAll(activitiesInfoList);
  }

  /**
   * 获取关联的子集群ID
   * @return 子集群ID
   */
  public String getSubClusterId() {
    return subClusterId;
  }

  /**
   * 设置关联的子集群ID
   * @param subClusterId 子集群ID
   */
  public void setSubClusterId(String subClusterId) {
    this.subClusterId = subClusterId;
  }
}