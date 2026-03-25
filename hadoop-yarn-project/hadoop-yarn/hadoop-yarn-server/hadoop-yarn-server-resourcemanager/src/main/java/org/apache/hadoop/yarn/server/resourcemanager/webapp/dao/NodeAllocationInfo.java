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

import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.NodeAllocation;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN RM Web UI 节点心跳分配信息数据访问对象，用于封装单次节点心跳的容器分配信息，
 * 供Web接口序列化返回前端展示分配活动详情。
 */
/*
 * DAO object to display each node allocation in node heartbeat.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeAllocationInfo {
  private String partition;
  private String updatedContainerId;
  private String finalAllocationState;
  private ActivityNodeInfo root = null;

  private static final Logger LOG =
      LoggerFactory.getLogger(NodeAllocationInfo.class);

  /**
   * 默认无参构造函数，供JAXB序列化使用。
   */
  NodeAllocationInfo() {
  }

  /**
   * 根据调度层节点分配信息构造Web DAO对象，转换分配活动树结构。
   * @param allocation 调度层节点分配原始信息
   * @param groupBy 活动分组方式
   */
  NodeAllocationInfo(NodeAllocation allocation,
      RMWSConsts.ActivitiesGroupBy groupBy) {
    this.partition = allocation.getPartition();
    this.updatedContainerId = allocation.getContainerId();
    this.finalAllocationState = allocation.getFinalAllocationState().name();
    root = new ActivityNodeInfo(allocation.getRoot(), groupBy);
  }

  public String getPartition() {
    return partition;
  }

  public String getUpdatedContainerId() {
    return updatedContainerId;
  }

  public String getFinalAllocationState() {
    return finalAllocationState;
  }

  public ActivityNodeInfo getRoot() {
    return root;
  }
}