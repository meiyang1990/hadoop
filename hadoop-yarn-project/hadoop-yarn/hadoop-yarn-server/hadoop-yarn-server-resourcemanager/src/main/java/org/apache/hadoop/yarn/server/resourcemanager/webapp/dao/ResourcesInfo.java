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

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;

/**
 * YARN RM Web UI 资源信息数据访问对象，封装队列/用户按分区划分的资源使用信息
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ResourcesInfo {
  // 按节点标签分区划分的资源使用信息列表
  List<PartitionResourcesInfo> resourceUsagesByPartition =
      new ArrayList<>();

  public ResourcesInfo() {
  }

  /**
   * 从调度器资源使用信息构造ResourcesInfo对象
   * @param resourceUsage 调度器原生资源使用信息
   * @param considerAMUsage 是否需要统计ApplicationMaster资源使用信息
   */
  public ResourcesInfo(ResourceUsage resourceUsage,
      boolean considerAMUsage) {
    if (resourceUsage == null) {
      return;
    }
    // 遍历所有存在的节点标签分区，逐个构造分区资源信息
    for (String partitionName : resourceUsage.getExistingNodeLabels()) {
      resourceUsagesByPartition.add(new PartitionResourcesInfo(partitionName,
          new ResourceInfo(resourceUsage.getUsed(partitionName)),
          new ResourceInfo(resourceUsage.getReserved(partitionName)),
          new ResourceInfo(resourceUsage.getPending(partitionName)),
          considerAMUsage ? new ResourceInfo(resourceUsage
              .getAMUsed(partitionName)) : null,
          considerAMUsage ? new ResourceInfo(resourceUsage
              .getAMLimit(partitionName)) : null,
          considerAMUsage ? new ResourceInfo(resourceUsage
              .getUserAMLimit(partitionName)) : null));
    }
  }

  /**
   * 默认构造，默认统计ApplicationMaster资源使用信息
   * @param resourceUsage 调度器原生资源使用信息
   */
  public ResourcesInfo(ResourceUsage resourceUsage) {
    this(resourceUsage, true);
  }

  public List<PartitionResourcesInfo> getPartitionResourceUsages() {
    return resourceUsagesByPartition;
  }

  public void setPartitionResourceUsages(
      List<PartitionResourcesInfo> resources) {
    this.resourceUsagesByPartition = resources;
  }

  /**
   * 根据分区名称获取对应分区的资源使用信息
   * @param partitionName 分区（节点标签）名称
   * @return 对应分区资源信息，未找到返回空对象
   */
  public PartitionResourcesInfo getPartitionResourceUsageInfo(
      String partitionName) {
    for (PartitionResourcesInfo partitionResourceUsageInfo :
      resourceUsagesByPartition) {
      if (partitionResourceUsageInfo.getPartitionName().equals(partitionName)) {
        return partitionResourceUsageInfo;
      }
    }
    return new PartitionResourcesInfo();
  }
}