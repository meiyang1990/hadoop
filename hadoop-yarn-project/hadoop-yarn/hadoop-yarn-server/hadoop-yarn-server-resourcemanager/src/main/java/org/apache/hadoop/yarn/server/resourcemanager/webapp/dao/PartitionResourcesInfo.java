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
 * 代表指定资源分区中队列或用户的资源使用信息，用于ResourceManager Web UI数据展示
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class PartitionResourcesInfo {
  // 资源分区名称
  private String partitionName;
  // 已使用资源信息
  private ResourceInfo used = new ResourceInfo();
  // 已预留资源信息
  private ResourceInfo reserved;
  // 等待分配资源信息
  private ResourceInfo pending;
  // ApplicationMaster已使用资源信息
  private ResourceInfo amUsed;
  // ApplicationMaster资源总限额信息
  private ResourceInfo amLimit = new ResourceInfo();
  // 单用户ApplicationMaster资源限额信息
  private ResourceInfo userAmLimit;

  /**
   * 默认无参构造函数，用于JAXB序列化反序列化
   */
  public PartitionResourcesInfo() {
  }

  /**
   * 全参数构造函数，创建指定分区资源信息对象
   * @param partitionName 资源分区名称
   * @param used 已使用资源
   * @param reserved 已预留资源
   * @param pending 待分配资源
   * @param amResourceUsed ApplicationMaster已使用资源
   * @param amResourceLimit ApplicationMaster总资源限额
   * @param perUserAmResourceLimit 单用户ApplicationMaster资源限额
   */
  public PartitionResourcesInfo(String partitionName, ResourceInfo used,
      ResourceInfo reserved, ResourceInfo pending,
      ResourceInfo amResourceUsed, ResourceInfo amResourceLimit,
      ResourceInfo perUserAmResourceLimit) {
    super();
    this.partitionName = partitionName;
    this.used = used;
    this.reserved = reserved;
    this.pending = pending;
    this.amUsed = amResourceUsed;
    this.amLimit = amResourceLimit;
    this.userAmLimit = perUserAmResourceLimit;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public void setPartitionName(String partitionName) {
    this.partitionName = partitionName;
  }

  public ResourceInfo getUsed() {
    return used;
  }

  public void setUsed(ResourceInfo used) {
    this.used = used;
  }

  public ResourceInfo getReserved() {
    return reserved;
  }

  public void setReserved(ResourceInfo reserved) {
    this.reserved = reserved;
  }

  public ResourceInfo getPending() {
    return pending;
  }

  public void setPending(ResourceInfo pending) {
    this.pending = pending;
  }

  public ResourceInfo getAmUsed() {
    return amUsed;
  }

  public void setAmUsed(ResourceInfo amResourceUsed) {
    this.amUsed = amResourceUsed;
  }

  public ResourceInfo getAMLimit() {
    return amLimit;
  }

  public void setAMLimit(ResourceInfo amLimit) {
    this.amLimit = amLimit;
  }

  /**
   * @return 单用户ApplicationMaster资源限额
   */
  public ResourceInfo getUserAmLimit() {
    return userAmLimit;
  }

  /**
   * @param userAmLimit 设置单用户ApplicationMaster资源限额
   */
  public void setUserAmLimit(ResourceInfo userAmLimit) {
    this.userAmLimit = userAmLimit;
  }
}