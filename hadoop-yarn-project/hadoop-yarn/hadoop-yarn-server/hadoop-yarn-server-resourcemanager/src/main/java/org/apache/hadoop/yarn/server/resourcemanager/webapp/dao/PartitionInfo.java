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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN RM Web API 分区资源信息数据访问对象，用于序列化输出分区资源信息
 */
@XmlRootElement(name = "partitionInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class PartitionInfo {

  @XmlElement(name = "resourceAvailable")
  private ResourceInfo resourceAvailable;

  public PartitionInfo() {
  }

  public PartitionInfo(ResourceInfo resourceAvailable) {
    this.resourceAvailable = resourceAvailable;
  }

  public ResourceInfo getResourceAvailable() {
    return resourceAvailable;
  }

  /**
   * 合并两个分区资源信息，返回新的合并后的分区资源对象
   *
   * @param left 第一个待合并的分区信息对象
   * @param right 第二个待合并的分区信息对象
   * @return 合并后的新分区信息对象
   */
  public static PartitionInfo addTo(PartitionInfo left, PartitionInfo right) {
    // 初始化左分区资源，默认0值
    Resource leftResource = Resource.newInstance(0, 0);
    // 如果左分区有效，提取其可用资源
    if (left != null && left.getResourceAvailable() != null) {
      ResourceInfo leftResourceInfo = left.getResourceAvailable();
      leftResource = leftResourceInfo.getResource();
    }

    // 初始化右分区资源，默认0值
    Resource rightResource = Resource.newInstance(0, 0);
    // 如果右分区有效，提取其可用资源
    if (right != null && right.getResourceAvailable() != null) {
      ResourceInfo rightResourceInfo = right.getResourceAvailable();
      rightResource = rightResourceInfo.getResource();
    }

    // 累加两个分区的可用资源
    Resource resource = Resources.addTo(leftResource, rightResource);
    // 封装为新分区信息对象返回
    return new PartitionInfo(new ResourceInfo(resource));
  }
}