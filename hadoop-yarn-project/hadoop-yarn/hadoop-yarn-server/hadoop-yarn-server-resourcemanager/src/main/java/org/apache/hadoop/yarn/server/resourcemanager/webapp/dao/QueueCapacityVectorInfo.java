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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.util.ArrayList;
import java.util.List;

/**
 * YARN RM Web DAO，封装队列多资源容量向量信息，供Web API序列化返回
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class QueueCapacityVectorInfo {
  // 容量向量的字符串表示
  private String configuredCapacityVector;
  // 按资源拆分的容量向量条目列表
  private List<QueueCapacityVectorEntryInfo> capacityVectorEntries;

  /**
   * 默认无参构造函数，用于JAXB序列化
   */
  public QueueCapacityVectorInfo() {
  }

  /**
   * 从调度器端的容量向量对象构造Web DAO对象
   * @param queueCapacityVector 调度器端原始容量向量对象
   */
  public QueueCapacityVectorInfo(QueueCapacityVector queueCapacityVector) {
    this.configuredCapacityVector = queueCapacityVector.toString();
    this.capacityVectorEntries = new ArrayList<>();
    // 遍历原始容量向量条目，转换为Web DAO条目
    for (QueueCapacityVector.QueueCapacityVectorEntry
            queueCapacityVectorEntry : queueCapacityVector) {
      this.capacityVectorEntries.add(
              new QueueCapacityVectorEntryInfo(queueCapacityVectorEntry.getResourceName(),
                    queueCapacityVectorEntry.getResourceWithPostfix()));
    }
  }

  public String getConfiguredCapacityVector() {
    return configuredCapacityVector;
  }

  public void setConfiguredCapacityVector(String configuredCapacityVector) {
    this.configuredCapacityVector = configuredCapacityVector;
  }

  public List<QueueCapacityVectorEntryInfo> getCapacityVectorEntries() {
    return capacityVectorEntries;
  }

  public void setCapacityVectorEntries(List<QueueCapacityVectorEntryInfo> capacityVectorEntries) {
    this.capacityVectorEntries = capacityVectorEntries;
  }
}