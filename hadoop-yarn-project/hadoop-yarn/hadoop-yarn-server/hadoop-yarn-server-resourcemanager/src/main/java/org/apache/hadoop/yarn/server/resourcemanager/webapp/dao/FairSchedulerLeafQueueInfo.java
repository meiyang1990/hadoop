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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;

/**
 * 公平调度器叶子队列信息DAO，为Web UI提供叶子队列统计数据
 * 叶子队列是公平调度队列层次中实际运行应用的最底层队列
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FairSchedulerLeafQueueInfo extends FairSchedulerQueueInfo {
  /** 待运行应用数量 */
  private int numPendingApps;
  /** 活跃运行应用数量 */
  private int numActiveApps;
  
  /**
   * 默认构造函数，供JAXB反序列化使用
   */
  public FairSchedulerLeafQueueInfo() {
  }
  
  /**
   * 从实际叶子队列构造数据访问对象，提取队列统计信息
   * @param queue 实际调度叶子队列
   * @param scheduler 公平调度器实例
   */
  public FairSchedulerLeafQueueInfo(FSLeafQueue queue, FairScheduler scheduler) {
    super(queue, scheduler);
    numPendingApps = queue.getNumPendingApps();
    numActiveApps = queue.getNumActiveApps();
  }
  
  /**
   * 获取活跃运行应用数量
   * @return 活跃应用数
   */
  public int getNumActiveApplications() {
    return numActiveApps;
  }
  
  /**
   * 获取待运行应用数量
   * @return 待运行应用数
   */
  public int getNumPendingApplications() {
    return numPendingApps;
  }
}