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
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;

/**
 * 公平调度器信息数据访问对象，为Web UI提供公平调度器的整体状态信息
 */
@XmlRootElement(name = "fairScheduler")
@XmlType(name = "fairScheduler")
@XmlAccessorType(XmlAccessType.FIELD)
public class FairSchedulerInfo extends SchedulerInfo {
  /** 无效公平份额标识，表示查询不到对应应用尝试 */
  public static final int INVALID_FAIR_SHARE = -1;
  /** 根队列信息对象 */
  private FairSchedulerQueueInfo rootQueue;
  
  @XmlTransient
  /** 底层公平调度器实例，不参与XML序列化 */
  private FairScheduler scheduler;
  
  /** JAXB默认构造函数，用于序列化反序列化 */
  public FairSchedulerInfo() {
  } // JAXB needs this
  
  /**
   * 构造公平调度器信息对象，从调度器实例提取信息
   * @param fs 公平调度器实例
   */
  public FairSchedulerInfo(FairScheduler fs) {
    scheduler = fs;
    // 构建根队列信息，从调度器队列管理器获取根队列
    rootQueue = new FairSchedulerQueueInfo(scheduler.getQueueManager().
        getRootQueue(), scheduler);
    schedulerName = "Fair Scheduler";
  }

  /**
   * Get the fair share assigned to the appAttemptId.
   * @param appAttemptId the application attempt id
   * @return The fair share assigned to the appAttemptId,
   * <code>FairSchedulerInfo#INVALID_FAIR_SHARE</code> if the scheduler does
   * not know about this application attempt.
   */
  /** 获取指定应用尝试的内存公平份额 */
  public long getAppFairShare(ApplicationAttemptId appAttemptId) {
    FSAppAttempt fsAppAttempt = scheduler.getSchedulerApp(appAttemptId);
    // 查询不到应用尝试返回无效标识，否则返回内存公平份额大小
    return fsAppAttempt == null ?
        INVALID_FAIR_SHARE :  fsAppAttempt.getFairShare().getMemorySize();
  }
  
  /** 获取根队列信息对象 */
  public FairSchedulerQueueInfo getRootQueueInfo() {
    return rootQueue;
  }
}