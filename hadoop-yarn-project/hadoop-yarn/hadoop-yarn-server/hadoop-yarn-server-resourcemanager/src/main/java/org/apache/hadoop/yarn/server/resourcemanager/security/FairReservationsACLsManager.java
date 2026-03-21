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
package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;

/**
 * 基于公平调度器(FairScheduler)实现的预留资源访问权限管理器，
 * 负责管理公平调度器下各计划队列的预留操作权限。
 */
public class FairReservationsACLsManager extends ReservationsACLsManager {

  /**
   * 构造函数，从公平调度器配置中加载所有计划队列的预留权限配置。
   * @param scheduler 资源调度器实例，必须为FairScheduler
   * @param conf 配置对象
   * @throws YarnException 配置加载异常
   */
  public FairReservationsACLsManager(ResourceScheduler scheduler,
      Configuration conf) throws YarnException {
    super(conf);
    // 获取公平调度器的分配配置
    AllocationConfiguration aConf = ((FairScheduler) scheduler)
        .getAllocationConfiguration();
    // 遍历所有计划队列，加载每个队列的预留权限
    for (String planQueue : scheduler.getPlanQueues()) {
      reservationAcls.put(planQueue, aConf.getReservationAcls(new QueuePath(planQueue)));
    }
  }

}