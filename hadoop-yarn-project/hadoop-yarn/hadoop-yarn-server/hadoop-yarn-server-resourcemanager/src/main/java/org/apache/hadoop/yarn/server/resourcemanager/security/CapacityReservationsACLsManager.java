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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

/**
 * 文件: CapacityReservationsACLsManager.java
 * 所属模块: YARN 服务端 - 资源调度器 - 容量调度器安全模块
 * 核心职责: 基于容量调度器(CapacityScheduler)实现预约容量访问控制列表(ACL)管理器，
 *          负责管理容量调度器中所有计划队列的预约操作权限，校验用户是否有权提交预约请求。
 * 实现了 {@link ReservationsACLsManager} 基于 {@link CapacityScheduler} 的具体扩展。
 */
public class CapacityReservationsACLsManager extends ReservationsACLsManager {

  /**
   * 构造函数，初始化容量调度器预约权限ACL管理器
   * @param scheduler 资源调度器实例
   * @param conf 配置对象
   * @throws YarnException 初始化异常
   */
  public CapacityReservationsACLsManager(ResourceScheduler scheduler,
      Configuration conf) throws YarnException {
    super(conf);
    // 包装容量调度器专用配置
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(
        conf);

    // 遍历所有计划队列，加载每个队列的预约ACL配置
    for (String planQueue : scheduler.getPlanQueues()) {
      // 获取队列对象
      CSQueue queue = ((CapacityScheduler) scheduler).getQueue(planQueue);
      // 从配置中读取队列预约ACL，存入权限映射表
      reservationAcls.put(planQueue,
          csConf.getReservationAcls(queue.getQueuePathObject()));
    }
  }

}