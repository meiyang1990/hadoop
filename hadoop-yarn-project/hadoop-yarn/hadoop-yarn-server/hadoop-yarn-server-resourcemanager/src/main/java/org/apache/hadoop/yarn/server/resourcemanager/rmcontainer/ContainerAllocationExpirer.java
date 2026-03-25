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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;

/**
 * 容器分配超时检查器，负责检测分配给NM但未被及时认领的容器，
 * 超时后触发过期事件回收容器资源，继承自AbstractLivelinessMonitor实现周期性检查
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class ContainerAllocationExpirer extends
    AbstractLivelinessMonitor<AllocationExpirationInfo> {

  // 事件分发处理器，用于投递容器过期事件
  private EventHandler dispatcher;

  /**
   * 构造容器分配超时检查器
   * @param d 事件分发器
   */
  public ContainerAllocationExpirer(Dispatcher d) {
    super(ContainerAllocationExpirer.class.getName());
    this.dispatcher = d.getEventHandler();
  }

  /**
   * 服务初始化，从配置读取超时间隔并设置检查参数
   * @param conf YARN配置对象
   * @throws Exception 初始化异常
   */
  public void serviceInit(Configuration conf) throws Exception {
    // 从配置中读取容器分配超时时间，使用默认值作为 fallback
    int expireIntvl = conf.getInt(
            YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS);
    // 设置容器分配超时间隔
    setExpireInterval(expireIntvl);
    // 设置监控线程检查间隔，为超时间隔的1/3
    setMonitorInterval(expireIntvl/3);
    super.serviceInit(conf);
  }

  /**
   * 处理过期的容器分配信息，向调度器发送容器过期事件
   * @param allocationExpirationInfo 过期的容器分配信息
   */
  @Override
  protected void expire(AllocationExpirationInfo allocationExpirationInfo) {
    dispatcher.handle(new ContainerExpiredSchedulerEvent(
        allocationExpirationInfo.getContainerId(),
            allocationExpirationInfo.isIncrease()));
  }
}