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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;

/**
 * NodeManager存活状态监控器，继承抽象活跃性监控框架，定期检查NodeManager心跳是否超时
 * 若超时则触发节点过期事件，通知ResourceManager处理异常节点
 */
public class NMLivelinessMonitor extends AbstractLivelinessMonitor<NodeId> {

  private EventHandler<Event> dispatcher;
  
  /**
   * 构造NMLivelinessMonitor实例，注入事件分发器
   * @param d 事件分发器，用于发送节点过期事件
   */
  public NMLivelinessMonitor(Dispatcher d) {
    super("NMLivelinessMonitor");
    this.dispatcher = d.getEventHandler();
  }

  /**
   * 服务初始化，从配置读取节点过期间隔，设置监控参数
   * @param conf YARN配置对象
   * @throws Exception 初始化异常
   */
  public void serviceInit(Configuration conf) throws Exception {
    // 读取NodeManager过期超时时间配置，使用默认值兜底
    int expireIntvl = conf.getInt(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);
    // 设置过期间隔
    setExpireInterval(expireIntvl);
    // 设置监控检查间隔为过期间隔的1/3，符合常规心跳检查策略
    setMonitorInterval(expireIntvl/3);
    super.serviceInit(conf);
  }

  /**
   * 处理已过期的NodeManager，发送节点过期事件到事件系统
   * @param id 过期NodeManager的节点ID
   */
  @Override
  protected void expire(NodeId id) {
    dispatcher.handle(
        new RMNodeEvent(id, RMNodeEventType.EXPIRE)); 
  }
}