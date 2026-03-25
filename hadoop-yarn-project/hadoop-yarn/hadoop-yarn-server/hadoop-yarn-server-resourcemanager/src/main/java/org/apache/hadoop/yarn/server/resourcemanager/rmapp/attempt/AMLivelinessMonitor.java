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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.Clock;

import java.util.concurrent.TimeUnit;

/**
 * ApplicationMaster 活性监控器，负责检测 YARN 中应用尝试实例的 AM 是否存活
 * 超过监控间隔未收到心跳则判定AM已失联，触发过期处理流程
 */
public class AMLivelinessMonitor extends AbstractLivelinessMonitor<ApplicationAttemptId> {

  private EventHandler<Event> dispatcher;
  
  /**
   * 构造 AM 活性监控器
   * @param d 事件分发器
   */
  public AMLivelinessMonitor(Dispatcher d) {
    super("AMLivelinessMonitor");
    this.dispatcher = d.getEventHandler();
  }

  /**
   * 构造带自定义时钟的 AM 活性监控器
   * @param d 事件分发器
   * @param clock 时钟对象，用于计时监控
   */
  public AMLivelinessMonitor(Dispatcher d, Clock clock) {
    super("AMLivelinessMonitor", clock);
    this.dispatcher = d.getEventHandler();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    long expireIntvl;
    // 从配置读取AM过期间隔配置字符串
    String rmAmExpiryIntervalMS = conf.get(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS);
    // 判断配置是否为纯数字（兼容性处理：旧版本配置仅支持纯数字毫秒）
    if (NumberUtils.isDigits(rmAmExpiryIntervalMS)) {
      expireIntvl = conf.getLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS);
    } else {
      // 带时间单位的配置，解析转换为毫秒
      expireIntvl = conf.getTimeDuration(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
    // 设置过期时间间隔
    setExpireInterval(expireIntvl);
    // 设置监控检查间隔为过期间隔的1/3
    setMonitorInterval(expireIntvl/3);
  }

  /**
   * 处理已过期的ApplicationMaster，发送过期事件触发清理
   * @param id 过期应用尝试实例ID
   */
  @Override
  protected void expire(ApplicationAttemptId id) {
    dispatcher.handle(
        new RMAppAttemptEvent(id, RMAppAttemptEventType.EXPIRE));
  }
}