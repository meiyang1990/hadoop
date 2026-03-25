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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.monitor;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.SystemClock;

/**
 * 应用生命周期监控服务，根据配置的应用最大运行时长监控应用，超出运行时长的应用会被强制杀死。
 */
public class RMAppLifetimeMonitor
    extends AbstractLivelinessMonitor<RMAppToMonitor> {

  private static final Logger LOG =
      LoggerFactory.getLogger(RMAppLifetimeMonitor.class);

  // YARN ResourceManager 上下文对象，保存全局状态
  private RMContext rmContext;

  /**
   * 构造应用生命周期监控器
   * @param rmContext RM全局上下文
   */
  public RMAppLifetimeMonitor(RMContext rmContext) {
    super(RMAppLifetimeMonitor.class.getName(), SystemClock.getInstance());
    this.rmContext = rmContext;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置获取监控检查间隔
    long monitorInterval =
        conf.getLong(YarnConfiguration.RM_APPLICATION_MONITOR_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_APPLICATION_MONITOR_INTERVAL_MS);
    // 非法间隔则使用默认值
    if (monitorInterval <= 0) {
      monitorInterval =
          YarnConfiguration.DEFAULT_RM_APPLICATION_MONITOR_INTERVAL_MS;
    }
    // 设置监控间隔
    setMonitorInterval(monitorInterval);
    // 不需要额外过期清理间隔
    setExpireInterval(0); // No need of expire interval for App.
    // 应用重启时不重置过期时间
    setResetTimeOnStart(false); // do not reset expire time on restart
    LOG.info("Application lifelime monitor interval set to " + monitorInterval
        + " ms.");
    super.serviceInit(conf);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected synchronized void expire(RMAppToMonitor monitoredAppKey) {
    // 获取监控应用ID
    ApplicationId appId = monitoredAppKey.getApplicationId();
    // 从RM上下文获取应用实例
    RMApp app = rmContext.getRMApps().get(appId);
    // 应用已不存在直接返回
    if (app == null) {
      return;
    }
    // 构建杀死应用的诊断信息
    String diagnostics = "Application is killed by ResourceManager as it"
        + " has exceeded the lifetime period.";
    // 发送杀死应用事件到RM事件分发器
    rmContext.getDispatcher().getEventHandler()
        .handle(new RMAppEvent(appId, RMAppEventType.KILL, diagnostics));
  }

  /**
   * 注册一个应用进行生命周期监控
   * @param appId 应用ID
   * @param timeoutType 超时类型
   * @param expireTime 过期时间戳
   */
  public void registerApp(ApplicationId appId,
      ApplicationTimeoutType timeoutType, long expireTime) {
    RMAppToMonitor appToMonitor = new RMAppToMonitor(appId, timeoutType);
    register(appToMonitor, expireTime);
  }

  /**
   * 取消指定类型超时的应用监控
   * @param appId 应用ID
   * @param timeoutType 超时类型
   */
  public void unregisterApp(ApplicationId appId,
      ApplicationTimeoutType timeoutType) {
    RMAppToMonitor remove = new RMAppToMonitor(appId, timeoutType);
    unregister(remove);
  }

  /**
   * 取消应用所有指定类型超时的监控
   * @param appId 应用ID
   * @param timeoutTypes 超时类型集合
   */
  public void unregisterApp(ApplicationId appId,
      Set<ApplicationTimeoutType> timeoutTypes) {
    for (ApplicationTimeoutType timeoutType : timeoutTypes) {
      unregisterApp(appId, timeoutType);
    }
  }

  /**
   * 更新应用多个类型超时配置
   * @param appId 应用ID
   * @param timeouts 超时类型与对应过期时间映射
   */
  public void updateApplicationTimeouts(ApplicationId appId,
      Map<ApplicationTimeoutType, Long> timeouts) {
    for (Entry<ApplicationTimeoutType, Long> entry : timeouts.entrySet()) {
      ApplicationTimeoutType timeoutType = entry.getKey();
      RMAppToMonitor update = new RMAppToMonitor(appId, timeoutType);
      register(update, entry.getValue());
    }
  }
}