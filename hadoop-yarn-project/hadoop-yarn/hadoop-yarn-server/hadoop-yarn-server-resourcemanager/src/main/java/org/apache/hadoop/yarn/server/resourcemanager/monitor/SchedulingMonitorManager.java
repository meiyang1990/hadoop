// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * YARN ResourceManager 调度监控管理器，负责管理所有调度监控器的生命周期，
 * 包括初始化、启动、停止、重新加载配置等操作，支撑抢占等调度动态调整功能。
 */
public class SchedulingMonitorManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      SchedulingMonitorManager.class);

  /** 存储当前正在运行的调度监控器，key为监控策略类全限定名，value为监控实例 */
  private Map<String, SchedulingMonitor> runningSchedulingMonitors =
      new HashMap<>();
  /** ResourceManager上下文对象，保存全局状态信息 */
  private RMContext rmContext;

  /**
   * 根据配置更新调度监控器集合，新增配置中新增的监控，停止移除配置中删除的监控
   * @param conf YARN配置对象
   * @param startImmediately 是否立即启动新增的监控器
   * @throws YarnException 当配置的策略类不存在或类型不匹配时抛出异常
   */
  private void updateSchedulingMonitors(Configuration conf,
      boolean startImmediately) throws YarnException {
    // 读取调度监控总开关配置
    boolean monitorsEnabled = conf.getBoolean(
        YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS);

    if (!monitorsEnabled) {
      if (!runningSchedulingMonitors.isEmpty()) {
        // 关闭监控时停止所有正在运行的监控器
        LOG.info("Scheduling Monitor disabled, stopping all services");
        stopAndRemoveAll();
      }

      return;
    }

    // 读取配置中指定的监控策略列表
    String[] configuredPolicies = conf.getTrimmedStrings(
        YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES);
    if (configuredPolicies == null || configuredPolicies.length == 0) {
      return;
    }

    // 将配置数组转为Set方便比对
    Set<String> configurePoliciesSet = new HashSet<>();
    for (String s : configuredPolicies) {
      configurePoliciesSet.add(s);
    }

    // 新增配置中存在但未启动的监控器
    for (String s : configurePoliciesSet) {
      if (!runningSchedulingMonitors.containsKey(s)) {
        Class<?> policyClass;
        try {
          // 加载策略类
          policyClass = Class.forName(s);
        } catch (ClassNotFoundException e) {
          String message = "Failed to find class of specified policy=" + s;
          LOG.warn(message);
          throw new YarnException(message);
        }

        // 检查是否为合法的调度编辑策略
        if (SchedulingEditPolicy.class.isAssignableFrom(policyClass)) {
          // 创建策略实例并初始化监控器
          SchedulingEditPolicy policyInstance =
              (SchedulingEditPolicy) ReflectionUtils.newInstance(policyClass,
                  null);
          SchedulingMonitor mon = new SchedulingMonitor(rmContext,
              policyInstance);
          mon.init(conf);
          // 需要时立即启动监控
          if (startImmediately) {
            mon.start();
          }
          runningSchedulingMonitors.put(s, mon);
        } else {
          String message =
              "Specified policy=" + s + " is not a SchedulingEditPolicy class.";
          LOG.warn(message);
          throw new YarnException(message);
        }
      }
    }

    // 停止并移除配置中已删除的监控器
    Set<String> disabledPolicies = Sets.difference(
        runningSchedulingMonitors.keySet(), configurePoliciesSet);
    for (String disabledPolicy : disabledPolicies) {
      LOG.info("SchedulingEditPolicy=" + disabledPolicy
          + " removed, stopping it now ...");
      silentlyStopSchedulingMonitor(disabledPolicy);
      runningSchedulingMonitors.remove(disabledPolicy);
    }
  }

  /**
   * 初始化调度监控管理器，首次加载配置创建所有监控器
   * @param rmContext ResourceManager上下文
   * @param configuration YARN配置对象
   * @throws YarnException 初始化失败时抛出异常
   */
  public synchronized void initialize(RMContext rmContext,
      Configuration configuration) throws YarnException {
    this.rmContext = rmContext;
    stopAndRemoveAll();

    updateSchedulingMonitors(configuration, false);
  }

  /**
   * 重新初始化调度监控管理器，根据更新后的配置刷新监控列表，立即启动新增监控
   * @param rmContext ResourceManager上下文
   * @param configuration 更新后的YARN配置对象
   * @throws YarnException 重新初始化失败时抛出异常
   */
  public synchronized void reinitialize(RMContext rmContext,
      Configuration configuration) throws YarnException {
    this.rmContext = rmContext;

    updateSchedulingMonitors(configuration, true);
  }

  /**
   * 启动所有已初始化的调度监控器
   */
  public synchronized void startAll() {
    for (SchedulingMonitor schedulingMonitor : runningSchedulingMonitors
        .values()) {
      schedulingMonitor.start();
    }
  }

  /**
   * 静默停止指定调度监控器，捕获并记录停止过程中的异常不向外抛出
   * @param name 要停止的监控器名称（策略类全限定名）
   */
  private void silentlyStopSchedulingMonitor(String name) {
    SchedulingMonitor mon = runningSchedulingMonitors.get(name);
    try {
      mon.stop();
      LOG.info("Sucessfully stopped monitor=" + mon.getName());
    } catch (Exception e) {
      LOG.warn("Exception while stopping monitor=" + mon.getName(), e);
    }
  }

  /**
   * 停止所有正在运行的调度监控器，并清空监控列表
   */
  private void stopAndRemoveAll() {
    if (!runningSchedulingMonitors.isEmpty()) {
      for (String schedulingMonitorName : runningSchedulingMonitors
          .keySet()) {
        silentlyStopSchedulingMonitor(schedulingMonitorName);
      }
      runningSchedulingMonitors.clear();
    }
  }

  /**
   * 检查当前是否没有运行任何调度监控器
   * @return 没有运行任何监控器返回true，否则返回false
   */
  public boolean isRSMEmpty() {
    return runningSchedulingMonitors.isEmpty();
  }

  /**
   * 检查当前运行的监控策略集合是否和指定配置集合一致
   * @param configurePoliciesSet 配置中的策略集合
   * @return 一致返回true，否则返回false
   */
  public boolean isSameConfiguredPolicies(Set<String> configurePoliciesSet) {
    return configurePoliciesSet.equals(runningSchedulingMonitors.keySet());
  }

  /**
   * 获取比例容量抢占策略对应的调度监控器（用于向后兼容）
   * @return 如果存在比例容量抢占监控则返回其实例，否则返回null
   */
  public SchedulingMonitor getAvailableSchedulingMonitor() {
    if (isRSMEmpty()) {
      return null;
    }
    for (SchedulingMonitor smon : runningSchedulingMonitors.values()) {
      if (smon.getSchedulingEditPolicy()
          instanceof ProportionalCapacityPreemptionPolicy) {
        return smon;
      }
    }
    return null;
  }

  /**
   * 停止调度监控管理器，停止并移除所有运行中的监控器
   * @throws YarnException 停止过程中发生错误抛出异常
   */
  public synchronized void stop() throws YarnException {
    stopAndRemoveAll();
  }
}