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

import static org.apache.hadoop.metrics2.lib.Interns.info;

import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.metrics.CustomResourceMetricValue;
import org.apache.hadoop.yarn.metrics.CustomResourceMetrics;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

/**
<<<<<<< HEAD
 * YARN 集群指标统计
 * 使用 Hadoop Metrics2 框架收集和暴露集群级别的运行时指标，包括：
 * - NodeManager 状态统计（active、decommissioning、decommissioned、lost、unhealthy 等）
 * - 资源容量和利用率（内存、虚拟核心、自定义资源）
 * - 应用性能指标（AM 启动延迟、注册延迟、容器分配延迟）
 * - 事件队列大小（RM 和调度器事件队列）
 * - 容器分配速率（每秒分配数）
 * 
 * 采用单例模式，通过定时任务统计容器分配速率
=======
 * YARN ResourceManager 集群指标采集类，负责收集和暴露整个YARN集群的运行状态指标，
 * 包括节点状态、资源使用、调度延迟、事件队列等核心监控数据，供Metrics系统采集展示。
>>>>>>> a7f26154e2430da367a92d826f772851319cf52d
 */
@InterfaceAudience.Private
@Metrics(context="yarn")
public class ClusterMetrics {
  
  // 确保单例只初始化一次
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);
  
  // NodeManager 状态相关指标
  @Metric("# of active NMs") MutableGaugeInt numActiveNMs;
  @Metric("# of decommissioning NMs") MutableGaugeInt numDecommissioningNMs;
  @Metric("# of decommissioned NMs") MutableGaugeInt numDecommissionedNMs;
  @Metric("# of lost NMs") MutableGaugeInt numLostNMs;
  @Metric("# of unhealthy NMs") MutableGaugeInt numUnhealthyNMs;
  @Metric("# of Rebooted NMs") MutableGaugeInt numRebootedNMs;
  @Metric("# of Shutdown NMs") MutableGaugeInt numShutdownNMs;
  
  // ApplicationMaster 性能指标
  @Metric("AM container launch delay") MutableRate aMLaunchDelay;
  @Metric("AM register delay") MutableRate aMRegisterDelay;
  @Metric("AM container allocation delay")
  private MutableRate aMContainerAllocationDelay;
  
  // 集群资源利用率和容量
  @Metric("Memory Utilization") MutableGaugeLong utilizedMB;
  @Metric("Vcore Utilization") MutableGaugeLong utilizedVirtualCores;
  @Metric("Memory Capability") MutableGaugeLong capabilityMB;
  @Metric("Vcore Capability") MutableGaugeLong capabilityVirtualCores;
  
  // RM 事件处理器 CPU 使用率监控
  @Metric("RM Event Processor CPU Usage 60 second Avg") MutableGaugeLong
    rmEventProcCPUAvg;
  @Metric("RM Event Processor CPU Usage 60 second Max") MutableGaugeLong
    rmEventProcCPUMax;
    
  // 容器分配速率
  @Metric("# of Containers assigned in the last second") MutableGaugeInt
    containerAssignedPerSecond;
    
  // 事件队列大小
  @Metric("# of rm dispatcher event queue size")
    MutableGaugeInt rmDispatcherEventQueueSize;
  @Metric("# of scheduler dispatcher event queue size")
    MutableGaugeInt schedulerDispatcherEventQueueSize;

  private boolean rmEventProcMonitorEnable = false;

  private static final MetricsInfo RECORD_INFO = info("ClusterMetrics",
  "Metrics for the Yarn Cluster");

  private static final String CUSTOM_RESOURCE_CAPABILITY_METRIC_PREFIX =
      "Capability.";
  private static final String CUSTOM_RESOURCE_CAPABILITY_METRIC_DESC =
      "NAME Capability";

  private static CustomResourceMetrics customResourceMetrics;

  private final CustomResourceMetricValue customResourceCapability =
      new CustomResourceMetricValue();
  
  private static volatile ClusterMetrics INSTANCE = null;
  private static MetricsRegistry registry;

  private AtomicInteger numContainersAssigned =  new AtomicInteger(0);
  private ScheduledThreadPoolExecutor assignCounterExecutor;

  /**
<<<<<<< HEAD
   * 构造方法：初始化定时任务统计每秒容器分配数
   * 使用定时线程池每秒将累计分配数更新到指标，并重置计数器
=======
   * 构造函数，初始化容器分配计数器定时任务，每秒统计一次分配容器数量。
>>>>>>> a7f26154e2430da367a92d826f772851319cf52d
   */
  ClusterMetrics() {
    // 创建守护线程池，负责每秒重置计数器
    assignCounterExecutor  = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder().
            setDaemon(true).setNameFormat("ContainerAssignmentCounterThread").
            build());
<<<<<<< HEAD
    // 每秒执行一次：将分配计数器的值更新到指标并归零
=======
    // 启动定时任务，每秒更新每秒容器分配数指标
>>>>>>> a7f26154e2430da367a92d826f772851319cf52d
    assignCounterExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        containerAssignedPerSecond.set(numContainersAssigned.getAndSet(0));
      }
    }, 1, 1, TimeUnit.SECONDS);
  }

  /**
<<<<<<< HEAD
   * 单例模式获取指标实例
   * 使用双重检查锁定确保线程安全的延迟初始化
=======
   * 获取ClusterMetrics单例实例，懒加载实现线程安全的单例模式。
   * @return 集群指标单例对象
>>>>>>> a7f26154e2430da367a92d826f772851319cf52d
   */
  public static ClusterMetrics getMetrics() {
    if(!isInitialized.get()){
      synchronized (ClusterMetrics.class) {
        if(INSTANCE == null){
          INSTANCE = new ClusterMetrics();
          registerMetrics();
          isInitialized.set(true);
        }
      }
    }
    return INSTANCE;
  }

  /**
<<<<<<< HEAD
   * 注册指标到 Hadoop Metrics2 系统
   * 对于自定义资源类型（超过 Memory 和 VCore），动态注册额外指标
=======
   * 注册集群指标到Hadoop Metrics系统，初始化自定义资源指标。
>>>>>>> a7f26154e2430da367a92d826f772851319cf52d
   */
  private static void registerMetrics() {
    // 创建指标注册表，标记所属组件为ResourceManager
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "ResourceManager");
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      // 将当前指标注册到默认指标系统
      ms.register("ClusterMetrics", "Metrics for the Yarn Cluster", INSTANCE);
    }

<<<<<<< HEAD
    // 如果存在自定义资源类型（如 GPU、FPGA 等），动态注册对应指标
=======
    // 如果存在除内存和vcore之外还有自定义资源类型，注册自定义资源指标
>>>>>>> a7f26154e2430da367a92d826f772851319cf52d
    if (ResourceUtils.getNumberOfKnownResourceTypes() > 2) {
      customResourceMetrics =
          new CustomResourceMetrics();
      Map<String, Long> customResources =
          customResourceMetrics.initAndGetCustomResources();
      customResourceMetrics.
          registerCustomResources(customResources,
              registry, CUSTOM_RESOURCE_CAPABILITY_METRIC_PREFIX,
              CUSTOM_RESOURCE_CAPABILITY_METRIC_DESC);
    }
  }

  /**
   * 销毁单例实例，关闭定时任务，用于测试场景。
   */
  @VisibleForTesting
  public synchronized static void destroy() {
    if (INSTANCE != null && INSTANCE.getAssignCounterExecutor() != null) {
      INSTANCE.getAssignCounterExecutor().shutdownNow();
    }
    isInitialized.set(false);
    INSTANCE = null;
  }
  
  /**
   * 设置RM事件处理器CPU监控是否启用。
   * @param value 是否启用
   */
  // Indicate whether RM Event Thread CPU Monitor is enabled
  public void setRmEventProcMonitorEnable(boolean value) {
    rmEventProcMonitorEnable = value;
  }
  public boolean getRmEventProcMonitorEnable() {
    return rmEventProcMonitorEnable;
  }
  // RM Event Processor CPU Usage
  public long getRmEventProcCPUAvg() {
    return rmEventProcCPUAvg.value();
  }
  public void setRmEventProcCPUAvg(long value) {
    rmEventProcCPUAvg.set(value);
  }
  public long getRmEventProcCPUMax() {
    return rmEventProcCPUMax.value();
  }
  public void setRmEventProcCPUMax(long value) {
    rmEventProcCPUMax.set(value);
  }

  //Active Nodemanagers
  public int getNumActiveNMs() {
    return numActiveNMs.value();
  }

  // Decommissioning NMs
  public int getNumDecommissioningNMs() {
    return numDecommissioningNMs.value();
  }

  public void incrDecommissioningNMs() {
    numDecommissioningNMs.incr();
  }

  public void setDecommissioningNMs(int num) {
    numDecommissioningNMs.set(num);
  }

  public void decrDecommissioningNMs() {
    numDecommissioningNMs.decr();
  }

  //Decommisioned NMs
  public int getNumDecommisionedNMs() {
    return numDecommissionedNMs.value();
  }

  public void incrDecommisionedNMs() {
    numDecommissionedNMs.incr();
  }

  public void setDecommisionedNMs(int num) {
    numDecommissionedNMs.set(num);
  }

  public void decrDecommisionedNMs() {
    numDecommissionedNMs.decr();
  }
  
  //Lost NMs
  public int getNumLostNMs() {
    return numLostNMs.value();
  }

  public void incrNumLostNMs() {
    numLostNMs.incr();
  }
  
  public void decrNumLostNMs() {
    numLostNMs.decr();
  }
  
  //Unhealthy NMs
  public int getUnhealthyNMs() {
    return numUnhealthyNMs.value();
  }

  public void incrNumUnhealthyNMs() {
    numUnhealthyNMs.incr();
  }
  
  public void decrNumUnhealthyNMs() {
    numUnhealthyNMs.decr();
  }
  
  //Rebooted NMs
  public int getNumRebootedNMs() {
    return numRebootedNMs.value();
  }
  
  public void incrNumRebootedNMs() {
    numRebootedNMs.incr();
  }
  
  public void decrNumRebootedNMs() {
    numRebootedNMs.decr();
  }

  // Shutdown NMs
  public int getNumShutdownNMs() {
    return numShutdownNMs.value();
  }

  public void incrNumShutdownNMs() {
    numShutdownNMs.incr();
  }

  public void decrNumShutdownNMs() {
    numShutdownNMs.decr();
  }

  public void incrNumActiveNodes() {
    numActiveNMs.incr();
  }

  public void decrNumActiveNodes() {
    numActiveNMs.decr();
  }

  /**
   * 添加一次AM容器启动延迟采样。
   * @param delay 延迟时间，单位毫秒
   */
  public void addAMLaunchDelay(long delay) {
    aMLaunchDelay.add(delay);
  }

  /**
   * 添加一次AM注册延迟采样。
   * @param delay 延迟时间，单位毫秒
   */
  public void addAMRegisterDelay(long delay) {
    aMRegisterDelay.add(delay);
  }

  public long getCapabilityMB() {
    return capabilityMB.value();
  }

  public long getCapabilityVirtualCores() {
    return capabilityVirtualCores.value();
  }

  /**
   * 获取所有自定义资源的集群总能力。
   * @return 自定义资源名称与总容量映射
   */
  public Map<String, Long> getCustomResourceCapability() {
    return customResourceCapability.getValues();
  }

  /**
   * 设置自定义资源总容量，从传入资源刷新当前值。
   * @param res 资源对象，包含所有自定义资源值
   */
  public void setCustomResourceCapability(Resource res) {
    this.customResourceCapability.set(res);
  }

  /**
   * 集群新增节点上线时，增加集群总资源容量。
   * @param res 新增节点的总资源
   */
  public void incrCapability(Resource res) {
    if (res != null) {
      capabilityMB.incr(res.getMemorySize());
      capabilityVirtualCores.incr(res.getVirtualCores());
      if (customResourceCapability != null) {
        customResourceCapability.increase(res);
      }
    }
  }

  /**
   * 节点下线时，减少集群总资源容量。
   * @param res 下线节点的总资源
   */
  public void decrCapability(Resource res) {
    if (res != null) {
      capabilityMB.decr(res.getMemorySize());
      capabilityVirtualCores.decr(res.getVirtualCores());
      if (customResourceCapability != null) {
        customResourceCapability.decrease(res);
      }
    }
  }

  /**
   * 添加一次AM容器分配延迟采样。
   * @param delay 延迟时间，单位毫秒
   */
  public void addAMContainerAllocationDelay(long delay) {
    aMContainerAllocationDelay.add(delay);
  }

  public MutableRate getAMContainerAllocationDelay() {
    return aMContainerAllocationDelay;
  }

  public long getUtilizedMB() {
    return utilizedMB.value();
  }

  /**
   * 分配容器后增加已使用内存容量。
   * @param delta 增量值
   */
  public void incrUtilizedMB(long delta) {
    utilizedMB.incr(delta);
  }

  /**
   * 释放容器后减少已使用内存容量。
   * @param delta 减少值
   */
  public void decrUtilizedMB(long delta) {
    utilizedMB.decr(delta);
  }

  /**
   * 释放容器后减少已使用vcore容量。
   * @param delta 减少值
   */
  public void decrUtilizedVirtualCores(long delta) {
    utilizedVirtualCores.decr(delta);
  }

  public long getUtilizedVirtualCores() {
    return utilizedVirtualCores.value();
  }

  /**
   * 分配容器后增加已使用vcore容量。
   * @param delta 增量值
   */
  public void incrUtilizedVirtualCores(long delta) {
    utilizedVirtualCores.incr(delta);
  }

  public int getContainerAssignedPerSecond() {
    return containerAssignedPerSecond.value();
  }

  /**
   * 增加一秒内分配容器计数。
   */
  public void incrNumContainerAssigned() {
    numContainersAssigned.incrementAndGet();
  }

  private ScheduledThreadPoolExecutor getAssignCounterExecutor(){
    return assignCounterExecutor;
  }

  public int getRmEventQueueSize() {
    return rmDispatcherEventQueueSize.value();
  }

  /**
   * 更新RM调度器事件队列当前大小指标。
   * @param rmEventQueueSize 当前队列大小
   */
  public void setRmEventQueueSize(int rmEventQueueSize) {
    this.rmDispatcherEventQueueSize.set(rmEventQueueSize);
  }

  public int getSchedulerEventQueueSize() {
    return schedulerDispatcherEventQueueSize.value();
  }

  /**
   * 更新调度器事件队列当前大小指标。
   * @param schedulerEventQueueSize 当前队列大小
   */
  public void setSchedulerEventQueueSize(int schedulerEventQueueSize) {
    this.schedulerDispatcherEventQueueSize.set(schedulerEventQueueSize);
  }
}