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
 * YARN 集群指标统计
 * 使用 Hadoop Metrics2 框架收集和暴露集群级别的运行时指标，包括：
 * - NodeManager 状态统计（active、decommissioning、decommissioned、lost、unhealthy 等）
 * - 资源容量和利用率（内存、虚拟核心、自定义资源）
 * - 应用性能指标（AM 启动延迟、注册延迟、容器分配延迟）
 * - 事件队列大小（RM 和调度器事件队列）
 * - 容器分配速率（每秒分配数）
 * 
 * 采用单例模式，通过定时任务统计容器分配速率
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
   * 构造方法：初始化定时任务统计每秒容器分配数
   * 使用定时线程池每秒将累计分配数更新到指标，并重置计数器
   */
  ClusterMetrics() {
    assignCounterExecutor  = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder().
            setDaemon(true).setNameFormat("ContainerAssignmentCounterThread").
            build());
    // 每秒执行一次：将分配计数器的值更新到指标并归零
    assignCounterExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        containerAssignedPerSecond.set(numContainersAssigned.getAndSet(0));
      }
    }, 1, 1, TimeUnit.SECONDS);
  }

  /**
   * 单例模式获取指标实例
   * 使用双重检查锁定确保线程安全的延迟初始化
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
   * 注册指标到 Hadoop Metrics2 系统
   * 对于自定义资源类型（超过 Memory 和 VCore），动态注册额外指标
   */
  private static void registerMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "ResourceManager");
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      ms.register("ClusterMetrics", "Metrics for the Yarn Cluster", INSTANCE);
    }

    // 如果存在自定义资源类型（如 GPU、FPGA 等），动态注册对应指标
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

  @VisibleForTesting
  public synchronized static void destroy() {
    if (INSTANCE != null && INSTANCE.getAssignCounterExecutor() != null) {
      INSTANCE.getAssignCounterExecutor().shutdownNow();
    }
    isInitialized.set(false);
    INSTANCE = null;
  }
  
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

  public void addAMLaunchDelay(long delay) {
    aMLaunchDelay.add(delay);
  }

  public void addAMRegisterDelay(long delay) {
    aMRegisterDelay.add(delay);
  }

  public long getCapabilityMB() {
    return capabilityMB.value();
  }

  public long getCapabilityVirtualCores() {
    return capabilityVirtualCores.value();
  }

  public Map<String, Long> getCustomResourceCapability() {
    return customResourceCapability.getValues();
  }

  public void setCustomResourceCapability(Resource res) {
    this.customResourceCapability.set(res);
  }

  public void incrCapability(Resource res) {
    if (res != null) {
      capabilityMB.incr(res.getMemorySize());
      capabilityVirtualCores.incr(res.getVirtualCores());
      if (customResourceCapability != null) {
        customResourceCapability.increase(res);
      }
    }
  }

  public void decrCapability(Resource res) {
    if (res != null) {
      capabilityMB.decr(res.getMemorySize());
      capabilityVirtualCores.decr(res.getVirtualCores());
      if (customResourceCapability != null) {
        customResourceCapability.decrease(res);
      }
    }
  }

  public void addAMContainerAllocationDelay(long delay) {
    aMContainerAllocationDelay.add(delay);
  }

  public MutableRate getAMContainerAllocationDelay() {
    return aMContainerAllocationDelay;
  }

  public long getUtilizedMB() {
    return utilizedMB.value();
  }

  public void incrUtilizedMB(long delta) {
    utilizedMB.incr(delta);
  }

  public void decrUtilizedMB(long delta) {
    utilizedMB.decr(delta);
  }

  public void decrUtilizedVirtualCores(long delta) {
    utilizedVirtualCores.decr(delta);
  }

  public long getUtilizedVirtualCores() {
    return utilizedVirtualCores.value();
  }

  public void incrUtilizedVirtualCores(long delta) {
    utilizedVirtualCores.incr(delta);
  }

  public int getContainerAssignedPerSecond() {
    return containerAssignedPerSecond.value();
  }

  public void incrNumContainerAssigned() {
    numContainersAssigned.incrementAndGet();
  }

  private ScheduledThreadPoolExecutor getAssignCounterExecutor(){
    return assignCounterExecutor;
  }

  public int getRmEventQueueSize() {
    return rmDispatcherEventQueueSize.value();
  }

  public void setRmEventQueueSize(int rmEventQueueSize) {
    this.rmDispatcherEventQueueSize.set(rmEventQueueSize);
  }

  public int getSchedulerEventQueueSize() {
    return schedulerDispatcherEventQueueSize.value();
  }

  public void setSchedulerEventQueueSize(int schedulerEventQueueSize) {
    this.schedulerDispatcherEventQueueSize.set(schedulerEventQueueSize);
  }
}
