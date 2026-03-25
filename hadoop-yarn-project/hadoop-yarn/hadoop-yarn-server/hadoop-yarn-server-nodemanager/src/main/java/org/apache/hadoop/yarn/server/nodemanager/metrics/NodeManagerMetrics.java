// 这个文件已经全部加上中文注释
/*
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
package org.apache.hadoop.yarn.server.nodemanager.metrics;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeFloat;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.yarn.api.records.Resource;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * NodeManager节点指标收集类，统一收集和管理NM节点上各类运行指标，
 * 包括容器生命周期、资源分配、磁盘状态、本地化缓存、资源利用率等关键监控数据。
 */
@Metrics(about="Metrics for node manager", context="yarn")
public class NodeManagerMetrics {
  // CHECKSTYLE:OFF:VisibilityModifier
  // 已启动容器总数
  @Metric MutableCounterInt containersLaunched;
  // 已完成容器总数
  @Metric MutableCounterInt containersCompleted;
  // 启动失败容器总数
  @Metric MutableCounterInt containersFailed;
  // 被杀死容器总数
  @Metric MutableCounterInt containersKilled;
  // 失败回滚容器总数
  @Metric MutableCounterInt containersRolledBackOnFailure;
  // 当前正在重新初始化的容器数量
  @Metric("# of reInitializing containers")
      MutableGaugeInt containersReIniting;
  // 当前正在初始化的容器数量
  @Metric("# of initializing containers")
      MutableGaugeInt containersIniting;
  // 当前正在运行的容器数量
  @Metric MutableGaugeInt containersRunning;
  // 当前暂停的容器数量
  @Metric("# of paused containers") MutableGaugeInt containersPaused;
  // 当前已分配内存总量，单位GB
  @Metric("Current allocated memory in GB")
      MutableGaugeInt allocatedGB;
  // 当前已分配容器总数
  @Metric("Current # of allocated containers")
      MutableGaugeInt allocatedContainers;
  // 当前可用内存总量，单位GB
  @Metric MutableGaugeInt availableGB;
  // 当前已分配虚拟CPU核心总数
  @Metric("Current allocated Virtual Cores")
      MutableGaugeInt allocatedVCores;
  // 当前可用虚拟CPU核心总数
  @Metric MutableGaugeInt availableVCores;
  // 容器启动耗时统计
  @Metric("Container launch duration")
      MutableRate containerLaunchDuration;

  // 队列中保证调度容器数量
  @Metric("Containers queued (Guaranteed)")
  MutableGaugeInt containersGuaranteedQueued;
  // 队列中机会调度容器数量
  @Metric("Containers queued (Opportunistic)")
  MutableGaugeInt containersOpportunisticQueued;

  // 损坏的本地目录数量
  @Metric("# of bad local dirs")
      MutableGaugeInt badLocalDirs;
  // 损坏的日志目录数量
  @Metric("# of bad log dirs")
      MutableGaugeInt badLogDirs;
  // 健康本地目录磁盘利用率百分比
  @Metric("Disk utilization % on good local dirs")
      MutableGaugeInt goodLocalDirsDiskUtilizationPerc;
  // 健康日志目录磁盘利用率百分比
  @Metric("Disk utilization % on good log dirs")
      MutableGaugeInt goodLogDirsDiskUtilizationPerc;

  // 机会容器已分配内存总量，单位GB
  @Metric("Current allocated memory by opportunistic containers in GB")
      MutableGaugeLong allocatedOpportunisticGB;
  // 机会容器已分配虚拟CPU核心总数
  @Metric("Current allocated Virtual Cores by opportunistic containers")
      MutableGaugeInt allocatedOpportunisticVCores;
  // 正在运行的机会容器数量
  @Metric("# of running opportunistic containers")
      MutableGaugeInt runningOpportunisticContainers;

  // 清理前本地缓存总大小，单位字节
  @Metric("Local cache size (public and private) before clean (Bytes)")
  MutableGaugeLong cacheSizeBeforeClean;
  // 本地缓存总共删除字节数
  @Metric("# of total bytes deleted from the public and private local cache")
  MutableGaugeLong totalBytesDeleted;
  // 公共本地缓存删除字节数
  @Metric("# of bytes deleted from the public local cache")
  MutableGaugeLong publicBytesDeleted;
  // 私有本地缓存删除字节数
  @Metric("# of bytes deleted from the private local cache")
  MutableGaugeLong privateBytesDeleted;
  // 所有容器当前已用物理内存，单位GB
  @Metric("Current used physical memory by all containers in GB")
  MutableGaugeInt containerUsedMemGB;
  // 所有容器当前已用虚拟内存，单位GB
  @Metric("Current used virtual memory by all containers in GB")
  MutableGaugeInt containerUsedVMemGB;
  // 所有容器聚合CPU利用率
  @Metric("Aggregated CPU utilization of all containers")
  MutableGaugeFloat containerCpuUtilization;
  // 当前节点总已用内存，单位GB
  @Metric("Current used memory by this node in GB")
  MutableGaugeInt nodeUsedMemGB;
  // 当前节点总已用虚拟内存，单位GB
  @Metric("Current used virtual memory by this node in GB")
  MutableGaugeInt nodeUsedVMemGB;
  // 当前节点CPU利用率
  @Metric("Current CPU utilization")
  MutableGaugeFloat nodeCpuUtilization;
  // 当前节点GPU利用率
  @Metric("Current GPU utilization")
  MutableGaugeFloat nodeGpuUtilization;
  // 当前正在运行的应用数量
  @Metric("Current running apps")
  MutableGaugeInt applicationsRunning;

  // 本地化缓存未命中总字节数
  @Metric("Missed localization requests in bytes")
      MutableCounterLong localizedCacheMissBytes;
  // 本地化缓存命中总字节数
  @Metric("Cached localization requests in bytes")
      MutableCounterLong localizedCacheHitBytes;
  // 本地化缓存按字节计算命中率百分比
  @Metric("Localization cache hit ratio (bytes)")
      MutableGaugeInt localizedCacheHitBytesRatio;
  // 本地化缓存未命中文件总数
  @Metric("Missed localization requests (files)")
      MutableCounterLong localizedCacheMissFiles;
  // 本地化缓存命中文件总数
  @Metric("Cached localization requests (files)")
      MutableCounterLong localizedCacheHitFiles;
  // 本地化缓存按文件计算命中率百分比
  @Metric("Localization cache hit ratio (files)")
      MutableGaugeInt localizedCacheHitFilesRatio;
  // 容器本地化耗时统计，单位毫秒
  @Metric("Container localization time in milliseconds")
      MutableRate localizationDurationMillis;

  // 容器监控总耗时，单位毫秒
  @Metric("ContainerMonitor time cost in milliseconds")
  MutableGaugeLong containersMonitorCostTime;

  // CHECKSTYLE:ON:VisibilityModifier

  // JVM指标实例
  private JvmMetrics jvmMetrics = null;

  // 已分配内存总量，单位MB
  private long allocatedMB;
  // 可用内存总量，单位MB
  private long availableMB;
  // 机会容器已分配内存总量，单位MB
  private long allocatedOpportunisticMB;

  /**
   * 私有构造函数，使用传入的JVM指标初始化NodeManager指标。
   * @param jvmMetrics JVM指标实例
   */
  private NodeManagerMetrics(JvmMetrics jvmMetrics) {
    this.jvmMetrics = jvmMetrics;
  }

  /**
   * 使用默认指标系统创建NodeManager指标实例。
   * @return NodeManagerMetrics实例
   */
  public static NodeManagerMetrics create() {
    return create(DefaultMetricsSystem.instance());
  }

  /**
   * 使用指定指标系统创建NodeManager指标实例。
   * @param ms 指标系统实例
   * @return NodeManagerMetrics实例
   */
  private static NodeManagerMetrics create(MetricsSystem ms) {
    JvmMetrics jm = JvmMetrics.initSingleton("NodeManager", null);
    return ms.register(new NodeManagerMetrics(jm));
  }

  /**
   * 获取JVM指标实例。
   * @return JVM指标实例
   */
  public JvmMetrics getJvmMetrics() {
    return jvmMetrics;
  }

  // Potential instrumentation interface methods

  /**
   * 记录容器启动，累加启动容器计数器。
   */
  public void launchedContainer() {
    containersLaunched.incr();
  }

  /**
   * 记录容器完成，累加完成容器计数器。
   */
  public void completedContainer() {
    containersCompleted.incr();
  }

  /**
   * 记录容器失败回滚，累加回滚容器计数器。
   */
  public void rollbackContainerOnFailure() {
    containersRolledBackOnFailure.incr();
  }

  /**
   * 记录容器启动失败，累加失败容器计数器。
   */
  public void failedContainer() {
    containersFailed.incr();
  }

  /**
   * 记录容器被杀死，累加杀死容器计数器。
   */
  public void killedContainer() {
    containersKilled.incr();
  }

  /**
   * 容器进入初始化阶段，增加初始化容器计数。
   */
  public void initingContainer() {
    containersIniting.incr();
  }

  /**
   * 容器结束初始化阶段，减少初始化容器计数。
   */
  public void endInitingContainer() {
    containersIniting.decr();
  }

  /**
   * 容器进入运行阶段，增加运行容器计数。
   */
  public void runningContainer() {
    containersRunning.incr();
  }

  /**
   * 容器结束运行阶段，减少运行容器计数。
   */
  public void endRunningContainer() {
    containersRunning.decr();
  }

  /**
   * 容器进入重新初始化阶段，增加重新初始化容器计数。
   */
  public void reInitingContainer() {
    containersReIniting.incr();
  }

  /**
   * 容器结束重新初始化阶段，减少重新初始化容器计数。
   */
  public void endReInitingContainer() {
    containersReIniting.decr();
  }

  /**
   * 应用进入运行阶段，增加运行应用计数。
   */
  public void runningApplication() {
    applicationsRunning.incr();
  }

  /**
   * 应用结束运行阶段，减少运行应用计数。
   */
  public void endRunningApplication() {
    applicationsRunning.decr();
  }

  /**
   * 容器进入暂停阶段，增加暂停容器计数。
   */
  public void pausedContainer() {
    containersPaused.incr();
  }

  /**
   * 容器结束暂停阶段，减少暂停容器计数。
   */
  public void endPausedContainer() {
    containersPaused.decr();
  }

  /**
   * 分配容器资源，更新已分配和可用资源指标。
   * @param res 要分配的资源
   */
  public void allocateContainer(Resource res) {
    allocatedContainers.incr();
    allocatedMB = allocatedMB + res.getMemorySize();
    allocatedGB.set((int)Math.ceil(allocatedMB/1024d));
    availableMB = availableMB - res.getMemorySize();
    availableGB.set((int)Math.floor(availableMB/1024d));
    allocatedVCores.incr(res.getVirtualCores());
    availableVCores.decr(res.getVirtualCores());
  }

  /**
   * 释放容器资源，更新已分配和可用资源指标。
   * @param res 要释放的资源
   */
  public void releaseContainer(Resource res) {
    allocatedContainers.decr();
    allocatedMB = allocatedMB - res.getMemorySize();
    allocatedGB.set((int)Math.ceil(allocatedMB/1024d));
    availableMB = availableMB + res.getMemorySize();
    availableGB.set((int)Math.floor(availableMB/1024d));
    allocatedVCores.decr(res.getVirtualCores());
    availableVCores.incr(res.getVirtualCores());
  }

  /**
   * 更新容器资源分配变更，根据资源变化量调整指标。
   * @param before 变更前分配的资源
   * @param now 变更后分配的资源
   */
  public void changeContainer(Resource before, Resource now) {
    long deltaMB = now.getMemorySize() - before.getMemorySize();
    int deltaVCores = now.getVirtualCores() - before.getVirtualCores();
    allocatedMB = allocatedMB + deltaMB;
    allocatedGB.set((int)Math.ceil(allocatedMB/1024d));
    availableMB = availableMB - deltaMB;
    availableGB.set((int)Math.floor(availableMB/1024d));
    allocatedVCores.incr(deltaVCores);
    availableVCores.decr(deltaVCores);
  }

  /**
   * 启动机会调度容器，更新机会容器资源指标。
   * @param res 机会容器占用的资源
   */
  public void startOpportunisticContainer(Resource res) {
    runningOpportunisticContainers.incr();
    allocatedOpportunisticMB = allocatedOpportunisticMB + res.getMemorySize();
    allocatedOpportunisticGB
        .set((int) Math.ceil(allocatedOpportunisticMB / 1024d));
    allocatedOpportunisticVCores.incr(res.getVirtualCores());
  }

  /**
   * 完成机会调度容器，释放机会容器资源指标。
   * @param res 机会容器释放的资源
   */
  public void completeOpportunisticContainer(Resource res) {
    runningOpportunisticContainers.decr();
    allocatedOpportunisticMB = allocatedOpportunisticMB - res.getMemorySize();
    allocatedOpportunisticGB
        .set((int) Math.ceil(allocatedOpportunisticMB / 1024d));
    allocatedOpportunisticVCores.decr(res.getVirtualCores());
  }

  /**
   * 设置队列中不同类型容器排队数量。
   * @param opportunisticCount 机会容器排队数量
   * @param guaranteedCount 保证容器排队数量
   */
  public void setQueuedContainers(int opportunisticCount, int guaranteedCount) {
    containersOpportunisticQueued.set(opportunisticCount);
    containersGuaranteedQueued.set(guaranteedCount);
  }

  /**
   * 新增节点可用资源，更新节点可用资源指标。
   * @param res 新增的可用资源
   */
  public void addResource(Resource res) {
    availableMB = availableMB + res.getMemorySize();
    availableGB.set((int)Math.floor(availableMB/1024d));
    availableVCores.incr(res.getVirtualCores());
  }

  /**
   * 添加容器启动耗时样本。
   * @param value 容器启动耗时
   */
  public void addContainerLaunchDuration(long value) {
    containerLaunchDuration.add(value);
  }

  /**
   * 设置损坏本地目录数量指标。
   * @param badLocalDirs 损坏本地目录数量
   */
  public void setBadLocalDirs(int badLocalDirs) {
    this.badLocalDirs.set(badLocalDirs);
  }

  /**
   * 设置损坏日志目录数量指标。
   * @param badLogDirs 损坏日志目录数量
   */
  public void setBadLogDirs(int badLogDirs) {
    this.badLogDirs.set(badLogDirs);
  }

  /**
   * 设置健康本地目录磁盘利用率指标。
   * @param goodLocalDirsDiskUtilizationPerc 健康本地目录磁盘利用率百分比
   */
  public void setGoodLocalDirsDiskUtilizationPerc(
      int goodLocalDirsDiskUtilizationPerc) {
    this.goodLocalDirsDiskUtilizationPerc.set(goodLocalDirsDiskUtilizationPerc);
  }

  /**
   * 设置健康日志目录磁盘利用率指标。
   * @param goodLogDirsDiskUtilizationPerc 健康日志目录磁盘利用率百分比
   */
  public void setGoodLogDirsDiskUtilizationPerc(
      int goodLogDirsDiskUtilizationPerc) {
    this.goodLogDirsDiskUtilizationPerc.set(goodLogDirsDisk