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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupElasticMemoryController;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.server.nodemanager.webapp.ContainerLogsUtils;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;

import java.util.Arrays;
import java.io.File;
import java.util.Map;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * YARN NodeManager容器监控服务实现，负责收集容器资源使用情况，
 * 当容器资源使用超出配额时抢占并杀死容器，同时监控容器日志大小超限。
 * 核心功能包括内存/CPU资源监控、日志大小监控、资源超限处理和指标上报。
 */
public class ContainersMonitorImpl extends AbstractService implements
    ContainersMonitor {

  private final static Logger LOG =
       LoggerFactory.getLogger(ContainersMonitorImpl.class);
  private final static Logger AUDITLOG =
       LoggerFactory.getLogger(ContainersMonitorImpl.class.getName()+".audit");

  private long monitoringInterval;
  private MonitoringThread monitoringThread;
  private int logCheckInterval;
  private LogMonitorThread logMonitorThread;
  private long logDirSizeLimit;
  private long logTotalSizeLimit;
  private CGroupElasticMemoryController oomListenerThread;
  private boolean containerMetricsEnabled;
  private long containerMetricsPeriodMs;
  private long containerMetricsUnregisterDelayMs;

  @VisibleForTesting
  final Map<ContainerId, ProcessTreeInfo> trackingContainers =
      new ConcurrentHashMap<>();

  private final ContainerExecutor containerExecutor;
  private final Dispatcher eventDispatcher;
  private final Context context;
  private ResourceCalculatorPlugin resourceCalculatorPlugin;
  private Configuration conf;
  private static float vmemRatio;
  private Class<? extends ResourceCalculatorProcessTree> processTreeClass;

  /** Maximum virtual memory in bytes. */
  private long maxVmemAllottedForContainers = UNKNOWN_MEMORY_LIMIT;
  /** Maximum physical memory in bytes. */
  private long maxPmemAllottedForContainers = UNKNOWN_MEMORY_LIMIT;

  private boolean pmemCheckEnabled;
  private boolean vmemCheckEnabled;
  private boolean elasticMemoryEnforcement;
  private boolean strictMemoryEnforcement;
  private boolean containersMonitorEnabled;
  private boolean logMonitorEnabled;

  private long maxVCoresAllottedForContainers;

  private static final long UNKNOWN_MEMORY_LIMIT = -1L;
  private int nodeCpuPercentageForYARN;

  /**
   * Type of container metric.
   */
  @Private
  public enum ContainerMetric {
    CPU, MEMORY
  }

  private ResourceUtilization containersUtilization;

  private volatile boolean stopped = false;

  /**
   * 构造容器监控服务实例，初始化监控线程和基础配置
   * @param exec 容器执行器，用于获取容器进程ID等信息
   * @param dispatcher 事件分发器，用于分发容器杀死事件
   * @param context NodeManager上下文，用于获取容器信息
   */
  public ContainersMonitorImpl(ContainerExecutor exec,
      AsyncDispatcher dispatcher, Context context) {
    super("containers-monitor");

    this.containerExecutor = exec;
    this.eventDispatcher = dispatcher;
    this.context = context;

    this.monitoringThread = new MonitoringThread();

    this.logMonitorThread = new LogMonitorThread();

    this.containersUtilization = ResourceUtilization.newInstance(0, 0, 0.0f);
  }

  @Override
  protected void serviceInit(Configuration myConf) throws Exception {
    this.conf = myConf;
    // 读取监控间隔配置
    this.monitoringInterval =
        this.conf.getLong(YarnConfiguration.NM_CONTAINER_MON_INTERVAL_MS,
            this.conf.getLong(YarnConfiguration.NM_RESOURCE_MON_INTERVAL_MS,
                YarnConfiguration.DEFAULT_NM_RESOURCE_MON_INTERVAL_MS));

    // 读取日志监控相关配置
    this.logCheckInterval =
        conf.getInt(YarnConfiguration.NM_CONTAINER_LOG_MON_INTERVAL_MS,
            YarnConfiguration.DEFAULT_NM_CONTAINER_LOG_MON_INTERVAL_MS);
    this.logDirSizeLimit =
        conf.getLong(YarnConfiguration.NM_CONTAINER_LOG_DIR_SIZE_LIMIT_BYTES,
            YarnConfiguration.DEFAULT_NM_CONTAINER_LOG_DIR_SIZE_LIMIT_BYTES);
    this.logTotalSizeLimit =
        conf.getLong(YarnConfiguration.NM_CONTAINER_LOG_TOTAL_SIZE_LIMIT_BYTES,
            YarnConfiguration.DEFAULT_NM_CONTAINER_LOG_TOTAL_SIZE_LIMIT_BYTES);

    // 初始化资源计算器插件
    this.resourceCalculatorPlugin =
        ResourceCalculatorPlugin.getContainersMonitorPlugin(this.conf);
    LOG.info("Using ResourceCalculatorPlugin: {}",
        this.resourceCalculatorPlugin);
    processTreeClass = this.conf.getClass(
            YarnConfiguration.NM_CONTAINER_MON_PROCESS_TREE, null,
            ResourceCalculatorProcessTree.class);
    LOG.info("Using ResourceCalculatorProcessTree: {}", this.processTreeClass);

    // 读取容器指标配置
    this.containerMetricsEnabled =
        this.conf.getBoolean(YarnConfiguration.NM_CONTAINER_METRICS_ENABLE,
            YarnConfiguration.DEFAULT_NM_CONTAINER_METRICS_ENABLE);
    this.containerMetricsPeriodMs =
        this.conf.getLong(YarnConfiguration.NM_CONTAINER_METRICS_PERIOD_MS,
            YarnConfiguration.DEFAULT_NM_CONTAINER_METRICS_PERIOD_MS);
    this.containerMetricsUnregisterDelayMs = this.conf.getLong(
        YarnConfiguration.NM_CONTAINER_METRICS_UNREGISTER_DELAY_MS,
        YarnConfiguration.DEFAULT_NM_CONTAINER_METRICS_UNREGISTER_DELAY_MS);

    // 获取节点上分配给容器的总物理内存和vcore数
    long configuredPMemForContainers =
        NodeManagerHardwareUtils.getContainerMemoryMB(
            this.resourceCalculatorPlugin, this.conf);

    int configuredVCoresForContainers =
        NodeManagerHardwareUtils.getVCores(
            this.resourceCalculatorPlugin, this.conf);

    // 计算虚拟内存和物理内存比例，必须至少为1
    vmemRatio = this.conf.getFloat(YarnConfiguration.NM_VMEM_PMEM_RATIO,
        YarnConfiguration.DEFAULT_NM_VMEM_PMEM_RATIO);
    Preconditions.checkArgument(vmemRatio > 0.99f,
        YarnConfiguration.NM_VMEM_PMEM_RATIO + " should be at least 1.0");

    // 设置分配给容器的总资源，供UI展示使用
    Resource resourcesForContainers = Resource.newInstance(
        configuredPMemForContainers, configuredVCoresForContainers);
    setAllocatedResourcesForContainers(resourcesForContainers);

    // 读取各类内存检查开关配置
    pmemCheckEnabled = this.conf.getBoolean(
        YarnConfiguration.NM_PMEM_CHECK_ENABLED,
        YarnConfiguration.DEFAULT_NM_PMEM_CHECK_ENABLED);
    vmemCheckEnabled = this.conf.getBoolean(
        YarnConfiguration.NM_VMEM_CHECK_ENABLED,
        YarnConfiguration.DEFAULT_NM_VMEM_CHECK_ENABLED);
    elasticMemoryEnforcement = this.conf.getBoolean(
        YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_ENABLED,
        YarnConfiguration.DEFAULT_NM_ELASTIC_MEMORY_CONTROL_ENABLED);
    strictMemoryEnforcement = conf.getBoolean(
        YarnConfiguration.NM_MEMORY_RESOURCE_ENFORCED,
        YarnConfiguration.DEFAULT_NM_MEMORY_RESOURCE_ENFORCED);
    LOG.info("Physical memory check enabled: {}", pmemCheckEnabled);
    LOG.info("Virtual memory check enabled: {}", vmemCheckEnabled);
    LOG.info("Elastic memory control enabled: {}", elasticMemoryEnforcement);
    LOG.info("Strict memory control enabled: {}", strictMemoryEnforcement);

    // 如果启用弹性内存控制，初始化cgroup OOM监听线程
    if (elasticMemoryEnforcement) {
      if (!CGroupElasticMemoryController.isAvailable()) {
        throw new YarnException(
            "CGroup Elastic Memory controller enabled but " +
            "it is not available. Exiting.");
      } else {
        this.oomListenerThread = new CGroupElasticMemoryController(
            conf,
            context,
            ResourceHandlerModule.getCGroupsHandler(),
            pmemCheckEnabled,
            vmemCheckEnabled,
            pmemCheckEnabled ?
                maxPmemAllottedForContainers : maxVmemAllottedForContainers
        );
      }
    }

    // 判断容器监控是否启用
    containersMonitorEnabled =
        isContainerMonitorEnabled() && monitoringInterval > 0;
    LOG.info("ContainersMonitor enabled: {}", containersMonitorEnabled);

    // 判断日志监控是否启用
    logMonitorEnabled =
            conf.getBoolean(YarnConfiguration.NM_CONTAINER_LOG_MONITOR_ENABLED,
                    YarnConfiguration.DEFAULT_NM_CONTAINER_LOG_MONITOR_ENABLED);
    LOG.info("Container Log Monitor Enabled: "+ logMonitorEnabled);

    // 获取YARN可用CPU占比
    nodeCpuPercentageForYARN =
        NodeManagerHardwareUtils.getNodeCpuPercentage(this.conf);

    // 如果启用物理内存检查，检查配置是否合理
    if (pmemCheckEnabled) {
      long totalPhysicalMemoryOnNM = UNKNOWN_MEMORY_LIMIT;
      if (this.resourceCalculatorPlugin != null) {
        totalPhysicalMemoryOnNM = this.resourceCalculatorPlugin
            .getPhysicalMemorySize();
        if (totalPhysicalMemoryOnNM <= 0) {
          LOG.warn("NodeManager's totalPmem could not be calculated. "
              + "Setting it to {}", UNKNOWN_MEMORY_LIMIT);
          totalPhysicalMemoryOnNM = UNKNOWN_MEMORY_LIMIT;
        }
      }

      // 如果分配给容器的物理内存超过节点总内存80%，输出警告提示
      if (totalPhysicalMemoryOnNM != UNKNOWN_MEMORY_LIMIT &&
          this.maxPmemAllottedForContainers > totalPhysicalMemoryOnNM * 0.80f) {
        LOG.warn(
            "NodeManager configured with {} physical memory allocated to " +
            "containers, which is more than 80% of the total physical memory " +
            "available ({}). Thrashing might happen.",
            TraditionalBinaryPrefix.long2String(
                maxPmemAllottedForContainers, "B", 1),
            TraditionalBinaryPrefix.long2String(
                totalPhysicalMemoryOnNM, "B", 1));
      }
    }
    super.serviceInit(this.conf);
  }

  /**
   * 从配置读取容器监控总开关
   * @return 容器监控是否启用
   */
  private boolean isContainerMonitorEnabled() {
    return conf.getBoolean(YarnConfiguration.NM_CONTAINER_MONITOR_ENABLED,
        YarnConfiguration.DEFAULT_NM_CONTAINER_MONITOR_ENABLED);
  }

  /**
   * 获取适配当前系统的进程树计算器实例
   * @param pId 容器根进程ID
   * @return 进程树计算器实例
   */
  private ResourceCalculatorProcessTree
      getResourceCalculatorProcessTree(String pId) {
    return ResourceCalculatorProcessTree.
        getResourceCalculatorProcessTree(
            pId, processTreeClass, conf);
  }

  /**
   * 检查当前系统资源计算器是否可用
   * @return 资源计算器可用返回true，否则false
   */
  private boolean isResourceCalculatorAvailable() {
    if (resourceCalculatorPlugin == null) {
      LOG.info("ResourceCalculatorPlugin is unavailable on this system. "
          + "{} is disabled.", this.getClass().getName());
      return false;
    }
    if (getResourceCalculatorProcessTree("1") == null) {
      LOG.info("ResourceCalculatorProcessTree is unavailable on this system. "
          + "{} is disabled.", this.getClass().getName());
      return false;
    }
    return true;
  }

  @Override
  protected void serviceStart() throws Exception {
    if (containersMonitorEnabled) {
      this.monitoringThread.start();
    }
    if (oomListenerThread != null) {
      oomListenerThread.start();
    }
    if (logMonitorEnabled) {
      this.logMonitorThread.start();
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if (containersMonitorEnabled) {
      this.monitoringThread.interrupt();
      try {
        this.monitoringThread.join();
      } catch (InterruptedException e) {
        LOG.info("ContainersMonitorImpl monitoring thread interrupted");
      }
      if (this.oomListenerThread != null) {
        this.oomListenerThread.stopListening();
        try {
          this.oomListenerThread.join();
        } finally {
          this.oomListenerThread = null;
        }
      }
    }
    if (logMonitorEnabled) {
      this.logMonitorThread.interrupt();
      try {
        this.logMonitorThread.join();
      } catch (InterruptedException e) {
      }
    }
    super.serviceStop();
  }

  /**
   * 封装容器进程树的资源配额信息，存储容器ID、根进程ID和进程树实例
   */
  public static class ProcessTreeInfo {
    private ContainerId containerId;
    private String pid;
    private ResourceCalculatorProcessTree pTree;
    private long vmemLimit;
    private long pmemLimit;
    private int cpuVcores;

    /**
     * 构造进程树信息实例
     * @param containerId 容器ID
     * @param pid 根进程ID
     * @param pTree 进程树计算器实例
     * @param vmemLimit 虚拟内存配额（字节）
     * @param pmemLimit 物理内存配额（字节）
     * @param cpuVcores CPU vcore配额
     */
    public ProcessTreeInfo(ContainerId containerId, String pid,
        ResourceCalculatorProcessTree pTree, long vmemLimit, long pmemLimit,
        int cpuVcores) {
      this.containerId = containerId;
      this.pid = pid;
      this.pTree = pTree;
      this.vmemLimit = vmemLimit;
      this.pmemLimit = pmemLimit;
      this.cpuVcores = cpuVcores;
    }

    public ContainerId getContainerId() {
      return this.containerId;
    }

    public String getPID() {
      return this.pid;
    }

    public void setPid(String pid) {
      this.pid = pid;
    }

    ResourceCalculatorProcessTree getProcessTree() {
      return this.pTree;
    }

    void setProcessTree(ResourceCalculatorProcessTree mypTree) {
      this.pTree = mypTree;
    }

    /**
     * @return Virtual memory limit for the process tree in bytes
     */
    public synchronized long getVmemLimit() {
      return this.vmemLimit;
    }

    /**
     * @return Physical memory limit for the process tree in bytes
     */
    public synchronized long getPmemLimit() {
      return this.pmemLimit;