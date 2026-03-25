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

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuNodeResourceUpdateHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 节点资源监控器实现类，定期监控节点资源利用率并上报给NodeManager。
 */
public class NodeResourceMonitorImpl extends AbstractService implements
    NodeResourceMonitor {

  final static Logger LOG =
       LoggerFactory.getLogger(NodeResourceMonitorImpl.class);

  /** 节点资源利用率监控间隔（毫秒） */
  private long monitoringInterval;
  /** 资源监控后台线程 */
  private MonitoringThread monitoringThread;

  /** 资源计算器插件，用于获取系统资源使用情况 */
  private ResourceCalculatorPlugin resourceCalculatorPlugin;

  /** GPU资源插件实例 */
  private GpuResourcePlugin gpuResourcePlugin;
  /** GPU资源信息更新处理器 */
  private GpuNodeResourceUpdateHandler gpuNodeResourceUpdateHandler;

  /** 自定义资源（如GPU）的使用率信息 */
  private Map<String, Float> customResources = new HashMap<>();

  /** 当前节点的总资源利用率快照 */
  private ResourceUtilization nodeUtilization =
      ResourceUtilization.newInstance(0, 0, 0f, customResources);
  /** NodeManager上下文，持有节点全局信息 */
  private Context nmContext;

  /**
   * 构造节点资源监控器，绑定到NodeManager上下文。
   * @param context NodeManager上下文对象
   */
  public NodeResourceMonitorImpl(Context context) {
    super(NodeResourceMonitorImpl.class.getName());
    this.nmContext = context;
    this.monitoringThread = new MonitoringThread();
  }

  /**
   * 初始化监控服务，加载配置和插件。
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置读取监控间隔
    this.monitoringInterval =
        conf.getLong(YarnConfiguration.NM_RESOURCE_MON_INTERVAL_MS,
            YarnConfiguration.DEFAULT_NM_RESOURCE_MON_INTERVAL_MS);

    // 获取节点资源监控插件实例
    this.resourceCalculatorPlugin =
        ResourceCalculatorPlugin.getNodeResourceMonitorPlugin(conf);

    // 尝试获取并初始化GPU资源处理器
    if (nmContext.getResourcePluginManager() != null) {
      this.gpuResourcePlugin =
          (GpuResourcePlugin)nmContext.getResourcePluginManager().
          getNameToPlugins().get(ResourceInformation.GPU_URI);

      if (gpuResourcePlugin != null) {
        this.gpuNodeResourceUpdateHandler =
            (GpuNodeResourceUpdateHandler)gpuResourcePlugin.
                getNodeResourceHandlerInstance();
      }
    }

    LOG.info(" Using ResourceCalculatorPlugin : "
        + this.resourceCalculatorPlugin);
    super.serviceInit(conf);
  }

  /**
   * 检查监控功能是否启用。
   * @return true 表示监控可用，false表示禁用
   */
  private boolean isEnabled() {
    if (this.monitoringInterval <= 0) {
      LOG.info("Node Resource monitoring interval is <=0. "
          + this.getClass().getName() + " is disabled.");
      return false;
    }
    if (resourceCalculatorPlugin == null) {
      LOG.info("ResourceCalculatorPlugin is unavailable on this system. "
          + this.getClass().getName() + " is disabled.");
      return false;
    }
    return true;
  }

  /**
   * 启动监控服务，开启后台监控线程。
   */
  @Override
  protected void serviceStart() throws Exception {
    if (this.isEnabled()) {
      this.monitoringThread.start();
    }
    super.serviceStart();
  }

  /**
   * 停止监控服务，终止后台线程。
   */
  @Override
  protected void serviceStop() throws Exception {
    if (this.isEnabled()) {
      this.monitoringThread.interrupt();
      try {
        // 等待线程终止，最多等待10秒
        this.monitoringThread.join(10 * 1000);
      } catch (InterruptedException e) {
        LOG.warn("Could not wait for the thread to join");
      }
    }
    super.serviceStop();
  }

  /**
   * 后台监控线程，周期性采集节点资源利用率信息。
   */
  private class MonitoringThread extends SubjectInheritingThread {
    /**
     * 初始化监控线程，设置为守护线程。
     */
    public MonitoringThread() {
      super("Node Resource Monitor");
      this.setDaemon(true);
    }

    /**
     * 周期性执行监控主循环，定期采集节点资源使用数据。
     */
    @Override
    public void work() {
      while (true) {
        // 计算已用物理内存 = 总物理内存 - 可用物理内存
        long pmem = resourceCalculatorPlugin.getPhysicalMemorySize() -
            resourceCalculatorPlugin.getAvailablePhysicalMemorySize();
        // 计算已用虚拟内存 = 总虚拟内存 - 可用虚拟内存
        long vmem =
            resourceCalculatorPlugin.getVirtualMemorySize()
                - resourceCalculatorPlugin.getAvailableVirtualMemorySize();
        // 获取已用CPU核数利用率
        float vcores = resourceCalculatorPlugin.getNumVCoresUsed();

        // 初始化总GPU利用率
        float totalNodeGpuUtilization = 0F;
        try {
          // 如果GPU处理器存在，获取当前节点总GPU利用率
          if (gpuNodeResourceUpdateHandler != null) {
            totalNodeGpuUtilization =
                gpuNodeResourceUpdateHandler.getTotalNodeGpuUtilization();
          }
        } catch (Exception e) {
          LOG.error("Get Node GPU Utilization error: " + e);
        }

        // 更新GPU利用率到自定义资源映射
        customResources.
            put(ResourceInformation.GPU_URI, totalNodeGpuUtilization);
        // 更新节点总利用率，转换字节单位为MB，生成新的利用率快照
        nodeUtilization =
            ResourceUtilization.newInstance(
                (int) (pmem >> 20), // B -> MB
                (int) (vmem >> 20), // B -> MB
                vcores,     // Used Virtual Cores
                customResources);  // Used GPUs

        // 将节点利用率数据上报到NodeManager指标系统
        NodeManagerMetrics nmMetrics = nmContext.getNodeManagerMetrics();
        if (nmMetrics != null) {
          nmMetrics.setNodeUsedMemGB(nodeUtilization.getPhysicalMemory());
          nmMetrics.setNodeUsedVMemGB(nodeUtilization.getVirtualMemory());
          nmMetrics.setNodeCpuUtilization(nodeUtilization.getCPU());
          nmMetrics.setNodeGpuUtilization(totalNodeGpuUtilization);
        }

        try {
          // 睡眠到下一次监控周期
          Thread.sleep(monitoringInterval);
        } catch (InterruptedException e) {
          LOG.warn(NodeResourceMonitorImpl.class.getName()
              + " is interrupted. Exiting.");
          break;
        }
      }
    }
  }

  /**
   * 获取当前节点的最新资源利用率快照。
   * @return 节点资源利用率对象
   */
  @Override
  public ResourceUtilization getUtilization() {
    return this.nodeUtilization;
  }
}