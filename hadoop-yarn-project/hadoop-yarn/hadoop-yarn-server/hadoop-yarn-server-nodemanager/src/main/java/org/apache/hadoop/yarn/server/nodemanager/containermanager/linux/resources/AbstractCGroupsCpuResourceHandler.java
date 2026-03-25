// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
资源org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Linux CGroups CPU资源处理器抽象基类，实现CGroups CPU管控的通用逻辑，
 * 不同CGroups版本（v1/v2）通过子类实现差异部分
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public abstract class AbstractCGroupsCpuResourceHandler implements CpuResourceHandler {

  static final Logger LOG =
       LoggerFactory.getLogger(AbstractCGroupsCpuResourceHandler.class);

  // CGroups处理器实例，用于操作CGroups文件系统
  protected CGroupsHandler cGroupsHandler;
  // 是否开启严格资源使用限制模式
  private boolean strictResourceUsageMode = false;
  // YARN可使用的CPU核心总数
  private float yarnProcessors;
  // 当前节点总的vCore数量
  private int nodeVCores;
  // CPU CGroups控制器
  private static final CGroupsHandler.CGroupController CPU =
      CGroupsHandler.CGroupController.CPU;

  @VisibleForTesting
  // CPU最大配额值，单位微秒
  static final int MAX_QUOTA_US = 1000 * 1000;
  @VisibleForTesting
  // CPU最小周期值，单位微秒，符合CGroups要求
  static final int MIN_PERIOD_US = 1000;

  /**
   * 构造方法，传入CGroups处理器实例
   * @param cGroupsHandler CGroups处理器
   */
  AbstractCGroupsCpuResourceHandler(CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration conf)
      throws ResourceHandlerException {
    return bootstrap(
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf), conf);
  }

  @VisibleForTesting
  /**
   * 初始化CPU资源 handler，完成CGroups控制器初始化和全局CPU限额设置
   * @param plugin 资源计算插件
   * @param conf YARN配置
   * @return 特权操作列表
   * @throws ResourceHandlerException 初始化异常
   */
  List<PrivilegedOperation> bootstrap(
      ResourceCalculatorPlugin plugin, Configuration conf)
      throws ResourceHandlerException {
    // 从配置读取是否开启严格资源限制模式
    this.strictResourceUsageMode = conf.getBoolean(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE,
        YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE);
    // 初始化CPU CGroups控制器
    this.cGroupsHandler.initializeCGroupController(CPU);
    // 获取节点总vCore数量
    nodeVCores = NodeManagerHardwareUtils.getVCores(plugin, conf);

    // 获取YARN分配给容器的CPU核心总数，限制YARN整体CPU使用
    yarnProcessors = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    // 获取节点物理CPU核心总数
    int systemProcessors = NodeManagerHardwareUtils.getNodeCPUs(plugin, conf);
    boolean existingCpuLimits;
    // 检查根cgroup是否已经存在CPU限制
    existingCpuLimits = cpuLimitExists(
        cGroupsHandler.getPathForCGroup(CPU, ""));

    // 如果YARN分配的CPU总数不等于节点总核心数，需要设置全局限额
    if (systemProcessors != (int) yarnProcessors) {
      LOG.info("YARN containers restricted to " + yarnProcessors + " cores");
      // 计算CFS配额和周期
      int[] limits = getOverallLimits(yarnProcessors);
      // 更新根cgroup的CPU最大限制
      updateCgroupMaxCpuLimit("", String.valueOf(limits[1]), String.valueOf(limits[0]));
    // 如果YARN用满所有核心但已有CPU限制，移除限制
    } else if (existingCpuLimits) {
      LOG.info("Removing CPU constraints for YARN containers.");
      updateCgroupMaxCpuLimit("", String.valueOf(-1), null);
    }
    return null;
  }

  /**
   * 更新cgroup的CPU最大配额限制，子类实现
   * @param cgroupId cgroup ID
   * @param quota CPU配额
   * @param period CPU周期
   * @throws ResourceHandlerException 更新异常
   */
  protected abstract void updateCgroupMaxCpuLimit(String cgroupId, String quota, String period)
      throws ResourceHandlerException;

  /**
   * 检查指定路径下是否已经存在CPU限制，子类实现
   * @param path cgroup路径
   * @return 是否存在CPU限制
   * @throws ResourceHandlerException 检查异常
   */
  protected abstract boolean cpuLimitExists(String path) throws ResourceHandlerException;


  @VisibleForTesting
  @InterfaceAudience.Private
  /**
   * 根据YARN可使用CPU总数计算CFS配额和周期参数
   * @param yarnProcessors YARN可使用CPU核心数
   * @return 返回[periodUS, quotaUS]即[周期微秒, 配额微秒]
   */
  public static int[] getOverallLimits(float yarnProcessors) {

    int[] ret = new int[2];

    if (yarnProcessors < 0.01f) {
      throw new IllegalArgumentException("Number of processors can't be <= 0.");
    }

    int quotaUS = MAX_QUOTA_US;
    int periodUS = (int) (MAX_QUOTA_US / yarnProcessors);
    // CPU总数小于1核心时的特殊处理
    if (yarnProcessors < 1.0f) {
      periodUS = MAX_QUOTA_US;
      quotaUS = (int) (periodUS * yarnProcessors);
      // 配额小于最小值时修正为最小值
      if (quotaUS < MIN_PERIOD_US) {
        LOG.warn("The quota calculated for the cgroup was too low."
            + " The minimum value is " + MIN_PERIOD_US
            + ", calculated value is " + quotaUS
            + ". Setting quota to minimum value.");
        quotaUS = MIN_PERIOD_US;
      }
    }

    // CFS周期不能小于1000微秒，不满足则使用不限制模式
    if (periodUS < MIN_PERIOD_US) {
      LOG.warn("The period calculated for the cgroup was too low."
          + " The minimum value is " + MIN_PERIOD_US
          + ", calculated value is " + periodUS
          + ". Using all available CPU.");
      periodUS = MAX_QUOTA_US;
      quotaUS = -1;
    }

    ret[0] = periodUS;
    ret[1] = quotaUS;
    return ret;
  }

  @Override
  /**
   * 容器启动前准备，创建容器cgroup并更新配置
   * @param container 待启动容器
   * @return 需要执行的特权操作列表，包含将容器PID加入cgroup的操作
   * @throws ResourceHandlerException 操作异常
   */
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String cgroupId = container.getContainerId().toString();
    // 为容器创建CPU cgroup
    cGroupsHandler.createCGroup(CPU, cgroupId);
    // 更新容器CPU配置
    updateContainer(container);
    List<PrivilegedOperation> ret = new ArrayList<>();
    // 添加将容器PID加入cgroup的特权操作
    ret.add(new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupsHandler
            .getPathForCGroupTasks(CPU, cgroupId)));
    return ret;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  /**
   * 更新容器CPU资源配置，根据容器vCore数量和执行类型设置CPU权重和配额
   * @param container 需要更新的容器
   * @return 空列表，无需额外特权操作
   * @throws ResourceHandlerException 更新异常
   */
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    Resource containerResource = container.getResource();
    String cgroupId = container.getContainerId().toString();
    File cgroup = new File(cGroupsHandler.getPathForCGroup(CPU, cgroupId));
    // 只处理存在的cgroup
    if (cgroup.exists()) {
      try {
        int containerVCores = containerResource.getVirtualCores();
        ContainerTokenIdentifier id = container.getContainerTokenIdentifier();
        // 机会容器（空闲资源容器）设置专门的CPU权重
        if (id != null && id.getExecutionType() ==
            ExecutionType.OPPORTUNISTIC) {
          updateCgroupCpuWeight(cgroupId, getOpportunisticCpuWeight());
        // 普通容器根据vCore数量设置CPU权重
        } else {
          updateCgroupCpuWeight(cgroupId, getCpuWeightByContainerVcores(containerVCores));
        }
        // 严格模式下需要设置CPU配额限制
        if (strictResourceUsageMode) {
          if (nodeVCores != containerVCores) {
            // 按比例计算容器可使用的CPU核心数
            float containerCPU =
                (containerVCores * yarnProcessors) / (float) nodeVCores;
            // 计算CFS配额参数
            int[] limits = getOverallLimits(containerCPU);
            // 更新容器cgroup的CPU限额
            updateCgroupMaxCpuLimit(cgroupId, String.valueOf(limits[1]), String.valueOf(limits[0]));
          }
        }
      } catch (ResourceHandlerException re) {
        // 更新失败删除cgroup并抛出异常
        cGroupsHandler.deleteCGroup(CPU, cgroupId);
        LOG.warn("Could not update cgroup for container", re);
        throw re;
      }
    }
    return null;
  }

  /**
   * 获取机会容器的CPU权重，子类实现
   * @return CPU权重值
   */
  protected abstract int getOpportunisticCpuWeight();

  /**
   * 根据容器vCore数量计算CPU权重，子类实现
   * @param containerVcores 容器vCore数量
   * @return CPU权重值
   */
  protected abstract int getCpuWeightByContainerVcores(int containerVcores);

  /**
   * 更新cgroup的CPU权重，子类实现
   * @param cgroupId cgroup ID
   * @param weight CPU权重值
   * @throws ResourceHandlerException 更新异常
   */
  protected abstract void updateCgroupCpuWeight(String cgroupId, int weight)
      throws ResourceHandlerException;

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    // 容器完成后删除对应cgroup
    cGroupsHandler.deleteCGroup(CPU, containerId.toString());
    return null;
  }

  @Override public List<PrivilegedOperation> teardown()
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public String toString() {
    return AbstractCGroupsCpuResourceHandler.class.getName();
  }
}