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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * cgroups内存资源处理器抽象基类，定义了容器内存资源限制管理的通用流程，
 * 具体的cgroups内存配置操作由子类实现。
 * 用于YARN NodeManager端，基于Linux cgroups对容器内存资源进行管控。
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public abstract class AbstractCGroupsMemoryResourceHandler implements MemoryResourceHandler {

  static final Logger LOG =
      LoggerFactory.getLogger(CGroupsMemoryResourceHandlerImpl.class);
  protected static final CGroupsHandler.CGroupController MEMORY =
      CGroupsHandler.CGroupController.MEMORY;

  private CGroupsHandler cGroupsHandler;

  // 机会容器内存软限制固定为0
  protected static final int OPPORTUNISTIC_SOFT_LIMIT = 0;
  // 内存软限制比例系数，取值范围0~1，基于容器内存大小计算
  private float softLimit = 0.0f;
  // 是否开启内存资源强制限制
  private boolean enforce = true;

  /**
   * 构造函数，初始化cgroups处理器依赖。
   * @param cGroupsHandler cgroups处理器实例
   */
  public AbstractCGroupsMemoryResourceHandler(CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
  }

  /**
   * 获取当前使用的cgroups处理器实例。
   * @return cgroups处理器实例
   */
  protected CGroupsHandler getCGroupsHandler() {
    return cGroupsHandler;
  }

  @Override
  /**
   * 初始化内存cgroup控制器，加载配置参数。
   * @param conf YARN配置对象
   * @return 特权操作列表，返回null表示无额外操作
   * @throws ResourceHandlerException 配置参数非法时抛出异常
   */
  public List<PrivilegedOperation> bootstrap(Configuration conf)
      throws ResourceHandlerException {
    // 初始化内存cgroup控制器
    this.cGroupsHandler.initializeCGroupController(MEMORY);
    // 加载是否强制限制内存的配置
    enforce = conf.getBoolean(
        YarnConfiguration.NM_MEMORY_RESOURCE_ENFORCED,
        YarnConfiguration.DEFAULT_NM_MEMORY_RESOURCE_ENFORCED);
    // 加载内存软限制百分比配置
    float softLimitPerc = conf.getFloat(
        YarnConfiguration.NM_MEMORY_RESOURCE_CGROUPS_SOFT_LIMIT_PERCENTAGE,
        YarnConfiguration.
            DEFAULT_NM_MEMORY_RESOURCE_CGROUPS_SOFT_LIMIT_PERCENTAGE);
    // 转换为0~1的比例系数
    softLimit = softLimitPerc / 100.0f;
    // 参数合法性校验
    if (softLimitPerc < 0.0f || softLimitPerc > 100.0f) {
      throw new ResourceHandlerException(
          "Illegal value '" + softLimitPerc + "' "
              + YarnConfiguration.
              NM_MEMORY_RESOURCE_CGROUPS_SOFT_LIMIT_PERCENTAGE
              + ". Value must be between 0 and 100.");
    }
    return null;
  }

  @Override
  /**
   * 更新已存在容器的内存限制配置。
   * @param container 目标容器对象
   * @return 特权操作列表，返回null表示无额外操作
   * @throws ResourceHandlerException 更新失败时抛出异常
   */
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    String cgroupId = container.getContainerId().toString();
    // 获取容器对应内存cgroup目录
    File cgroup = new File(cGroupsHandler.getPathForCGroup(MEMORY, cgroupId));
    // 仅处理已存在的cgroup
    if (cgroup.exists()) {
      // 计算容器内存软限制：容器总内存 * 软限制比例，单位MB
      long containerSoftLimit =
          (long) (container.getResource().getMemorySize() * this.softLimit);
      // 容器内存硬限制等于容器申请的总内存大小，单位MB
      long containerHardLimit = container.getResource().getMemorySize();
      // 如果开启强制限制，更新内存限制
      if (enforce) {
        try {
          // 更新内存硬限制
          updateMemoryHardLimit(cgroupId, containerHardLimit);
          // 获取容器令牌标识，判断容器执行类型
          ContainerTokenIdentifier id = container.getContainerTokenIdentifier();
          if (id != null && id.getExecutionType() ==
              ExecutionType.OPPORTUNISTIC) {
            // 机会容器更新特殊内存限制
            updateOpportunisticMemoryLimits(cgroupId);
          } else {
            // 保障容器更新常规软限制
            updateGuaranteedMemoryLimits(cgroupId, containerSoftLimit);
          }
        } catch (ResourceHandlerException re) {
          // 更新失败清理cgroup，抛出异常
          cGroupsHandler.deleteCGroup(MEMORY, cgroupId);
          LOG.warn("Could not update cgroup for container", re);
          throw re;
        }
      }
    }
    return null;
  }

  /**
   * 抽象方法：更新容器内存硬限制，由具体子类实现不同版本cgroups的操作。
   * @param cgroupId 容器对应的cgroup id
   * @param containerHardLimit 容器内存硬限制，单位MB
   * @throws ResourceHandlerException 更新失败时抛出异常
   */
  protected abstract void updateMemoryHardLimit(String cgroupId, long containerHardLimit)
      throws ResourceHandlerException;

  /**
   * 抽象方法：更新机会容器的内存限制，由具体子类实现。
   * @param cgroupId 容器对应的cgroup id
   * @throws ResourceHandlerException 更新失败时抛出异常
   */
  protected abstract void updateOpportunisticMemoryLimits(String cgroupId)
      throws ResourceHandlerException;

  /**
   * 抽象方法：更新保障容器的内存软限制，由具体子类实现。
   * @param cgroupId 容器对应的cgroup id
   * @param containerSoftLimit 容器内存软限制，单位MB
   * @throws ResourceHandlerException 更新失败时抛出异常
   */
  protected abstract void updateGuaranteedMemoryLimits(String cgroupId, long containerSoftLimit)
      throws ResourceHandlerException;

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  /**
   * 容器启动前准备，创建内存cgroup并添加容器PID到cgroup。
   * @param container 即将启动的容器
   * @return 需要执行的特权操作列表，包含添加PID到cgroup的操作
   * @throws ResourceHandlerException 创建或更新cgroup失败时抛出异常
   */
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String cgroupId = container.getContainerId().toString();
    // 创建容器对应的内存cgroup
    cGroupsHandler.createCGroup(MEMORY, cgroupId);
    // 更新容器内存限制配置
    updateContainer(container);
    // 构建返回的特权操作列表
    List<PrivilegedOperation> ret = new ArrayList<>();
    // 添加将容器PID加入cgroup的特权操作
    ret.add(new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        PrivilegedOperation.CGROUP_ARG_PREFIX
            + cGroupsHandler.getPathForCGroupTasks(MEMORY, cgroupId)));
    return ret;
  }

  @Override
  /**
   * 容器完成后清理，删除容器对应的内存cgroup。
   * @param containerId 已完成的容器ID
   * @return 特权操作列表，返回null表示无额外操作
   * @throws ResourceHandlerException 删除失败时抛出异常
   */
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    cGroupsHandler.deleteCGroup(MEMORY, containerId.toString());
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown() throws ResourceHandlerException {
    return null;
  }

  @Override
  public String toString() {
    return AbstractCGroupsMemoryResourceHandler.class.getName();
  }
}