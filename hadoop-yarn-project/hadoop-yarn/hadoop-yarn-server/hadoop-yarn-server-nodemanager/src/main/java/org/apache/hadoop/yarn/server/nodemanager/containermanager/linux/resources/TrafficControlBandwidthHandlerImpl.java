// 这个文件已经全部加上中文注释
/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 使用Linux流量控制(tc)实现容器出站带宽限流的处理器
 * 依赖cgroups net_cls子系统标记容器流量，配合tc规则实现带宽隔离
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TrafficControlBandwidthHandlerImpl
    implements OutboundBandwidthResourceHandler {

  private static final Logger LOG =
       LoggerFactory.getLogger(TrafficControlBandwidthHandlerImpl.class);
  // 在RM未支持带宽资源调度前，使用最大容器数倒推单个容器保障带宽，该参数后续会移除
  private static final int MAX_CONTAINER_COUNT = 50;

  private final PrivilegedOperationExecutor privilegedOperationExecutor;
  private final CGroupsHandler cGroupsHandler;
  private final TrafficController trafficController;
  // 容器ID到net_cls类ID的映射表，用于清理和指标统计
  private final ConcurrentHashMap<ContainerId, Integer> containerIdClassIdMap;

  private Configuration conf;
  // 需要限流的网络设备名称
  private String device;
  // 是否开启严格限流模式，严格模式下容器带宽不超过软限制，否则允许抢占空闲带宽
  private boolean strictMode;
  // 单个容器出站带宽软限制（单位：Mbit/s）
  private int containerBandwidthMbit;
  // 根Qdisc总带宽限制（物理网卡总带宽，单位：Mbit/s）
  private int rootBandwidthMbit;
  // YARN整体可用出站带宽限制（单位：Mbit/s）
  private int yarnBandwidthMbit;

  /**
   * 构造流量控制带宽处理器
   * @param privilegedOperationExecutor 特权操作执行器，用于执行需要root权限的操作
   * @param cGroupsHandler cgroups处理器，用于操作net_cls cgroup
   * @param trafficController 流量控制器，封装tc规则操作
   */
  public TrafficControlBandwidthHandlerImpl(PrivilegedOperationExecutor
      privilegedOperationExecutor, CGroupsHandler cGroupsHandler,
      TrafficController trafficController) {
    this.privilegedOperationExecutor = privilegedOperationExecutor;
    this.cGroupsHandler = cGroupsHandler;
    this.trafficController = trafficController;
    this.containerIdClassIdMap = new ConcurrentHashMap<>();
  }

  /**
   * Bootstrapping 'outbound-bandwidth' resource handler - mounts net_cls
   * controller and bootstraps a traffic control bandwidth shaping hierarchy
   * @param configuration yarn configuration in use
   * @return (potentially empty) list of privileged operations to execute.
   * @throws ResourceHandlerException
   */

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    conf = configuration;
    // 初始化net_cls cgroup控制器，一次性操作
    cGroupsHandler
        .initializeCGroupController(CGroupsHandler.CGroupController.NET_CLS);
    // 读取需要限流的网络设备配置
    device = conf.get(YarnConfiguration.NM_NETWORK_RESOURCE_INTERFACE,
        YarnConfiguration.DEFAULT_NM_NETWORK_RESOURCE_INTERFACE);
    // 读取严格限流模式配置
    strictMode = configuration.getBoolean(YarnConfiguration
        .NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE, YarnConfiguration
        .DEFAULT_NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE);
    // 读取根带宽总限制配置
    rootBandwidthMbit = conf.getInt(YarnConfiguration
        .NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_MBIT, YarnConfiguration
        .DEFAULT_NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_MBIT);
    // 读取YARN可用带宽限制配置，未配置则使用根带宽
    yarnBandwidthMbit = conf.getInt(YarnConfiguration
        .NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_YARN_MBIT, rootBandwidthMbit);
    // 计算单个容器默认带宽限制：YARN总带宽 / 最大容器数，向上取整
    containerBandwidthMbit = (int) Math.ceil((double) yarnBandwidthMbit /
        MAX_CONTAINER_COUNT);

    StringBuilder logLine = new StringBuilder("strict mode is set to :")
        .append(strictMode).append(System.lineSeparator());

    if (strictMode) {
      logLine.append("container bandwidth will be capped to soft limit.")
          .append(System.lineSeparator());
    } else {
      logLine.append(
          "containers will be allowed to use spare YARN bandwidth.")
          .append(System.lineSeparator());
    }

    logLine
        .append("containerBandwidthMbit soft limit (in mbit/sec) is set to : ")
        .append(containerBandwidthMbit);

    LOG.info(logLine.toString());
    // 初始化tc根队列和YARN带宽分类层次
    trafficController.bootstrap(device, rootBandwidthMbit, yarnBandwidthMbit);

    return null;
  }

  /**
   * Pre-start hook for 'outbound-bandwidth' resource. A cgroup is created
   * and a net_cls classid is generated and written to a cgroup file. A
   * traffic control shaping rule is created in order to limit outbound
   * bandwidth utilization.
   * @param container Container being launched
   * @return privileged operations for some cgroups/tc operations.
   * @throws ResourceHandlerException
   */
  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String containerIdStr = container.getContainerId().toString();
    // 分配唯一的net_cls类ID
    int classId = trafficController.getNextClassId();
    // 转换为net_cls要求的格式字符串
    String classIdStr = trafficController.getStringForNetClsClassId(classId);

    // 为当前容器创建net_cls cgroup
    cGroupsHandler.createCGroup(CGroupsHandler.CGroupController
            .NET_CLS,
        containerIdStr);
    // 将分配好的类ID写入net_cls cgroup的classid文件
    cGroupsHandler.updateCGroupParam(CGroupsHandler.CGroupController.NET_CLS,
        containerIdStr, CGroupsHandler.CGROUP_PARAM_CLASSID, classIdStr);
    // 保存容器ID到类ID的映射
    containerIdClassIdMap.put(container.getContainerId(), classId);

    // 构造将容器PID添加到cgroup tasks文件的特权操作
    // 该操作需要特权权限，只能在容器启动时由特权执行器完成
    String tasksFile = cGroupsHandler.getPathForCGroupTasks(
        CGroupsHandler.CGroupController.NET_CLS, containerIdStr);
    String opArg = new StringBuilder(PrivilegedOperation.CGROUP_ARG_PREFIX)
        .append(tasksFile).toString();
    List<PrivilegedOperation> ops = new ArrayList<>();

    ops.add(new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP, opArg));

    // 构造为容器创建tc限流规则的批量操作
    // 返回给容器执行器批量执行，减少fork/exec次数优化性能
    TrafficController.BatchBuilder builder = trafficController.new
        BatchBuilder(PrivilegedOperation.OperationType.TC_MODIFY_STATE);

    builder.addContainerClass(classId, containerBandwidthMbit, strictMode);
    ops.add(builder.commitBatchToTempFile());

    return ops;
  }

  /**
   * Reacquires state for a container - reads the classid from the cgroup
   * being used for the container being reacquired
   * @param containerId id of the container being reacquired.
   * @return (potentially empty) list of privileged operations
   * @throws ResourceHandlerException
   */

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    String containerIdStr = containerId.toString();

    LOG.debug("Attempting to reacquire classId for container: {}",
        containerIdStr);

    // 从已存在的cgroup中读取classid
    String classIdStrFromFile = cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.NET_CLS, containerIdStr,
        CGroupsHandler.CGROUP_PARAM_CLASSID);
    int classId = trafficController
        .getClassIdFromFileContents(classIdStrFromFile);

    LOG.info("Reacquired containerId -> classId mapping: " + containerIdStr
        + " -> " + classId);
    // 重新保存映射关系
    containerIdClassIdMap.put(containerId, classId);

    return null;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    return null;
  }

  /**
   * Returns total bytes sent per container to be used for metrics tracking
   * purposes.
   * @return a map of containerId to bytes sent
   * @throws ResourceHandlerException
   */
  public Map<ContainerId, Integer> getBytesSentPerContainer()
      throws ResourceHandlerException {
    // 从tc读取每个类ID的发送字节统计
    Map<Integer, Integer> classIdStats = trafficController.readStats();
    Map<ContainerId, Integer> containerIdStats = new HashMap<>();

    // 转换为容器ID到发送字节的映射，供监控指标使用
    for (Map.Entry<ContainerId, Integer> entry : containerIdClassIdMap
        .entrySet()) {
      ContainerId containerId = entry.getKey();
      Integer classId = entry.getValue();
      Integer bytesSent = classIdStats.get(classId);

      if (bytesSent == null) {
        LOG.warn("No bytes sent metric found for container: " + containerId +
            " with classId: " + classId);
        continue;
      }
      containerIdStats.put(containerId, bytesSent);
    }

    return containerIdStats;
  }

  /**
   * Cleanup operations once container is completed - deletes cgroup and
   * removes traffic shaping rule(s).
   * @param containerId of the container that was completed.
   * @return null
   * @throws ResourceHandlerException
   */
  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    LOG.info("postComplete for container: " + containerId.toString());
    // 删除容器对应的net_cls cgroup
    cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.NET_CLS,
        containerId.toString());

    Integer classId = containerIdClassIdMap.get(containerId);

    if (classId != null) {
      // 构造删除容器tc限流规则的操作
      PrivilegedOperation op = trafficController.new
          BatchBuilder(PrivilegedOperation.OperationType.TC_MODIFY_STATE)
          .deleteContainerClass(classId).commitBatchToTempFile();

      try {
        // 执行删除操作
        privilegedOperationExecutor.executePrivilegedOperation(op, false);
        // 释放classId供后续容器复用
        trafficController.releaseClassId(classId);
      } catch (PrivilegedOperationException e) {
        LOG.warn("Failed to delete tc rule for classId: " + classId);
        throw new ResourceHandlerException(
            "Failed to delete tc rule for classId:" + classId);
      }
    } else {
      LOG.warn("Not cleaning up tc rules. classId unknown for container: " +
          containerId.toString());
    }

    // 移除映射关系
    containerIdClassIdMap.remove(containerId);
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown()
      throws ResourceHandlerException {
    LOG.debug("teardown(): Nothing to do");

    return null;
  }

  @Override
  public String toString() {
    return TrafficControlBandwidthHandlerImpl.class.getName();
  }
}