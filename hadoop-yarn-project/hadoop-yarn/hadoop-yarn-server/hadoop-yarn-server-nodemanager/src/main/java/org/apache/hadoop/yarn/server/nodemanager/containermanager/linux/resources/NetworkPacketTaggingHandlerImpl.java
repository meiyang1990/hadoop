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
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;

/**
 * YARN NodeManager 网络数据包标签处理器实现，基于 Linux cgroups net_cls 控制器实现容器网络流量标记，支持后续流量限流、QoS管控。
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NetworkPacketTaggingHandlerImpl
    implements ResourceHandler {

  private static final Logger LOG =
       LoggerFactory.getLogger(NetworkPacketTaggingHandlerImpl.class);

  private final CGroupsHandler cGroupsHandler;

  private Configuration conf;
  private NetworkTagMappingManager tagMappingManager;

  /**
   * 构造网络数据包标签处理器。
   * @param privilegedOperationExecutor 特权操作执行器
   * @param cGroupsHandler cgroups处理器
   */
  public NetworkPacketTaggingHandlerImpl(
      PrivilegedOperationExecutor privilegedOperationExecutor,
      CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
  }

  /**
   * 启动初始化网络数据包标签处理器，挂载 net_cls cgroup控制器，初始化标签映射管理器。
   * @param configuration YARN配置对象
   * @return 需要执行的特权操作列表，返回null表示无额外操作
   * @throws ResourceHandlerException 资源处理异常
   */
  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    conf = configuration;

    // 初始化net_cls cgroup控制器
    cGroupsHandler
        .initializeCGroupController(CGroupsHandler.CGroupController.NET_CLS);

    // 创建并初始化网络标签映射管理器
    this.tagMappingManager = createNetworkTagMappingManager(conf);
    this.tagMappingManager.initialize(conf);
    return null;
  }

  /**
   * 容器启动前预处理，为容器创建独立net_cls cgroup，分配并写入网络标签，添加容器进程ID到cgroup任务文件。
   *
   * @param container 待启动的容器对象
   * @return 需要执行的特权操作列表
   * @throws ResourceHandlerException 资源处理异常
   */
  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String containerIdStr = container.getContainerId().toString();
    // 为容器分配十六进制格式的网络标签ID
    String classIdStr = this.tagMappingManager.getNetworkTagHexID(
        container);

    // 为容器创建独立的net_cls cgroup
    cGroupsHandler.createCGroup(CGroupsHandler.CGroupController
            .NET_CLS, containerIdStr);
    // 将分配的标签ID写入cgroup配置文件
    cGroupsHandler.updateCGroupParam(CGroupsHandler.CGroupController.NET_CLS,
        containerIdStr, CGroupsHandler.CGROUP_PARAM_CLASSID, classIdStr);

    // 构造将容器根进程ID写入cgroup tasks文件的特权操作，该操作需要特权权限执行
    String tasksFile = cGroupsHandler.getPathForCGroupTasks(
        CGroupsHandler.CGroupController.NET_CLS, containerIdStr);
    String opArg = new StringBuilder(PrivilegedOperation.CGROUP_ARG_PREFIX)
        .append(tasksFile).toString();
    List<PrivilegedOperation> ops = new ArrayList<>();

    ops.add(new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP, opArg));

    return ops;
  }

  /**
   * 重新获取已存在容器状态，当前不需要额外操作。
   * @param containerId 容器ID
   * @return 需要执行的特权操作列表，返回null表示无操作
   * @throws ResourceHandlerException 资源处理异常
   */
  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    return null;
  }

  /**
   * 容器完成后清理，删除容器对应的net_cls cgroup。
   *
   * @param containerId 已完成容器的ID
   * @return 需要执行的特权操作列表，返回null表示无额外操作
   * @throws ResourceHandlerException 资源处理异常
   */
  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    LOG.info("postComplete for container: " + containerId.toString());
    // 删除容器对应的net_cls cgroup目录
    cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.NET_CLS,
        containerId.toString());
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown()
      throws ResourceHandlerException {
    LOG.debug("teardown(): Nothing to do");

    return null;
  }

  /**
   * 创建网络标签映射管理器，工厂方法，供测试覆盖。
   * @param conf YARN配置对象
   * @return 网络标签映射管理器实例
   */
  @Private
  @VisibleForTesting
  public NetworkTagMappingManager createNetworkTagMappingManager(
      Configuration conf) {
    return NetworkTagMappingManagerFactory.getManager(conf);
  }

  @Override
  public String toString() {
    return NetworkPacketTaggingHandlerImpl.class.getName();
  }
}