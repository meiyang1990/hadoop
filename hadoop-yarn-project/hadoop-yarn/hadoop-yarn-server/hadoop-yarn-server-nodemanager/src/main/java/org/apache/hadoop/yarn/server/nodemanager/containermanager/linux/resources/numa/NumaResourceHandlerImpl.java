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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.numa;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation.OperationType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;

/**
 * 为容器分配NUMA资源的资源处理器实现，实现了YARN NodeManager的NUMA感知调度能力。
 * 通过绑定容器到指定NUMA节点，减少跨NUMA节点访问内存的性能开销，提升容器运行性能。
 */
public class NumaResourceHandlerImpl implements ResourceHandler {

  private static final Logger LOG = LoggerFactory
      .getLogger(NumaResourceHandlerImpl.class);
  // NUMA资源分配器实例，负责核心的分配、回收和恢复逻辑
  private final NumaResourceAllocator numaResourceAllocator;
  // 系统numactl命令路径，用于设置容器NUMA绑定参数
  private final String numaCtlCmd;

  /**
   * 构造NUMA资源处理器，初始化分配器并加载numactl命令路径配置。
   * @param conf YARN配置对象
   * @param nmContext NodeManager上下文对象
   */
  public NumaResourceHandlerImpl(Configuration conf, Context nmContext) {
    LOG.info("NUMA resources allocation is enabled, initializing NUMA resources"
        + " allocator.");
    numaResourceAllocator = new NumaResourceAllocator(nmContext);
    numaCtlCmd = conf.get(YarnConfiguration.NM_NUMA_AWARENESS_NUMACTL_CMD,
        YarnConfiguration.DEFAULT_NM_NUMA_AWARENESS_NUMACTL_CMD);
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    try {
      // 初始化NUMA资源分配器，解析本节点NUMA拓扑信息
      numaResourceAllocator.init(configuration);
    } catch (YarnException e) {
      throw new ResourceHandlerException(e);
    }
    return null;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    List<PrivilegedOperation> ret = null;
    // 为容器分配对应的NUMA节点
    NumaResourceAllocation numaAllocation = numaResourceAllocator
        .allocateNumaNodes(container);
    if (numaAllocation != null) {
      // 如果分配成功，构造numactl参数添加到容器启动参数
      ret = new ArrayList<>();
      ArrayList<String> args = new ArrayList<>();
      args.add(numaCtlCmd);
      // 设置内存交错分配策略
      args.add(
          "--interleave=" + String.join(",", numaAllocation.getMemNodes()));
      // 设置CPU节点绑定策略
      args.add(
          "--cpunodebind=" + String.join(",", numaAllocation.getCpuNodes()));
      ret.add(new PrivilegedOperation(OperationType.ADD_NUMA_PARAMS, args));
    }
    return ret;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    try {
      // 恢复重启后原有容器的NUMA资源分配记录
      numaResourceAllocator.recoverNumaResource(containerId);
    } catch (Throwable e) {
      throw new ResourceHandlerException(
          "Failed to recover numa resource for " + containerId, e);
    }
    return null;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    // 容器完成后释放占用的NUMA资源
    numaResourceAllocator.releaseNumaResource(containerId);
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown() throws ResourceHandlerException {
    return null;
  }

  @Override
  public String toString() {
    return NumaResourceHandlerImpl.class.getName() + "{" +
        "numaResourceAllocator=" + numaResourceAllocator +
        ", numaCtlCmd='" + numaCtlCmd + '\'' +
        '}';
  }
}