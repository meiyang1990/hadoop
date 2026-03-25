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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * NUMA资源分配器，负责读取主机NUMA拓扑信息，并为容器分配NUMA节点资源，实现NUMA感知调度，提升容器访问内存性能。
 */
public class NumaResourceAllocator {

  private static final Logger LOG = LoggerFactory.
      getLogger(NumaResourceAllocator.class);

  // 匹配NUMA节点ID范围的正则表达式，示例格式: 'available: 2 nodes (0-1)'
  private static final String NUMA_NODEIDS_REGEX =
      "available:\\s*[0-9]+\\s*nodes\\s*\\(([0-9\\-,]*)\\)";

  // 匹配NUMA节点内存容量的正则表达式，示例格式: 'node 0 size: 73717 MB'
  private static final String NUMA_NODE_MEMORY_REGEX =
      "node\\s*<NUMA-NODE>\\s*size:\\s*([0-9]+)\\s*([KMG]B)";

  // 匹配NUMA节点CPU列表的正则表达式，示例格式: 'node 0 cpus: 0 2 4 6'
  private static final String NUMA_NODE_CPUS_REGEX =
      "node\\s*<NUMA-NODE>\\s*cpus:\\s*([0-9\\s]+)";

  private static final String GB = "GB";
  private static final String KB = "KB";
  private static final String NUMA_NODE = "<NUMA-NODE>";
  private static final String SPACE = "\\s";
  private static final long DEFAULT_NUMA_NODE_MEMORY = 1024;
  private static final int DEFAULT_NUMA_NODE_CPUS = 1;
  private static final String NUMA_RESOURCE_TYPE = "numa";

  // 存储所有NUMA节点资源信息的列表
  private List<NumaNodeResource> numaNodesList = new ArrayList<>();
  // NUMA节点ID到资源信息的映射表
  private Map<String, NumaNodeResource> numaNodeIdVsResource = new HashMap<>();
  // 轮询分配的当前节点索引
  private int currentAssignNode;

  // NodeManager上下文对象，用于访问NM状态存储等服务
  private Context context;

  /**
   * 构造NUMA资源分配器，关联NodeManager上下文。
   * @param context NodeManager上下文
   */
  public NumaResourceAllocator(Context context) {
    this.context = context;
  }

  /**
   * 初始化NUMA资源分配器，根据配置自动发现或手动读取NUMA拓扑信息。
   * @param conf 配置对象
   * @throws YarnException 初始化失败时抛出异常
   */
  public void init(Configuration conf) throws YarnException {
    if (conf.getBoolean(YarnConfiguration.NM_NUMA_AWARENESS_READ_TOPOLOGY,
        YarnConfiguration.DEFAULT_NM_NUMA_AWARENESS_READ_TOPOLOGY)) {
      LOG.info("Reading NUMA topology using 'numactl --hardware' command.");
      // 执行numactl命令获取拓扑输出
      String cmdOutput = executeNGetCmdOutput(conf);
      // 按行拆分输出
      String[] outputLines = cmdOutput.split("\\n");
      Pattern pattern = Pattern.compile(NUMA_NODEIDS_REGEX);
      String nodeIdsStr = null;
      // 遍历行查找NUMA节点ID行
      for (String line : outputLines) {
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
          nodeIdsStr = matcher.group(1);
          break;
        }
      }
      // 未解析到节点ID抛出异常
      if (nodeIdsStr == null) {
        throw new YarnException("Failed to get numa nodes from"
            + " 'numactl --hardware' output and output is:\n" + cmdOutput);
      }
      // 拆分节点ID或范围
      String[] nodeIdCommaSplits = nodeIdsStr.split("[,\\s]");
      // 遍历每个节点ID/范围
      for (String nodeIdOrRange : nodeIdCommaSplits) {
        if (nodeIdOrRange.contains("-")) {
          // 处理范围格式，如 0-1
          String[] beginNEnd = nodeIdOrRange.split("-");
          int endNode = Integer.parseInt(beginNEnd[1]);
          // 展开范围中每个节点ID
          for (int nodeId = Integer
              .parseInt(beginNEnd[0]); nodeId <= endNode; nodeId++) {
            // 解析节点内存容量
            long memory = parseMemory(outputLines, String.valueOf(nodeId));
            // 解析节点CPU数量
            int cpus = parseCpus(outputLines, String.valueOf(nodeId));
            // 添加到节点集合
            addToCollection(String.valueOf(nodeId), memory, cpus);
          }
        } else {
          // 处理单个节点ID
          long memory = parseMemory(outputLines, nodeIdOrRange);
          int cpus = parseCpus(outputLines, nodeIdOrRange);
          addToCollection(nodeIdOrRange, memory, cpus);
        }
      }
    } else {
      // 从配置文件读取NUMA拓扑信息
      LOG.info("Reading NUMA topology using configurations.");
      Collection<String> nodeIds = conf
          .getStringCollection(YarnConfiguration.NM_NUMA_AWARENESS_NODE_IDS);
      // 遍历每个配置的节点
      for (String nodeId : nodeIds) {
        // 读取配置的内存容量，使用默认值兜底
        long mem = conf.getLong(
            "yarn.nodemanager.numa-awareness." + nodeId + ".memory",
            DEFAULT_NUMA_NODE_MEMORY);
        // 读取配置的CPU数量，使用默认值兜底
        int cpus = conf.getInt(
            "yarn.nodemanager.numa-awareness." + nodeId + ".cpus",
            DEFAULT_NUMA_NODE_CPUS);
        addToCollection(nodeId, mem, cpus);
      }
    }
    // 未获取到任何NUMA节点抛出异常
    if (numaNodesList.isEmpty()) {
      throw new YarnException("There are no available NUMA nodes"
          + " for making containers NUMA aware.");
    }
    LOG.info("Available numa nodes with capacities : " + numaNodesList.size());
  }

  @VisibleForTesting
  /**
   * 执行numactl命令获取NUMA拓扑输出。
   * @param conf 配置对象
   * @return 命令输出字符串
   * @throws YarnException 命令执行失败时抛出异常
   */
  public String executeNGetCmdOutput(Configuration conf) throws YarnException {
    String numaCtlCmd = conf.get(
        YarnConfiguration.NM_NUMA_AWARENESS_NUMACTL_CMD,
        YarnConfiguration.DEFAULT_NM_NUMA_AWARENESS_NUMACTL_CMD);
    String[] args = new String[] {numaCtlCmd, "--hardware"};
    ShellCommandExecutor shExec = new ShellCommandExecutor(args);
    try {
      shExec.execute();
    } catch (IOException e) {
      throw new YarnException("Failed to read the numa configurations.", e);
    }
    return shExec.getOutput();
  }

  /**
   * 从numactl输出中解析指定NUMA节点的CPU数量。
   * @param outputLines numactl输出行数组
   * @param nodeId NUMA节点ID
   * @return 节点包含的CPU数量
   */
  private int parseCpus(String[] outputLines, String nodeId) {
    int cpus = 0;
    Pattern patternNodeCPUs = Pattern
        .compile(NUMA_NODE_CPUS_REGEX.replace(NUMA_NODE, nodeId));
    for (String line : outputLines) {
      Matcher matcherNodeCPUs = patternNodeCPUs.matcher(line);
      if (matcherNodeCPUs.find()) {
        String cpusStr = matcherNodeCPUs.group(1);
        // 按空格拆分得到每个CPU，统计个数
        cpus = cpusStr.split(SPACE).length;
        break;
      }
    }
    return cpus;
  }

  /**
   * 从numactl输出中解析指定NUMA节点的内存容量，统一转换为MB单位。
   * @param outputLines numactl输出行数组
   * @param nodeId NUMA节点ID
   * @return 节点内存容量（MB）
   * @throws YarnException 解析失败时抛出异常
   */
  private long parseMemory(String[] outputLines, String nodeId)
      throws YarnException {
    long memory = 0;
    String units;
    Pattern patternNodeMem = Pattern
        .compile(NUMA_NODE_MEMORY_REGEX.replace(NUMA_NODE, nodeId));
    for (String line : outputLines) {
      Matcher matcherNodeMem = patternNodeMem.matcher(line);
      if (matcherNodeMem.find()) {
        try {
          memory = Long.parseLong(matcherNodeMem.group(1));
          units = matcherNodeMem.group(2);
          // 单位转换为MB
          if (GB.equals(units)) {
            memory = memory * 1024;
          } else if (KB.equals(units)) {
            memory = memory / 1024;
          }
        } catch (Exception ex) {
          throw new YarnException("Failed to get memory for node:" + nodeId,
              ex);
        }
        break;
      }
    }
    return memory;
  }

  /**
   * 将解析完成的NUMA节点添加到内部集合。
   * @param nodeId NUMA节点ID
   * @param memory 内存容量（MB）
   * @param cpus CPU数量
   */
  private void addToCollection(String nodeId, long memory, int cpus) {
    NumaNodeResource numaNode = new NumaNodeResource(nodeId, memory, cpus);
    numaNodesList.add(numaNode);
    numaNodeIdVsResource.put(nodeId, numaNode);
  }

  /**
   * 为容器分配NUMA节点资源，并持久化分配信息到NM状态存储。
   *
   * @param container 目标容器
   * @return 分配结果，无可用资源返回null
   * @throws ResourceHandlerException 存储分配信息失败时抛出异常
   */
  public synchronized NumaResourceAllocation allocateNumaNodes(
      Container container) throws ResourceHandlerException {
    NumaResourceAllocation allocation = allocate(container.getContainerId(),
        container.getResource());
    if (allocation != null) {
      try {
        // 将分配信息持久化到NM状态存储，用于恢复
        context.getNMStateStore().storeAssignedResources(container,
            NUMA_RESOURCE_TYPE, Arrays.asList(allocation));
      } catch (IOException e) {
        // 存储失败回滚分配
        releaseNumaResource(container.getContainerId());
        throw new ResourceHandlerException(e);
      }
    }
    return allocation;
  }

  /**
   * 核心分配逻辑，优先尝试单节点分配，无法满足时跨节点分配内存和CPU。
   * @param containerId 容器ID
   * @param resource 容器申请的资源
   * @return 分配结果，无可用资源返回null
   * @throws ResourceHandlerException 分配异常
   */
  private NumaResourceAllocation allocate(ContainerId containerId,
      Resource resource) throws ResourceHandlerException {
    // 轮询查找可满足整个容器资源的单个NUMA节点
    for (int index = 0; index < numaNodesList.size(); index++) {
      NumaNodeResource numaNode = numaNodesList
          .get((currentAssignNode + index) % numaNodesList.size());
      if (numaNode.isResourcesAvailable(resource)) {
        // 找到合适节点，分配资源
        numaNode.assignResources(resource, containerId);
        LOG.info("Assigning NUMA node " + numaNode.getNodeId() + " for memory, "
            + numaNode.getNodeId() + " for cpus for the " + containerId);
        // 更新轮询索引
        currentAssignNode = (currentAssignNode + index + 1)
            % numaNodesList.size();
        // 返回单节点分配结果
        return new NumaResourceAllocation(numaNode.getNodeId(),
            resource.getMemorySize(), numaNode.getNodeId(),
            resource.getVirtualCores());
      }
    }

    // 单个节点无法满足，开始跨节点分配内存
    long memoryRequirement = resource.getMemorySize();
    Map<String, Long> memoryAllocations = Maps.newHashMap();
    for (NumaNodeResource numaNode : numaNodesList) {
      // 在当前节点分配尽可能多的内存，返回剩余需求量
      long memoryRemaining = numaNode.
          assignAvailableMemory(memoryRequirement, containerId);
      // 记录当前节点分配量
      memoryAllocations.put(numaNode.getNodeId(),
          memoryRequirement - memoryRemaining);
      // 更新剩余需求量
      memoryRequirement = memoryRemaining;
      if (memoryRequirement == 0) {
        break;
      }
    }
    // 内存分配失败，释放已分配资源返回null
    if (memoryRequirement != 0) {
      LOG.info("There is no available memory:" + resource.getMemorySize()
          + " in numa nodes for " + containerId);
      releaseNumaResource(containerId);
      return null;
    }

    // 内存分配成功，开始跨节点分配CPU
    int cpusRequirement = resource.getVirtualCores();
    Map<String, Integer> cpuAllocations = Maps.newHashMap();
    for (int index = 0; index < numaNodesList.size(); index++) {
      NumaNodeResource numaNode = numaNodesList
          .get((currentAssignNode + index) % numaNodesList.size());
      // 在当前节点分配尽可能多的CPU，返回剩余需求量
      int cpusRemaining = numaNode.
          assignAvailableCpus(cpusRequirement, containerId);
      // 记录当前节点分配量
      cpuAllocations.put(numaNode.getNodeId(), cpusRequirement - cpusRemaining);
      // 更新剩余需求量
      cpusRequirement = cpusRemaining;
      if (cpusRequirement == 0) {
        // 更新轮询索引
        currentAssignNode = (currentAssignNode + index + 1)
            % numaNodesList.size();
        break;
      }
    }

    // CPU分配失败，释放已分配资源返回null
    if (cpusRequirement != 0) {
      LOG.info("There are no available cpus:" + resource.getVirtualCores()
          + " in numa nodes for " + containerId);
      releaseNumaResource(containerId);
      return null;
    }

    // 跨节点分配成功，构造并返回分配结果
    NumaResourceAllocation assignedNumaNodeInfo =
        new NumaResourceAllocation(memoryAllocations, cpuAllocations);
    LOG.info("Assigning multiple NUMA nodes ("
        + StringUtils.join(",", assignedNumaNodeInfo.getMemNodes())
        + ") for memory, ("
        + StringUtils.join(",", assignedNumaNodeInfo.getCpuNodes())
        + ") for cpus for " + containerId);
    return assignedNumaNodeInfo;
  }

  /**
   * 释放容器占用的NUMA资源，从NM状态存储删除分配记录。
   *
   * @param containerId 容器ID
   * @throws ResourceHandlerException 删除状态记录失败时抛出异常
   */
  public synchronized void releaseNumaResource(ContainerId containerId