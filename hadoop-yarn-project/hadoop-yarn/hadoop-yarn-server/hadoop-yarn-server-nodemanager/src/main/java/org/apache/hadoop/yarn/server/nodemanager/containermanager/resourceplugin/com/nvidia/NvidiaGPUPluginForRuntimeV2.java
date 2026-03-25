// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.com.nvidia;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePluginScheduler;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRegisterRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * 支持Nvidia容器运行时v2(Docker)和非Docker容器的Nvidia GPU设备插件
 * 同时提供拓扑感知调度和基础调度两种能力
 */
public class NvidiaGPUPluginForRuntimeV2 implements DevicePlugin,
    DevicePluginScheduler {
  public static final Logger LOG = LoggerFactory.getLogger(
      NvidiaGPUPluginForRuntimeV2.class);

  // GPU资源名称，YARN资源模型标识
  public static final String NV_RESOURCE_NAME = "nvidia.com/gpu";

  private NvidiaCommandExecutor shellExecutor = new NvidiaCommandExecutor();

  // 容器环境变量
  private Map<String, String> environment = new HashMap<>();

  // 环境变量：直接指定nvidia-smi二进制路径
  private static final String ENV_BINARY_PATH = "NVIDIA_SMI_PATH";

  // 默认二进制文件名
  private static final String DEFAULT_BINARY_NAME = "nvidia-smi";

  // GPU设备文件前缀
  private static final String DEV_NAME_PREFIX = "nvidia";

  private String pathOfGpuBinary = null;

  // 命令执行最大超时时间：10秒
  private static final int MAX_EXEC_TIMEOUT_MS = 10 * 1000;

  // 默认二进制搜索路径列表，包含nvidia-docker默认安装路径
  private static final Set<String> DEFAULT_BINARY_SEARCH_DIRS = ImmutableSet.of(
      "/usr/bin", "/bin", "/usr/local/nvidia/bin");

  // 拓扑信息是否已初始化
  private boolean topoInitialized = false;

  // 缓存上次探测到的GPU设备列表
  private Set<Device> lastTimeFoundDevices;

  /**
   * 缓存不同GPU设备组合及其通信开销
   * 键是申请的GPU数量，值是按开销升序排列的组合-开销列表
   * 例如：
   * { 2=> [[device1,device2]=>0, [device1,device3]=>10]
   *   3 => [[device1,device2,device3]=>10, [device2,device3,device5]=>20],
   * }
   * */
  private Map<Integer, List<Map.Entry<Set<Device>, Integer>>> costTable
      = new HashMap<>();

  /**
   * 存储两个GPU设备之间的连接权重
   * 键是设备对标识，如"0-1"表示0号和1号GPU，值是通信开销权重
   * */
  private Map<String, Integer> devicePairToWeight = new HashMap<>();

  /**
   * 容器环境变量：指定GPU调度拓扑策略
   * */
  public static final String TOPOLOGY_POLICY_ENV_KEY = "NVIDIA_TOPO_POLICY";

  /**
   * PACK策略：优先选择GPU-GPU通信更快的组合，适合重GPU计算负载
   * */
  public static final String TOPOLOGY_POLICY_PACK = "PACK";

  /**
   * SPREAD策略：优先选择CPU-GPU通信更快的组合，适合重CPU-GPU IO负载
   * */
  public static final String TOPOLOGY_POLICY_SPREAD = "SPREAD";

  @Override
  public DeviceRegisterRequest getRegisterRequestInfo() throws Exception {
    // 向YARN注册GPU资源，返回注册请求信息
    return DeviceRegisterRequest.Builder.newInstance()
        .setResourceName(NV_RESOURCE_NAME).build();
  }

  @Override
  public Set<Device> getDevices() throws Exception {
    // 搜索nvidia-smi二进制路径
    shellExecutor.searchBinary();
    TreeSet<Device> r = new TreeSet<>();
    String output;
    try {
      // 调用nvidia-smi获取设备信息
      output = shellExecutor.getDeviceInfo();
      String[] lines = output.trim().split("\n");
      int id = 0;
      for (String oneLine : lines) {
        String[] tokensEachLine = oneLine.split(",");
        if (tokensEachLine.length != 2) {
          throw new Exception("Cannot parse the output to get device info. "
              + "Unexpected format in it:" + oneLine);
        }
        String minorNumber = tokensEachLine[0].trim();
        String busId = tokensEachLine[1].trim();
        String majorNumber = getMajorNumber(DEV_NAME_PREFIX
            + minorNumber);
        if (majorNumber != null) {
          r.add(Device.Builder.newInstance()
              .setId(id)
              .setMajorNumber(Integer.parseInt(majorNumber))
              .setMinorNumber(Integer.parseInt(minorNumber))
              .setBusID(busId)
              .setDevPath("/dev/" + DEV_NAME_PREFIX + minorNumber)
              .setHealthy(true)
              .build());
          id++;
        }
      }
      // 缓存设备列表供拓扑调度使用
      lastTimeFoundDevices = r;
      return r;
    } catch (IOException e) {
      LOG.debug("Failed to get output from {}", pathOfGpuBinary);
      throw new YarnException(e);
    }
  }

  @Override
  public DeviceRuntimeSpec onDevicesAllocated(Set<Device> allocatedDevices,
      YarnRuntimeType yarnRuntime) throws Exception {
    LOG.debug("Generating runtime spec for allocated devices: {}, {}",
        allocatedDevices, yarnRuntime.getName());
    if (yarnRuntime == YarnRuntimeType.RUNTIME_DOCKER) {
      // Docker运行时，使用nvidia容器运行时v2
      String nvidiaRuntime = "nvidia";
      String nvidiaVisibleDevices = "NVIDIA_VISIBLE_DEVICES";
      StringBuilder gpuMinorNumbersSB = new StringBuilder();
      for (Device device : allocatedDevices) {
        gpuMinorNumbersSB.append(device.getMinorNumber() + ",");
      }
      String minorNumbers = gpuMinorNumbersSB.toString();
      LOG.info("Nvidia Docker v2 assigned GPU: " + minorNumbers);
      // 生成Docker运行时配置：设置可见GPU环境变量和指定运行时
      return DeviceRuntimeSpec.Builder.newInstance()
          .addEnv(nvidiaVisibleDevices,
              minorNumbers.substring(0, minorNumbers.length() - 1))
          .setContainerRuntime(nvidiaRuntime)
          .build();
    }
    // 非Docker运行时无需额外配置
    return null;
  }

  @Override
  public void onDevicesReleased(Set<Device> releasedDevices) throws Exception {
    // 不需要额外处理资源释放
  }

  /**
   * 从/dev下的设备文件获取主设备号
   * @param devName 设备文件名
   * @return 主设备号字符串，获取失败返回null
   */
  private String getMajorNumber(String devName) {
    String output = null;
    // stat命令输出格式：十六进制的"主设备号:次设备号"
    try {
      LOG.debug("Get major numbers from /dev/{}", devName);
      output = shellExecutor.getMajorMinorInfo(devName);
      String[] strs = output.trim().split(":");
      LOG.debug("stat output:{}", output);
      output = Integer.toString(Integer.parseInt(strs[0], 16));
    } catch (IOException e) {
      String msg =
          "Failed to get major number from reading /dev/" + devName;
      LOG.warn(msg);
    } catch (NumberFormatException e) {
      LOG.error("Failed to parse device major number from stat output");
      output = null;
    }
    return output;
  }

  @Override
  public Set<Device> allocateDevices(Set<Device> availableDevices, int count,
      Map<String, String> envs) {
    Set<Device> allocation = new TreeSet<>();
    /**
     * 边界场景处理：不需要拓扑感知调度，直接使用基础调度
     * - 可用GPU总数少于3台
     * - 只申请1台GPU
     * - 申请所有可用GPU
     * */
    if (availableDevices.size() < 3
        || count == 1
        || availableDevices.size() == count) {
      basicSchedule(allocation, count, availableDevices);
      return allocation;
    }

    try {
      if (!topoInitialized) {
        // 初始化GPU拓扑开销表
        initCostTable();
      }
      // 执行拓扑感知调度
      topologyAwareSchedule(allocation, count,
          envs, availableDevices, this.costTable);
      if (allocation.size() == count) {
        return allocation;
      } else {
        LOG.error("Failed to do topology scheduling. Skip to use basic "
            + "scheduling");
      }
    } catch (IOException e) {
      LOG.error("Error in getting GPU topology info. "
          + "Skip topology aware scheduling", e);
    }
    // 拓扑调度失败，回退到基础调度
    basicSchedule(allocation, count, availableDevices);
    return allocation;
  }

  @VisibleForTesting
  public void initCostTable() throws IOException {
    // 获取GPU拓扑信息
    String topo = shellExecutor.getTopologyInfo();
    // 解析拓扑，生成设备对权重表
    parseTopo(topo, devicePairToWeight);
    // 如果没有缓存设备列表，重新探测
    if (lastTimeFoundDevices == null) {
      try {
        getDevices();
      } catch (Exception e) {
        LOG.error("Failed to get devices!", e);
        return;
      }
    }
    // 构建所有可能设备组合的开销表
    buildCostTable(costTable, lastTimeFoundDevices);
    // 调试日志打印开销表
    loggingCostTable(costTable);
    this.topoInitialized = true;
  }

  private void loggingCostTable(
      Map<Integer, List<Map.Entry<Set<Device>, Integer>>> cTable) {
    if (LOG.isDebugEnabled()) {
      // 格式化开销表输出到调试日志
      StringBuilder sb = new StringBuilder("The costTable is:");
      sb.append("\n{");
      for (Map.Entry<Integer, List<Map.Entry<Set<Device>, Integer>>> entry
          : cTable.entrySet()) {
        sb.append("\n\t")
            .append(entry.getKey())
            .append(" => [");
        for (Map.Entry<Set<Device>, Integer> e : entry.getValue()) {
          sb.append("\n\t\t").append(e.toString()).append(",\n");
        }
        sb.append("\t\t]\n");
      }
      sb.append("}\n");
      LOG.debug(sb.toString());
    }
  }

  /**
   * 生成所有设备组合及其开销，存入开销表
   * */
  private void buildCostTable(
      Map<Integer, List<Map.Entry<Set<Device>, Integer>>> cTable,
      Set<Device> ltfDevices) {
    Device[] deviceList = new Device[ltfDevices.size()];
    ltfDevices.toArray(deviceList);
    generateAllDeviceCombination(cTable, deviceList, deviceList.length);
  }

  /**
   * 生成2到n-1个设备的所有可能组合，计算每个组合的开销并排序
   */
  private void generateAllDeviceCombination(
      Map<Integer, List<Map.Entry<Set<Device>, Integer>>> cTable,
      Device[] allDevices, int n) {
    // 生成从2到n-1个设备的所有组合（1和n不需要拓扑调度）
    for (int i = 2; i < n; i++) {
      Map<Set<Device>, Integer> combinationToCost =
          new HashMap<>();
      buildCombination(combinationToCost, allDevices, n, i);
      // 按开销升序排序
      List<Map.Entry<Set<Device>, Integer>> listSortedByCost =
          new LinkedList<>(combinationToCost.entrySet());
      Collections.sort(listSortedByCost,
          (o1, o2) -> (o1.getValue()).compareTo(o2.getValue()));
      cTable.put(i, listSortedByCost);
    }
  }

  private void buildCombination(Map<Set<Device>, Integer> combinationToCost,
      Device[] allDevices, int n, int r) {
    // 临时数组存储当前组合
    Device[] subDeviceList = new Device[r];
    // 递归生成所有组合
    combinationRecursive(combinationToCost, allDevices, subDeviceList,
        0, n - 1, 0, r);
  }

  /**
   * 递归生成所有大小为r的设备组合，计算并存储每个组合的开销
   *
   * @param cTc           组合到开销的映射表
   * @param allDevices    全部GPU设备数组
   * @param subDeviceList 临时存储当前组合
   * @param start         全量数组起始索引
   * @param end           全量数组结束索引
   * @param index         当前组合已填充位置索引
   * @param r             目标组合大小
   */
  void combinationRecursive(Map<Set<Device>, Integer> cTc,
      Device[] allDevices, Device[] subDeviceList,
      int start, int end, int index, int r) {
    // 当前组合已达到目标大小，计算开销并存入表
    if (index == r) {
      Set<Device> oneSet = new TreeSet<>(Arrays.asList(subDeviceList));
      int cost = computeCostOfDevices(subDeviceList);
      cTc.put(oneSet, cost);
      return;
    }
    // 递归枚举所有可能选择
    for (int i = start; i <= end; i++) {
      subDeviceList[index] = allDevices[i];
      combinationRecursive(cTc, allDevices, subDeviceList,
          i + 1, end, index + 1, r);
    }
  }

  /**
   * 计算给定GPU设备组合的总通信开销
   * 累加组合中每对GPU设备的连接权重
   */
  @VisibleForTesting
  public int computeCostOfDevices(Device[] devices) {
    int cost = 0;
    String gpuIndex0;
    String gpuIndex1;
    // 遍历所有不重复的设备对，累加权重
    for (int i = 0; i < devices.length; i++) {
      gpuIndex0 = String.valueOf(devices[i].getMinorNumber());
      for (int j = i + 1; j < devices.length; j++) {
        gpuIndex1 = String.valueOf(devices[j].getMinorNumber());
        cost += this.devicePairToWeight.get(gpuIndex0 + "-" + gpuIndex1);
      }
    }
    return cost;
  }

  /**
   * GPU拓扑感知调度算法
   * 支持PACK和SPREAD两种策略，通过容器环境变量指定，默认PACK策略
   * PACK策略优先选择GPU-GPU通信更快的组合，SPREAD优先选择分散布局
   * */
  @VisibleForTesting
  public void topologyAwareSchedule(Set<Device> allocation, int count,
      Map<String, String> envs,
      Set<Device> availableDevices,
      Map<Integer, List<Map.Entry<Set<Device>, Integer>>> cTable) {
    int num = 0;
    // 从环境变量获取调度策略，默认PACK
    String policy = envs.get(TOPOLOGY_POLICY_ENV_KEY);
    if (policy == null) {
      policy = TOPOLOGY_POLICY_PACK;
    }

    /**
     * 从开销表中取出对应申请数量的所有已排序组合
     * */
    if (cTable == null) {
      LOG.error("No cost table initialized!");
      return;
    }
    List<Map.Entry<Set<Device>, Integer>> combinationsToCost =
        cTable.get(count);
    Iterator<