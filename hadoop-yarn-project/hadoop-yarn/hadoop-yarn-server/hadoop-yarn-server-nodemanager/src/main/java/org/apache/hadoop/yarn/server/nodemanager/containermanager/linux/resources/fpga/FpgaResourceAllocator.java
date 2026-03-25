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


package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga;


import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDevice;


import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.FPGA_URI;

/**
 * 节点管理器上FPGA资源分配器，支持不同FPGA厂商插件，分配时考虑设备类型维度
 * 负责管理本节点上FPGA设备的分配、回收与恢复
 */
public class FpgaResourceAllocator {

  static final Logger LOG = LoggerFactory.
      getLogger(FpgaResourceAllocator.class);

  /** 本节点允许使用的所有FPGA设备列表 */
  private List<FpgaDevice> allowedFpgas = new LinkedList<>();

  //key is resource type of FPGA, vendor plugin supported ID
  /** 按类型分组的可用FPGA设备，key为FPGA类型，value为该类型可用设备列表 */
  private Map<String, List<FpgaDevice>> availableFpgas = new HashMap<>();

  //key is the container ID
  /** 容器与已分配FPGA设备的映射，key为容器ID字符串，value为分配给该容器的设备列表 */
  private Map<String, List<FpgaDevice>> containerToFpgaMapping =
      new HashMap<>();

  /** 节点管理器上下文 */
  private Context nmContext;

  @VisibleForTesting
  Map<String, List<FpgaDevice>> getAvailableFpga() {
    return availableFpgas;
  }

  @VisibleForTesting
  List<FpgaDevice> getAllowedFpga() {
    return allowedFpgas;
  }

  /**
   * 构造FPGA资源分配器
   * @param ctx 节点管理器上下文
   */
  public FpgaResourceAllocator(Context ctx) {
    this.nmContext = ctx;
  }

  @VisibleForTesting
  int getAvailableFpgaCount() {
    int count = 0;

    count = availableFpgas.values()
      .stream()
      .mapToInt(i -> i.size())
      .sum();

    return count;
  }

  @VisibleForTesting
  Map<String, List<FpgaDevice>> getUsedFpga() {
    return containerToFpgaMapping;
  }

  @VisibleForTesting
  int getUsedFpgaCount() {
    int count = 0;

    count = containerToFpgaMapping.values()
        .stream()
        .mapToInt(i -> i.size())
        .sum();

    return count;
  }

  /**
   * FPGA分配结果封装类，包含成功分配和未分配的设备列表
   */
  public static class FpgaAllocation {

    private List<FpgaDevice> allowed = Collections.emptyList();

    private List<FpgaDevice> denied = Collections.emptyList();

    FpgaAllocation(List<FpgaDevice> allowed, List<FpgaDevice> denied) {
      if (allowed != null) {
        this.allowed = ImmutableList.copyOf(allowed);
      }
      if (denied != null) {
        this.denied = ImmutableList.copyOf(denied);
      }
    }

    public List<FpgaDevice> getAllowed() {
      return allowed;
    }

    public List<FpgaDevice> getDenied() {
      return denied;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("\nFpgaAllocation\n\tAllowed:\n");
      for (FpgaDevice device : allowed) {
        sb.append("\t\t");
        sb.append(device + "\n");
      }
      sb.append("\tDenied\n");
      for (FpgaDevice device : denied) {
        sb.append("\t\t");
        sb.append(device + "\n");
      }
      return sb.toString();
    }
  }

  /**
   * 初始化时添加FPGA设备，仅调用一次
   * @param type FPGA设备类型
   * @param list 待添加的设备列表
   */
  public synchronized void addFpgaDevices(String type, List<FpgaDevice> list) {
    availableFpgas.putIfAbsent(type, new LinkedList<>());
    List<FpgaDevice> fpgaDevices = new LinkedList<>();

    for (FpgaDevice device : list) {
      if (!allowedFpgas.contains(device)) {
        fpgaDevices.add(device);
        availableFpgas.get(type).add(device);
      } else {
        LOG.warn("Duplicate device found: " + device + ". Ignored");
      }
    }

    allowedFpgas = ImmutableList.copyOf(fpgaDevices);
    LOG.info("Added a list of FPGA Devices: " + allowedFpgas);
  }

  /**
   * 更新已分配FPGA设备的IPID和比特流哈希信息
   * @param requestor 请求容器ID
   * @param device 待更新的FPGA设备
   * @param newIPID 新的IPID
   * @param newHash 新的aocx文件哈希
   */
  public synchronized void updateFpga(String requestor,
      FpgaDevice device, String newIPID, String newHash) {
    device.setIPID(newIPID);
    device.setAocxHash(newHash);
    LOG.info("Update IPID to " + newIPID +
        " for this allocated device: " + device);
    LOG.info("Update IP hash to " + newHash);
  }

  /**
   * 分配FPGA设备，优先分配匹配比特流哈希的设备，不足时分配任意可用设备
   * @param type vendor plugin supported FPGA device type
   * @param count requested FPGA slot count
   * @param container container id
   * @param ipidHash hash of the localized aocx file
   * @return Instance consists two List of allowed and denied {@link FpgaDevice}
   * @throws ResourceHandlerException When failed to allocate or write state store
   * */
  public synchronized FpgaAllocation assignFpga(String type, long count,
      Container container, String ipidHash) throws ResourceHandlerException {
    List<FpgaDevice> currentAvailableFpga = availableFpgas.get(type);

    String requestor = container.getContainerId().toString();
    if (null == currentAvailableFpga) {
      throw new ResourceHandlerException("No such type of FPGA resource available: " + type);
    }
    if (count < 0 || count > currentAvailableFpga.size()) {
      throw new ResourceHandlerException("Invalid FPGA request count or not enough, requested:" +
          count + ", available:" + getAvailableFpgaCount());
    }
    if (count > 0) {
      // Allocate devices with matching IP first, then any device is ok
      List<FpgaDevice> assignedFpgas = new LinkedList<>();
      int matchIPCount = 0;
      // 优先分配比特流哈希匹配的设备，减少重复编程开销
      for (int i = 0; i < currentAvailableFpga.size(); i++) {
        String deviceIPIDhash = currentAvailableFpga.get(i).getAocxHash();
        if (deviceIPIDhash != null &&
            deviceIPIDhash.equalsIgnoreCase(ipidHash)) {
          assignedFpgas.add(currentAvailableFpga.get(i));
          currentAvailableFpga.remove(i);
          matchIPCount++;
        }
      }
      // 不足时分配剩余任意设备
      int remaining = (int) count - matchIPCount;
      while (remaining > 0) {
        assignedFpgas.add(currentAvailableFpga.remove(0));
        remaining--;
      }

      // Record in state store if we allocated anything
      if (!assignedFpgas.isEmpty()) {
        try {
          // 将分配信息持久化到节点状态存储，支持节点重启恢复
          nmContext.getNMStateStore().storeAssignedResources(container,
              FPGA_URI, new LinkedList<>(assignedFpgas));
        } catch (IOException e) {
          // 持久化失败，回滚分配
          currentAvailableFpga.addAll(assignedFpgas);
          throw new ResourceHandlerException(e);
        }

        // 持久化成功，更新本地分配映射
        containerToFpgaMapping.putIfAbsent(requestor, new LinkedList<>());
        containerToFpgaMapping.get(requestor).addAll(assignedFpgas);
      }

      return new FpgaAllocation(assignedFpgas, currentAvailableFpga);
    }
    // 请求数量为0，返回空分配结果
    return new FpgaAllocation(null, allowedFpgas);
  }

  /**
   * 节点重启后从状态存储恢复已分配FPGA设备
   * @param containerId 待恢复的容器ID
   * @throws ResourceHandlerException 恢复失败时抛出异常
   */
  public synchronized void recoverAssignedFpgas(ContainerId containerId) throws ResourceHandlerException {
    Container c = nmContext.getContainers().get(containerId);
    if (null == c) {
      throw new ResourceHandlerException(
          "This shouldn't happen, cannot find container with id="
              + containerId);
    }

    for (Serializable fpgaDevice :
        c.getResourceMappings().getAssignedResources(FPGA_URI)) {
      if (!(fpgaDevice instanceof FpgaDevice)) {
        throw new ResourceHandlerException(
            "Trying to recover allocated FPGA devices, however it"
                + " is not FpgaDevice type, this shouldn't happen");
      }

      // 校验设备在当前节点允许列表中
      if (!allowedFpgas.contains(fpgaDevice)) {
        throw new ResourceHandlerException("Try to recover FpgaDevice = " + fpgaDevice
            + " however it is not in allowed device list:" + StringUtils
            .join(";", allowedFpgas));
      }

      // 校验设备未被其他容器占用
      Iterator<Map.Entry<String, List<FpgaDevice>>> iterator =
          getUsedFpga().entrySet().iterator();
      while (iterator.hasNext()) {
        if (iterator.next().getValue().contains(fpgaDevice)) {
          throw new ResourceHandlerException("Try to recover FpgaDevice = " + fpgaDevice
              + " however it is already assigned to others");
        }
      }
      // 添加到容器分配映射
      getUsedFpga().putIfAbsent(containerId.toString(), new LinkedList<>());
      getUsedFpga().get(containerId.toString()).add((FpgaDevice) fpgaDevice);
      // 从可用列表移除已恢复设备
      getAvailableFpga().get(((FpgaDevice) fpgaDevice).getType()).remove(fpgaDevice);
    }
  }

  /**
   * 容器退出后清理FPGA设备分配，将设备归还可用列表
   * @param requestor 容器ID字符串
   */
  public synchronized void cleanupAssignFpgas(String requestor) {
    List<FpgaDevice> usedFpgas = containerToFpgaMapping.get(requestor);
    if (usedFpgas != null) {
      for (FpgaDevice device : usedFpgas) {
        // 将设备归还对应类型的可用列表
        availableFpgas.get(device.getType()).add(device);
      }
      // 移除容器分配映射
      containerToFpgaMapping.remove(requestor);
    }
  }
}