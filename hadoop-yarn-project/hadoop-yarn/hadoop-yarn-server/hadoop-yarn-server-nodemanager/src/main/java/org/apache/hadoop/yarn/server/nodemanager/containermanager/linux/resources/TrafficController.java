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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 对Linux tc(traffic control)工具的封装类，提供容器网络流量控制所需的特定tc功能
 * 用于实现YARN容器出站带宽限流
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable 
class TrafficController {
  private static final Logger LOG =
       LoggerFactory.getLogger(TrafficController.class);
  // 根队列规则的句柄ID
  private static final int ROOT_QDISC_HANDLE = 42;
  // 零类ID
  private static final int ZERO_CLASS_ID = 0;
  // 根类ID
  private static final int ROOT_CLASS_ID = 1;
  /** 用于处理所有未分类流量的流量整形类ID */
  private static final int DEFAULT_CLASS_ID = 2;
  /** 用于处理所有YARN流量的流量整形根类ID */
  private static final int YARN_ROOT_CLASS_ID = 3;
  /** 
   * 0-3已被系统保留使用，容器类ID必须从4开始分配
   * 避免和系统预置类ID冲突
   */
  private static final int MIN_CONTAINER_CLASS_ID = 4;
  /** 支持的最大容器流量整形类数量 */
  private static final int MAX_CONTAINER_CLASSES = 1024;

  private static final String MBIT_SUFFIX = "mbit";
  private static final String TMP_FILE_PREFIX = "tc.";
  private static final String TMP_FILE_SUFFIX = ".cmds";

  /** 挂载到网卡根的根排队规则命令模板 */
  private static final String FORMAT_QDISC_ADD_TO_ROOT_WITH_DEFAULT =
      "qdisc add dev %s root handle %d: htb default %s";
  /** 
   * 基于cgroup的过滤器添加命令模板
   * 根据出站包关联的classid选择对应的流量整形规则
   */
  private static final String FORMAT_FILTER_CGROUP_ADD_TO_PARENT =
      "filter add dev %s parent %d: protocol ip prio 10 handle 1: cgroup";
  /** 添加带带宽限制的流量整形类到父类的命令模板 */
  private static final String FORMAT_CLASS_ADD_TO_PARENT_WITH_RATES =
      "class add dev %s parent %d:%d classid %d:%d htb rate %s ceil %s";
  /** 删除流量整形类命令模板 */
  private static final String FORMAT_DELETE_CLASS =
      "class del dev %s classid %d:%d";
  /** net_cls cgroup使用的classid格式模板，要求为0xAAAABBBB形式 */
  private static final String FORMAT_NET_CLS_CLASS_ID = "0x%04d%04d";
  /** 读取网卡关联的qdisc/filter/class状态命令模板 */
  private static final String FORMAT_READ_STATE =
      "qdisc show dev %1$s%n" +
          "filter show dev %1$s%n" +
          "class show dev %1$s";
  private static final String FORMAT_READ_CLASSES = "class show dev %s";
  /** 删除根qdisc及其所有子元素（类/过滤器等）命令模板 */
  private static final String FORMAT_WIPE_STATE =
      "qdisc del dev %s parent root";

  private final Configuration conf;
  // 存储已分配的容器类ID，使用BitSet高效管理空闲/占用状态
  private final BitSet classIdSet;
  // 特权操作执行器，用于执行需要root权限的tc命令
  private final PrivilegedOperationExecutor privilegedOperationExecutor;

  // tc命令临时文件存放目录
  private String tmpDirPath;
  // 要限流的网络设备名称
  private String device;
  // 根队列总带宽，单位：Mbit
  private int rootBandwidthMbit;
  // YARN总可用带宽，单位：Mbit
  private int yarnBandwidthMbit;
  // 默认非YARN流量带宽，单位：Mbit
  private int defaultClassBandwidthMbit;

  /**
   * 构造TrafficController实例
   */
  TrafficController(Configuration conf, PrivilegedOperationExecutor exec) {
    this.conf = conf;
    this.classIdSet = new BitSet(MAX_CONTAINER_CLASSES);
    this.privilegedOperationExecutor = exec;
  }

  /**
   * 初始化引导tc配置，根据NM恢复策略决定是否清理已有配置
   * @param device 限流目标网络设备
   * @param rootBandwidthMbit 根队列总带宽
   * @param yarnBandwidthMbit YARN可用总带宽
   * @throws ResourceHandlerException 初始化失败时抛出异常
   */
  public void bootstrap(String device, int rootBandwidthMbit, int
      yarnBandwidthMbit)
      throws ResourceHandlerException {
    if (device == null) {
      throw new ResourceHandlerException("device cannot be null!");
    }

    String tmpDirBase = conf.get("hadoop.tmp.dir");
    if (tmpDirBase == null) {
      throw new ResourceHandlerException("hadoop.tmp.dir not set!");
    }
    tmpDirPath = tmpDirBase + "/nm-tc-rules";

    File tmpDir = new File(tmpDirPath);
    if (!(tmpDir.exists() || tmpDir.mkdirs())) {
      LOG.warn("Unable to create directory: " + tmpDirPath);
      throw new ResourceHandlerException("Unable to create directory: " +
          tmpDirPath);
    }

    this.device = device;
    this.rootBandwidthMbit = rootBandwidthMbit;
    this.yarnBandwidthMbit = yarnBandwidthMbit;
    // 计算默认非YARN流量可用带宽，如果YARN已经占用全部带宽则默认类也使用全部带宽
    defaultClassBandwidthMbit = (rootBandwidthMbit - yarnBandwidthMbit) <= 0
        ? rootBandwidthMbit : (rootBandwidthMbit - yarnBandwidthMbit);

    boolean recoveryEnabled = conf.getBoolean(YarnConfiguration
        .NM_RECOVERY_ENABLED, YarnConfiguration.DEFAULT_NM_RECOVERY_ENABLED);
    String state = null;

    if (!recoveryEnabled) {
      LOG.info("NM recovery is not enabled. We'll wipe tc state before proceeding.");
    } else {
      // NM恢复开启，先检查当前tc状态是否已经正确初始化
      state = readState();
      if (checkIfAlreadyBootstrapped(state)) {
        LOG.info("TC configuration is already in place. Not wiping state.");

        // 从已有状态中恢复已分配的容器类ID
        reacquireContainerClasses(state);
        return;
      } else {
        LOG.info("TC configuration is incomplete. Wiping tc state before proceeding");
      }
    }

    wipeState(); // 清理之前不完整的引导配置，从头开始
    initializeState();
  }

  /**
   * 初始化tc状态，创建根qdisc和系统预置类
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  private void initializeState() throws ResourceHandlerException {
    LOG.info("Initializing tc state.");

    // 使用BatchBuilder批量构建tc命令
    BatchBuilder builder = new BatchBuilder(PrivilegedOperation.
        OperationType.TC_MODIFY_STATE)
        .addRootQDisc()
        .addCGroupFilter()
        .addClassToRootQDisc(rootBandwidthMbit)
        .addDefaultClass(defaultClassBandwidthMbit, rootBandwidthMbit)
        // YARN带宽使用严格限制，rate等于ceil
        .addYARNRootClass(yarnBandwidthMbit, yarnBandwidthMbit);
    PrivilegedOperation op = builder.commitBatchToTempFile();

    try {
      privilegedOperationExecutor.executePrivilegedOperation(op, false);
    } catch (PrivilegedOperationException e) {
      LOG.warn("Failed to bootstrap outbound bandwidth configuration");

      throw new ResourceHandlerException(
          "Failed to bootstrap outbound bandwidth configuration", e);
    }
  }

  /**
   * 检查当前网卡是否已经完成了完整的tc引导配置
   * @param state 当前tc状态输出字符串
   * @return 已完成引导返回true，否则返回false
   */
  private boolean checkIfAlreadyBootstrapped(String state)
      throws ResourceHandlerException {
    List<String> regexes = new ArrayList<>();

    // 检查根qdisc是否存在
    regexes.add(String.format("^qdisc htb %d: root(.)*$",
        ROOT_QDISC_HANDLE));
    // 检查cgroup过滤器是否存在
    regexes.add(String.format("^filter parent %d: protocol ip " +
        "(.)*cgroup(.)*$", ROOT_QDISC_HANDLE));
    // 检查根类、默认类、YARN根类是否都存在
    regexes.add(String.format("^class htb %d:%d root(.)*$",
        ROOT_QDISC_HANDLE, ROOT_CLASS_ID));
    regexes.add(String.format("^class htb %d:%d parent %d:%d(.)*$",
        ROOT_QDISC_HANDLE, DEFAULT_CLASS_ID, ROOT_QDISC_HANDLE, ROOT_CLASS_ID));
    regexes.add(String.format("^class htb %d:%d parent %d:%d(.)*$",
        ROOT_QDISC_HANDLE, YARN_ROOT_CLASS_ID, ROOT_QDISC_HANDLE,
        ROOT_CLASS_ID));

    // 逐个正则匹配检查所有必需配置是否存在
    for (String regex : regexes) {
      Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);

      if (pattern.matcher(state).find()) {
        LOG.debug("Matched regex: {}", regex);
      } else {
        String logLine = new StringBuilder("Failed to match regex: ")
              .append(regex).append(" Current state: ").append(state).toString();
        LOG.warn(logLine);
        return false;
      }
    }

    LOG.info("Bootstrap check succeeded");

    return true;
  }

  /**
   * 读取当前网卡tc所有配置状态
   * @return tc命令输出的状态字符串
   * @throws ResourceHandlerException 读取失败抛出异常
   */
  private String readState() throws ResourceHandlerException {
    BatchBuilder builder = new BatchBuilder(PrivilegedOperation.
        OperationType.TC_READ_STATE)
        .readState();
    PrivilegedOperation op = builder.commitBatchToTempFile();

    try {
      String output =
          privilegedOperationExecutor.executePrivilegedOperation(op, true);

      LOG.debug("TC state: {}" + output);

      return output;
    } catch (PrivilegedOperationException e) {
      LOG.warn("Failed to bootstrap outbound bandwidth rules");
      throw new ResourceHandlerException(
          "Failed to bootstrap outbound bandwidth rules", e);
    }
  }

  /**
   * 清理当前网卡所有tc配置，恢复默认状态
   * @throws ResourceHandlerException 清理失败抛出异常
   */
  private void wipeState() throws ResourceHandlerException {
    BatchBuilder builder = new BatchBuilder(PrivilegedOperation.
        OperationType.TC_MODIFY_STATE)
        .wipeState();
    PrivilegedOperation op = builder.commitBatchToTempFile();

    try {
      LOG.info("Wiping tc state.");
      privilegedOperationExecutor.executePrivilegedOperation(op, false);
    } catch (PrivilegedOperationException e) {
      LOG.warn("Failed to wipe tc state. This could happen if the interface" +
          " is already in its default state. Ignoring.");
      // 网卡已经是默认状态时会执行失败，这种情况可以忽略，不抛出异常
    }
  }

  /**
   * 从NM恢复后的tc状态中重新获取已分配的容器类ID，恢复分配状态
   * @param state 当前tc状态输出字符串
   */
  private void reacquireContainerClasses(String state) {
    // 从状态中提取类信息部分
    String tcClassesStr = state.substring(state.indexOf("class"));
    // 按行分割类信息
    String[] tcClasses = Pattern.compile("$", Pattern.MULTILINE)
        .split(tcClassesStr);
    // 匹配tc类ID的正则
    Pattern tcClassPattern = Pattern.compile(String.format(
        "class htb %d:(\\d+) .*", ROOT_QDISC_HANDLE));

    synchronized (classIdSet) {
      for (String tcClassSplit : tcClasses) {
        String tcClass = tcClassSplit.trim();

        if (!tcClass.isEmpty()) {
          Matcher classMatcher = tcClassPattern.matcher(tcClass);
          if (classMatcher.matches()) {
            int classId = Integer.parseInt(classMatcher.group(1));
            // 只处理容器类ID，标记为已占用
            if (classId >= MIN_CONTAINER_CLASS_ID) {
              classIdSet.set(classId - MIN_CONTAINER_CLASS_ID);
              LOG.info("Reacquired container classid: " + classId);
            }
          } else {
            LOG.warn("Unable to match classid in string:" + tcClass);
          }
        }
      }
    }
  }

  /**
   * 读取所有容器类的流量统计信息
   * @return 类ID到已发送字节数的映射表
   * @throws ResourceHandlerException 读取统计失败抛出异常
   */
  public Map<Integer, Integer> readStats() throws ResourceHandlerException {
    BatchBuilder builder = new BatchBuilder(PrivilegedOperation.
        OperationType.TC_READ_STATS)
        .readClasses();
    PrivilegedOperation op = builder.commitBatchToTempFile();

    try {
      String output =
          privilegedOperationExecutor.executePrivilegedOperation(op, true);

      LOG.debug("TC stats output:{}", output);

      Map<Integer, Integer> classIdBytesStats = parseStatsString(output);

      LOG.debug("classId -> bytes sent {}", classIdBytesStats);

      return classIdBytesStats;
    } catch (PrivilegedOperationException e) {
      LOG.warn("Failed to get tc stats");
      throw new ResourceHandlerException("Failed to get tc stats", e);
    }
  }

  /**
   * 解析tc类统计输出，提取每个容器类的已发送字节数
   * @param stats tc命令输出的统计字符串
   * @return 类ID到已发送字节数的映射表
   */
  private Map<Integer, Integer> parseStatsString(String stats) {
    // 按行分割统计输出
    String[] lines = Pattern.compile("$", Pattern.MULTILINE)
        .split(stats);
    // 匹配tc类ID的正则
    Pattern tcClassPattern = Pattern.compile(String.format(
        "class htb %d:(\\d+) .*", ROOT_QDISC_HANDLE));
    // 匹配已发送字节数的正则
    Pattern bytesPattern = Pattern.compile("Sent (\\d+) bytes.*");

    int currentClassId = -1;
    Map<Integer, Integer> containerClassIdStats = new HashMap<>();

    for (String lineSplit : lines) {
      String line = lineSplit.trim();

      if (!line.isEmpty()) {
        // 检查是否是容器类行，更新当前处理的类ID
        Matcher classMatcher = tcClassPattern.matcher(line);
        if (classMatcher.matches()) {
          int classId = Integer.parseInt(classMatcher.group(1));
          if (classId >= MIN_CONTAINER_CLASS_ID) {
            currentClassId = classId;
            continue;
          }
        }

        // 检查是否是字节统计行，记录当前类的字节数
        Matcher bytesMatcher = bytesPattern.matcher(line);
        if (bytesMatcher.matches()) {
          if (currentClassId != -1) {
            int bytes = Integer.parseInt(bytesMatcher.group(1));
            containerClassIdStats.put(currentClassId, bytes);
          } else {