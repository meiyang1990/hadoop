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

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.CpuTimeTracker;
import org.apache.hadoop.util.SysInfoLinux;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.apache.hadoop.yarn.util.SystemClock;

/**
 * 基于控制组(cgroups)的资源计算器抽象基类，提供通用实现逻辑，支持不同版本cgroups的扩展。
 * 负责从cgroups文件中读取并计算容器进程的CPU、内存资源使用情况。
 */
public abstract class AbstractCGroupsResourceCalculator extends ResourceCalculatorProcessTree {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractCGroupsResourceCalculator.class);
  // 目标进程ID
  private final String pid;
  // 系统时钟，用于计算CPU时间间隔
  private final Clock clock = SystemClock.getInstance();
  // 存储从cgroups文件读取的统计数据，key为统计项名称，value为统计值
  private final Map<String, String> stats = new ConcurrentHashMap<>();

  // 系统每个时钟滴答(jiffy)对应的毫秒数
  private long jiffyLengthMs = SysInfoLinux.JIFFY_LENGTH_IN_MILLIS;
  // CPU时间跟踪器，用于计算CPU使用率
  private CpuTimeTracker cpuTimeTracker;
  // cgroups处理器，提供cgroups操作能力
  private CGroupsHandler cGroupsHandler;
  // proc文件系统挂载路径，默认为/proc
  private String procFs = "/proc";

  // 总CPU时间jiffy对应的统计项key列表，不同版本cgroups位置不同
  private final List<String> totalJiffiesKeys;
  // RSS常驻内存统计项key
  private final String rssMemoryKey;
  // 虚拟内存统计项key
  private final String virtualMemoryKey;

  /**
   * 构造函数，初始化cgroups资源计算器基础参数。
   * @param pid 目标进程ID
   * @param totalJiffiesKeys CPU总jiffies统计项key列表
   * @param rssMemoryKey RSS内存统计项key
   * @param virtualMemoryKey 虚拟内存统计项key
   */
  protected AbstractCGroupsResourceCalculator(
      String pid,
      List<String> totalJiffiesKeys,
      String rssMemoryKey,
      String virtualMemoryKey
  ) {
    super(pid);
    this.pid = pid;
    this.totalJiffiesKeys = totalJiffiesKeys;
    this.rssMemoryKey = rssMemoryKey;
    this.virtualMemoryKey = virtualMemoryKey;
  }

  @Override
  public void initialize() throws YarnException {
    // 初始化CPU时间跟踪器
    cpuTimeTracker = new CpuTimeTracker(jiffyLengthMs);
    // 获取CGroups处理器实例
    cGroupsHandler = ResourceHandlerModule.getCGroupsHandler();
  }

  @Override
  public long getCumulativeCpuTime() {
    long totalJiffies = getTotalJiffies();
    // 如果jiffy长度或总jiffies不可用，返回不可用标识
    return jiffyLengthMs == UNAVAILABLE || totalJiffies == UNAVAILABLE
        ? UNAVAILABLE
        : getTotalJiffies() * jiffyLengthMs;
  }

  @Override
  public long getRssMemorySize(int olderThanAge) {
    // 仅统计1个周期内的数据，超过则返回不可用
    return 1 < olderThanAge ? UNAVAILABLE : getStat(rssMemoryKey);
  }

  @Override
  public long getVirtualMemorySize(int olderThanAge) {
    // 仅统计1个周期内的数据，超过则返回不可用
    return 1 < olderThanAge ? UNAVAILABLE : getStat(virtualMemoryKey);
  }

  @Override
  public String getProcessTreeDump() {
    // cgroups不维护进程树结构，直接返回目标pid用于跟踪
    return pid;
  }

  @Override
  public boolean checkPidPgrpidForMatch() {
    // cgroups模式下不需要校验进程组匹配，默认返回成功
    return true;
  }

  @Override
  public float getCpuUsagePercent() {
    // 从CPU时间跟踪器获取当前CPU使用率
    return cpuTimeTracker.getCpuTrackerUsagePercent();
  }

  @Override
  public void updateProcessTree() {
    // 清空旧的统计数据
    stats.clear();
    // 遍历所有需要加载的cgroup统计文件
    for (Path statFile : getCGroupFilesToLoadInStats()) {
      try {
        List<String> lines = fileToLines(statFile);
        // 单行文件直接整体作为值存储
        if (1 == lines.size()) {
          addSingleLineToStat(statFile, lines.get(0));
        } else if (1 < lines.size()) {
          // 多行文件按行拆分存储键值对
          addMultiLineToStat(statFile, lines);
        }
      } catch (IOException e) {
        // 读取失败记录debug日志，不影响其他统计项
        LOG.debug(String.format("Failed to read cgroup file %s for pid %s", statFile, pid), e);
      }
    }
    LOG.debug("After updateProcessTree the {} pid has stats {}", pid, stats);
    // 更新CPU时间跟踪器，计算最新CPU使用率
    cpuTimeTracker.updateElapsedJiffies(BigInteger.valueOf(getTotalJiffies()), clock.getTime());
  }

  // 将单行统计文件内容添加到统计缓存
  private void addSingleLineToStat(Path file, String line) {
    Path fileName = file.getFileName();
    if (fileName != null) {
      stats.put(fileName.toString(), line.trim());
    }
  }

  // 将多行统计文件内容按行拆分添加到统计缓存
  private void addMultiLineToStat(Path file, List<String> lines) {
    for (String line : lines) {
      String[] parts = line.split(" ");
      if (1 < parts.length) {
        // 格式为 文件名#统计项 = 统计值
        stats.put(file.getFileName() + "#" + parts[0], parts[1]);
      }
    }
  }

  // 累加所有统计项得到总CPU jiffies
  private long getTotalJiffies() {
    Long reduce = totalJiffiesKeys.stream()
        .map(this::getStat)
        .filter(statValue -> statValue != UNAVAILABLE)
        .reduce(0L, Long::sum);
    // 累加结果为0说明无可用数据，返回不可用标识
    return reduce == 0 ? UNAVAILABLE : reduce;
  }

  // 从缓存获取指定统计项的数值，默认为不可用
  private long getStat(String key) {
    return Long.parseLong(stats.getOrDefault(key, String.valueOf(UNAVAILABLE)));
  }

  /**
   * 获取需要加载统计数据的cgroup文件列表，由具体子类实现。
   * @return 需要加载的cgroup文件路径列表
   */
  protected abstract List<Path> getCGroupFilesToLoadInStats();

  /**
   * 从/proc文件系统读取指定进程的cgroup配置文件内容。
   * @return cgroup配置文件行列表
   * @throws IOExceptions 读取文件失败时抛出
   */
  protected List<String> readLinesFromCGroupFileFromProcDir() throws IOException {
    // 参考内核文档：cgroup v1/v2进程cgroup路径定义
    Path cgroup = Paths.get(procFs, pid, "cgroup");
    List<String> result = Arrays.asList(fileToString(cgroup).split(System.lineSeparator()));
    LOG.debug("The {} pid has the following lines in the procfs cgroup file {}", pid, result);
    return result;
  }

  /**
   * 读取整个文件内容为字符串，使用UTF-8编码。
   * @param path 文件路径
   * @return 修剪后的文件内容字符串
   * @throws IOException 读取文件失败时抛出
   */
  protected String fileToString(Path path) throws IOException {
    return FileUtils.readFileToString(path.toFile(), StandardCharsets.UTF_8).trim();
  }

  /**
   * 读取文件内容按行拆分为列表。
   * @param path 文件路径
   * @return 文件行列表，文件不存在返回空列表
   * @throws IOException 读取文件失败时抛出
   */
  protected List<String> fileToLines(Path path) throws IOException {
    return !path.toFile().exists() ? Collections.emptyList()
      : Arrays.asList(FileUtils.readFileToString(path.toFile(), StandardCharsets.UTF_8)
        .trim().split(System.lineSeparator()));
  }

  @VisibleForTesting
  void setJiffyLengthMs(long jiffyLengthMs) {
    this.jiffyLengthMs = jiffyLengthMs;
  }

  @VisibleForTesting
  void setCpuTimeTracker(CpuTimeTracker cpuTimeTracker) {
    this.cpuTimeTracker = cpuTimeTracker;
  }

  @VisibleForTesting
  void setcGroupsHandler(CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
  }

  @VisibleForTesting
  void setProcFs(String procFs) {
    this.procFs = procFs;
  }

  public CGroupsHandler getcGroupsHandler() {
    return cGroupsHandler;
  }

  public String getPid() {
    return pid;
  }
}