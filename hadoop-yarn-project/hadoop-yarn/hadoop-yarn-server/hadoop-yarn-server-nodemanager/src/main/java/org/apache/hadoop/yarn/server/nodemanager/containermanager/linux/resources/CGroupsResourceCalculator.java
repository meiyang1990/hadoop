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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于cgroup v1文件系统的资源计算器实现，不提供进程树统计功能。
 * 
 * 警告：该实现无法在通过mapreduce.job.process-tree.class配置使用，无法正常工作。
 * ResourceCalculatorProcessTree依赖NodeManager进程中初始化的ResourceHandlerModule，
 * 而该模块不会在MapReduce任务容器中初始化，因此无法在任务上下文使用该计算器。
 * 
 * 限制说明：
 * 尽管ResourceCalculatorProcessTree可以通过mapreduce.job.process-tree.class参数配置，
 * 但其实例运行在MapReduce任务上下文，无法访问仅在NodeManager进程初始化的ResourceHandlerModule，
 * 因此该实现与该参数不兼容，任何尝试通过该参数使用此类的操作都会失败。
 */
public class CGroupsResourceCalculator extends AbstractCGroupsResourceCalculator {
  private static final Logger LOG = LoggerFactory.getLogger(CGroupsResourceCalculator.class);

  /**
   * CPU统计文件，记录cgroup内CPU使用时间，分为用户态和内核态时间，单位为USER_HZ。
   */
  private static final String CPU_STAT = "cpuacct.stat";

  /**
   * 内存使用统计文件，单位为字节，受内核优化影响为近似值。
   */
  private static final String MEM_STAT = "memory.usage_in_bytes";
  /** 内存+交换区总使用量统计文件，单位为字节 */
  private static final String MEMSW_STAT = "memory.memsw.usage_in_bytes";

  /**
   * 构造函数，初始化指定进程的cgroup资源计算器
   * @param pid 目标进程ID
   */
  public CGroupsResourceCalculator(String pid) {
    super(
        pid,
        Arrays.asList(CPU_STAT + "#user", CPU_STAT + "#system"),
        MEM_STAT,
        MEMSW_STAT
    );
  }

  @Override
  protected List<Path> getCGroupFilesToLoadInStats() {
    List<Path> result = new ArrayList<>();

    try {
      // 获取当前进程在CPUACCT控制器下的相对路径
      String cpuRelative = getCGroupRelativePath(CGroupsHandler.CGroupController.CPUACCT);
      if (cpuRelative != null) {
        // 构造CPUACCT统计文件绝对路径
        File cpuDir = new File(getcGroupsHandler().getControllerPath(
            CGroupsHandler.CGroupController.CPUACCT), cpuRelative);
        result.add(Paths.get(cpuDir.getAbsolutePath(), CPU_STAT));
      }
    } catch (IOException e) {
      LOG.debug("Exception while looking for CPUACCT controller for pid: " + getPid(), e);
    }

    try {
      // 获取当前进程在MEMORY控制器下的相对路径
      String memoryRelative = getCGroupRelativePath(CGroupsHandler.CGroupController.MEMORY);
      if (memoryRelative != null) {
        // 构造内存统计文件绝对路径
        File memDir = new File(getcGroupsHandler().getControllerPath(
            CGroupsHandler.CGroupController.MEMORY), memoryRelative);
        result.add(Paths.get(memDir.getAbsolutePath(), MEM_STAT));
        result.add(Paths.get(memDir.getAbsolutePath(), MEMSW_STAT));
      }
    } catch (IOException e) {
      LOG.debug("Exception while looking for MEMORY controller for pid: " + getPid(), e);
    }

    return result;
  }

  /**
   * 从/proc/<pid>/cgroup文件中查询指定控制器下当前进程的cgroup相对路径
   * @param controller 目标cgroup控制器
   * @return cgroup相对路径，未找到则返回null
   * @throws IOException 读取/proc文件失败时抛出
   */
  private String getCGroupRelativePath(CGroupsHandler.CGroupController controller)
      throws IOException {
    // 遍历/proc/<pid>/cgroup文件的每一行
    for (String line : readLinesFromCGroupFileFromProcDir()) {
      // 示例行格式：6:cpuacct,cpu:/yarn/container_1
      String[] parts = line.split(":");
      // 判断当前行是否包含目标控制器
      if (parts[1].contains(controller.getName())) {
        String cgroupPath = parts[2];
        // 提取cgroup路径的最后一级文件名
        Path fileName = new File(cgroupPath).toPath().getFileName();
        if (fileName != null) {
          // 获取容器对应的cgroup相对路径并返回
          return getcGroupsHandler().getRelativePathForCGroup(fileName.toString());
        }
      }
    }
    LOG.debug("No {} controller found for pid {}", controller, getPid());
    return null;
  }
}