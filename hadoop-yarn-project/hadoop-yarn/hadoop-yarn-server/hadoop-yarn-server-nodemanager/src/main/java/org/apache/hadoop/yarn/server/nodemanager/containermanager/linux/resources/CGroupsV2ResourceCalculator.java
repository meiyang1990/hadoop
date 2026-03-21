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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;

/**
 * 基于cgroup v2文件系统的资源计算器实现，不支持进程树特性
 *
 * 警告：该实现在配置mapreduce.job.process-tree.class作业属性时无法正常工作。
 * 理论上ResourceCalculatorProcessTree可通过mapreduce.job.process-tree.class作业属性配置，但它依赖ResourceHandlerModule实例，
 * 而该模块仅在NodeManager进程中初始化，不会在容器内部初始化。
 *
 * 限制说明：
 * ResourceCalculatorProcessTree类可通过MapReduce作业中的mapreduce.job.process-tree.class属性配置，
 * 但需要注意该类实例运行在MapReduce任务上下文内，无法访问仅在NodeManager进程初始化、不会在容器进程中初始化的ResourceHandlerModule，
 * 因此当前实现与mapreduce.job.process-tree.class属性不兼容。由于ResourceHandlerModule是资源使用监控管理的核心依赖，
 * 无法在MapReduce任务上下文中正常工作，任何尝试通过mapreduce.job.process-tree.class属性使用该类的操作都会失败。
 */
public class CGroupsV2ResourceCalculator extends AbstractCGroupsResourceCalculator {
  private static final Logger LOG = LoggerFactory.getLogger(CGroupsV2ResourceCalculator.class);

  /**
   * CPU使用率统计文件路径，格式为文件名#关键字，从cpu.stat中读取usage_usec获取总CPU使用时间
   */
  private static final String CPU_STAT = "cpu.stat#usage_usec";

  /**
   * 内存统计文件路径，格式为文件名#关键字，从memory.stat中读取anon获取匿名映射内存使用量
   */
  private static final String MEM_STAT = "memory.stat#anon";

  /**
   * 交换空间使用统计文件路径，直接从memory.swap.current读取当前cgroup总交换空间使用量
   */
  private static final String MEMSW_STAT = "memory.swap.current";

  /**
   * 构造函数，初始化cgroup v2资源计算器
   * @param pid 目标进程ID
   */
  public CGroupsV2ResourceCalculator(String pid) {
    super(
        pid,
        Collections.singletonList(CPU_STAT),
        MEM_STAT,
        MEMSW_STAT
    );
  }

  @Override
  protected List<Path> getCGroupFilesToLoadInStats() {
    List<Path> result = new ArrayList<>();
    // 遍历cgroup目录下所有文件，批量收集需要读取的统计文件
    try (Stream<Path> cGroupFiles = Files.list(getCGroupPath())){
      cGroupFiles.forEach(result::add);
    } catch (IOException e) {
      LOG.debug("Failed to list cgroup files for pid: " + getPid(), e);
    }
    LOG.debug("Found cgroup files for pid {} is {}", getPid(), result);
    return  result;
  }

  /**
   * 获取当前进程对应的cgroup v2根路径
   * @return 当前进程cgroup v2绝对路径
   * @throws IOException 读取/proc文件获取cgroup路径失败时抛出
   */
  private Path getCGroupPath() throws IOException {
    return Paths.get(
        getcGroupsHandler().getCGroupV2MountPath(),
        StringUtils.substringAfterLast(readLinesFromCGroupFileFromProcDir().get(0), ":")
    );
  }
}