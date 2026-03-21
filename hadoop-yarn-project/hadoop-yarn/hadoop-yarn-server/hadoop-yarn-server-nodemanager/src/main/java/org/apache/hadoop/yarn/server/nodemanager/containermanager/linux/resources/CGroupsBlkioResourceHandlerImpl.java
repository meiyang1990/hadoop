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

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * cgroup块IO控制器处理器，实现磁盘IO资源隔离。当前实现为所有容器平分IO权重，
 * 未来支持磁盘调度后可根据分配的磁盘资源动态调整权重。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CGroupsBlkioResourceHandlerImpl implements DiskResourceHandler {

  static final Logger LOG =
       LoggerFactory.getLogger(CGroupsBlkioResourceHandlerImpl.class);

  private CGroupsHandler cGroupsHandler;
  // 所有容器使用相同默认权重，保证IO均分，未来会动态计算每个容器权重
  @VisibleForTesting
  static final String DEFAULT_WEIGHT = "500";
  // 系统分区信息文件路径
  private static final String PARTITIONS_FILE = "/proc/partitions";

  /**
   * 构造cgroup块IO资源处理器，仅在Linux系统下检查磁盘调度器配置
   * @param cGroupsHandler cgroup处理器实例
   */
  CGroupsBlkioResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
    // 仅在Linux系统执行检查，避免非Linux测试环境输出警告信息
    if(Shell.LINUX) {
      checkDiskScheduler();
    }
  }


  /**
   * 检查系统磁盘分区调度器是否为CFQ，CFQ是cgroup blkio隔离工作的前提，若不是则输出警告
   */
  private void checkDiskScheduler() {
    String data;

    // 读取/proc/partitions获取所有分区信息，检查常见分区是否使用CFQ调度器
    try {
      // 读取分区文件全部内容
      byte[] contents = Files.readAllBytes(Paths.get(PARTITIONS_FILE));
      data = new String(contents, StandardCharsets.UTF_8).trim();
    } catch (IOException e) {
      String msg = "Couldn't read " + PARTITIONS_FILE +
          "; can't determine disk scheduler type";
      LOG.warn(msg, e);
      return;
    }
    // 按行分割分区信息
    String[] lines = data.split(System.lineSeparator());
    if (lines.length > 0) {
      // 遍历所有分区行
      for (String line : lines) {
        // 按空白分割列
        String[] columns = line.split("\\s+");
        if (columns.length > 4) {
          // 获取分区名称
          String partition = columns[4];
          // 只检查常见磁盘分区类型，做基本完整性检查
          if (partition.startsWith("sd") || partition.startsWith("hd")
              || partition.startsWith("vd") || partition.startsWith("xvd")) {
            // 拼接该分区调度器文件路径
            String schedulerPath =
                "/sys/block/" + partition + "/queue/scheduler";
            File schedulerFile = new File(schedulerPath);
            if (schedulerFile.exists()) {
              try {
                // 读取当前调度器配置
                byte[] contents = Files.readAllBytes(Paths.get(schedulerPath));
                String schedulerString = new String(contents, StandardCharsets.UTF_8).trim();
                // 检查是否启用CFQ调度器，否则输出警告
                if (!schedulerString.contains("[cfq]")) {
                  LOG.warn("Device " + partition + " does not use the CFQ"
                      + " scheduler; disk isolation using "
                      + "CGroups will not work on this partition.");
                }
              } catch (IOException ie) {
                LOG.warn(
                    "Unable to determine disk scheduler type for partition "
                      + partition, ie);
              }
            }
          }
        }
      }
    }
  }

  @Override
  /**
   * 初始化块IO cgroup控制器，完成资源处理器启动
   */
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    // 磁盘资源已启用，无需重复检查，直接初始化blkio控制器
    this.cGroupsHandler
      .initializeCGroupController(CGroupsHandler.CGroupController.BLKIO);
    return null;
  }

  @Override
  /**
   * 容器启动前为容器创建blkio cgroup，设置默认IO权重，准备将容器进程加入cgroup
   */
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {

    String cgroupId = container.getContainerId().toString();
    // 为容器创建blkio层级cgroup
    cGroupsHandler
      .createCGroup(CGroupsHandler.CGroupController.BLKIO, cgroupId);
    try {
      // 设置默认IO权重
      cGroupsHandler.updateCGroupParam(CGroupsHandler.CGroupController.BLKIO,
          cgroupId, CGroupsHandler.CGROUP_PARAM_WEIGHT, DEFAULT_WEIGHT);
    } catch (ResourceHandlerException re) {
      // 更新失败清理已创建的cgroup
      cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.BLKIO,
          cgroupId);
      LOG.warn("Could not update cgroup for container", re);
      throw re;
    }
    List<PrivilegedOperation> ret = new ArrayList<>();
    // 添加将容器PID加入cgroup的特权操作
    ret.add(new PrivilegedOperation(
      PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
      PrivilegedOperation.CGROUP_ARG_PREFIX
          + cGroupsHandler.getPathForCGroupTasks(
            CGroupsHandler.CGroupController.BLKIO, cgroupId)));
    return ret;
  }

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

  @Override
  /**
   * 容器完成后删除容器的blkio cgroup，释放资源
   */
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.BLKIO,
        containerId.toString());
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown() throws ResourceHandlerException {
    return null;
  }

  @Override
  public String toString() {
    return CGroupsBlkioResourceHandlerImpl.class.getName();
  }
}