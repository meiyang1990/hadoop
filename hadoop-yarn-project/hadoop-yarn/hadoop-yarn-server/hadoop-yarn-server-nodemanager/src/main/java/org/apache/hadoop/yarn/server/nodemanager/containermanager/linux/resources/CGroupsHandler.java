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

import java.util.HashSet;
import java.util.Set;

/**
 * Linux Cgroups 控制接口，提供Cgroups操作的统一抽象，所有实现必须保证线程安全
 * 用于YARN NodeManager对容器进行Linux内核Cgroups资源隔离
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface CGroupsHandler {

  /**
   * 支持的Cgroup控制器类型枚举，两个标识分别表示该控制器是否在v1/v2版本中有效
   */
  enum CGroupController {
    NET_CLS("net_cls", true, false),
    BLKIO("blkio", true, false),
    CPUACCT("cpuacct", true, false),
    FREEZER("freezer", true, false),
    DEVICES("devices", true, false),

    // v2 specific
    IO("io", false, true),

    // present in v1 and v2
    CPU("cpu", true, true),
    CPUSET("cpuset", true, true),
    MEMORY("memory", true, true);

    private final String name;
    private final boolean inV1;
    private final boolean inV2;

    CGroupController(String name, boolean inV1, boolean inV2) {
      this.name = name;
      this.inV1 = inV1;
      this.inV2 = inV2;
    }

    public String getName() {
      return name;
    }

    public boolean isInV1() {
      return inV1;
    }

    public boolean isInV2() {
      return inV2;
    }

    /**
     * 获取所有对cgroup v1有效的控制器名称集合
     * @return v1版本有效的控制器名称集合
     */
    public static Set<String> getValidV1CGroups() {
      HashSet<String> validCgroups = new HashSet<>();
      for (CGroupController controller : CGroupController.values()) {
        if (controller.isInV1()) {
          validCgroups.add(controller.getName());
        }
      }
      return validCgroups;
    }

    /**
     * 获取所有对cgroup v2有效的控制器名称集合
     * @return v2版本有效的控制器名称集合
     */
    public static Set<String> getValidV2CGroups() {
      HashSet<String> validCgroups = new HashSet<>();
      for (CGroupController controller : CGroupController.values()) {
        if (controller.isInV2()) {
          validCgroups.add(controller.getName());
        }
      }
      return validCgroups;
    }
  }

  // Cgroup v1 特定参数名称
  String CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES = "limit_in_bytes";
  String CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES = "memsw.limit_in_bytes";
  String CGROUP_PARAM_MEMORY_SOFT_LIMIT_BYTES = "soft_limit_in_bytes";
  String CGROUP_PARAM_MEMORY_OOM_CONTROL = "oom_control";
  String CGROUP_PARAM_MEMORY_SWAPPINESS = "swappiness";
  String CGROUP_PARAM_MEMORY_USAGE_BYTES = "usage_in_bytes";
  String CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES = "memsw.usage_in_bytes";
  String CGROUP_NO_LIMIT = "-1";
  String UNDER_OOM = "under_oom 1";
  String CGROUP_CPU_PERIOD_US = "cfs_period_us";
  String CGROUP_CPU_QUOTA_US = "cfs_quota_us";
  String CGROUP_CPU_SHARES = "shares";

  // Cgroup v2 特定参数名称
  String CGROUP_CONTROLLERS_FILE = "cgroup.controllers";
  String CGROUP_SUBTREE_CONTROL_FILE = "cgroup.subtree_control";
  String CGROUP_CPU_MAX = "max";
  String CGROUP_MEMORY_MAX = "max";
  String CGROUP_MEMORY_LOW = "low";

  // Cgroup v1 和 v2 通用参数名称
  String CGROUP_PROCS_FILE = "cgroup.procs";
  String CGROUP_PARAM_CLASSID = "classid";
  String CGROUP_PARAM_WEIGHT = "weight";

  /**
   * 挂载并初始化指定的cgroup控制器
   * @param controller 要初始化的控制器
   * @throws ResourceHandlerException 环境异常导致初始化失败
   */
  void initializeCGroupController(CGroupController controller)
      throws ResourceHandlerException;

  /**
   * 为指定控制器创建一个新的cgroup
   * @param controller 创建cgroup所用的控制器类型
   * @param cGroupId 要创建的cgroup ID
   * @return 创建好的cgroup的完整路径
   * @throws ResourceHandlerException 创建失败
   */
  String createCGroup(CGroupController controller, String cGroupId)
      throws ResourceHandlerException;

  /**
   * 删除指定的cgroup
   * @param controller cgroup所属控制器类型
   * @param cGroupId 要删除的cgroup ID
   * @throws ResourceHandlerException 删除失败
   */
  void deleteCGroup(CGroupController controller, String cGroupId) throws
      ResourceHandlerException;

  /**
   * 获取指定控制器的根路径
   * @param controller 控制器类型
   * @return 控制器根路径
   */
  String getControllerPath(CGroupController controller);

  /**
   * 根据当前使用的cgroup版本，获取所有有效的控制器名称集合
   * @return 当前版本有效的控制器名称集合
   */
  Set<String> getValidCGroups();

  /**
   * 根据cgroup ID获取相对于控制器根路径的相对路径
   * @param cGroupId cgroup ID
   * @return 相对于任意控制器根的相对路径
   */
  String getRelativePathForCGroup(String cGroupId);

  /**
   * 根据控制器和cgroup ID获取cgroup的完整路径
   * @param controller cgroup所属控制器类型
   * @param cGroupId cgroup ID
   * @return cgroup完整路径
   */
  String getPathForCGroup(CGroupController controller, String
      cGroupId);

  /**
   * 根据控制器和cgroup ID获取cgroup进程文件的完整路径
   * @param controller cgroup所属控制器类型
   * @param cGroupId cgroup ID
   * @return cgroup进程文件完整路径
   */
  String getPathForCGroupTasks(CGroupController controller, String
      cGroupId);

  /**
   * 根据控制器、cgroup ID和参数名称获取参数文件的完整路径
   * @param controller cgroup所属控制器类型
   * @param cGroupId cgroup ID
   * @param param cgroup参数名称，例如classid
   * @return cgroup参数文件完整路径
   */
  String getPathForCGroupParam(CGroupController controller, String
      cGroupId, String param);

  /**
   * 更新cgroup参数值
   * @param controller cgroup所属控制器类型
   * @param cGroupId cgroup ID
   * @param param cgroup参数名称，例如classid
   * @param value 要写入参数文件的值
   * @throws ResourceHandlerException 操作失败
   */
  void updateCGroupParam(CGroupController controller, String cGroupId,
      String param, String value) throws ResourceHandlerException;

  /**
   * 读取cgroup参数值
   * @param controller cgroup所属控制器类型
   * @param cGroupId cgroup ID
   * @param param cgroup参数名称，例如classid
   * @return 从参数文件读取到的参数值
   * @throws ResourceHandlerException 操作失败
   */
  String getCGroupParam(CGroupController controller, String cGroupId,
      String param) throws ResourceHandlerException;

  /**
   * 获取Cgroup根挂载路径
   * @return Cgroup根挂载路径
   */
  String getCGroupMountPath();

  /**
   * 获取Cgroup v2根挂载路径
   * @return Cgroup v2根挂载路径
   */
  String getCGroupV2MountPath();
}