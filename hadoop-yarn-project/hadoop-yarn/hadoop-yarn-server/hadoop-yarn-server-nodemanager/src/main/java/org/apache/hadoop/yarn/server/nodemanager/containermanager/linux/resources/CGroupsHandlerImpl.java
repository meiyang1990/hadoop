// 这个文件已经全部加上中文注释
/*
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
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * cgroup v1 子系统操作处理器实现，线程安全，为YARN NodeManager提供cgroup v1资源隔离的底层操作能力
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
class CGroupsHandlerImpl extends AbstractCGroupsHandler {
  private static final Logger LOG =
          LoggerFactory.getLogger(CGroupsHandlerImpl.class);
  // cgroup文件系统类型标识
  private static final String CGROUP_FSTYPE = "cgroup";

  /**
   * 构造cgroup v1处理器
   * @param conf 配置对象
   * @param privilegedOperationExecutor 特权操作执行器，用于执行需要root权限的操作
   * @param mtab 挂载信息文件路径
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  CGroupsHandlerImpl(Configuration conf, PrivilegedOperationExecutor
          privilegedOperationExecutor, String mtab)
          throws ResourceHandlerException {
    super(conf, privilegedOperationExecutor, mtab);
  }

  /**
   * 构造cgroup v1处理器，使用默认mtab路径
   * @param conf 配置对象
   * @param privilegedOperationExecutor 特权操作执行器，用于执行需要root权限的操作
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  CGroupsHandlerImpl(Configuration conf, PrivilegedOperationExecutor
          privilegedOperationExecutor) throws ResourceHandlerException {
    this(conf, privilegedOperationExecutor, MTAB_FILE);
  }

  @Override
  public Set<String> getValidCGroups() {
    // 获取cgroup v1支持的所有控制器名称
    return CGroupController.getValidV1CGroups();
  }

  @Override
  protected List<CGroupController> getCGroupControllers() {
    // 过滤得到所有属于cgroup v1的控制器
    return Arrays.stream(CGroupController.values()).filter(CGroupController::isInV1)
        .collect(Collectors.toList());
  }

  @Override
  protected Map<String, Set<String>> parsePreConfiguredMountPath() throws IOException {
    // 解析用户预先配置的cgroup挂载路径
    return ResourceHandlerModule.
            parseConfiguredCGroupPath(this.cGroupsMountConfig.getMountPath());
  }

  @Override
  protected Set<String> handleMtabEntry(String path, String type, String options) {
    // 获取所有合法的cgroup v1控制器名称
    Set<String> validCgroups = getValidCGroups();

    if (type.equals(CGROUP_FSTYPE)) {
      // 拆分挂载选项得到该挂载点包含的控制器列表
      Set<String> controllerSet =
              new HashSet<>(Arrays.asList(options.split(",")));
      // 只保留当前YARN支持的控制器
      controllerSet.retainAll(validCgroups);
      return controllerSet;
    }

    // 非cgroup类型挂载点返回空
    return null;
  }

  @Override
  protected void mountCGroupController(CGroupController controller)
          throws ResourceHandlerException {
    // 获取该控制器当前已存在的挂载路径
    String existingMountPath = getControllerPath(controller);
    // 计算配置要求的目标挂载路径
    String requestedMountPath =
            new File(cGroupsMountConfig.getMountPath(),
                    controller.getName()).getAbsolutePath();

    if (!requestedMountPath.equals(existingMountPath)) {
      // 获取写锁，防止并发修改挂载信息
      rwLock.writeLock().lock();
      try {
        // 如果控制器已存在挂载，需要复用原有挂载选项
        String mountOptions;
        if (existingMountPath != null) {
          // 从已解析的mtab信息中拼接原有挂载选项
          mountOptions = Joiner.on(',')
                  .join(parsedMtab.get(existingMountPath));
        } else {
          // 新挂载直接使用控制器名称作为选项
          mountOptions = controller.getName();
        }

        // 构造挂载参数
        String cGroupKV =
                mountOptions + "=" + requestedMountPath;
        PrivilegedOperation.OperationType opType = PrivilegedOperation
                .OperationType.MOUNT_CGROUPS;
        // 创建挂载cgroup的特权操作
        PrivilegedOperation op = new PrivilegedOperation(opType);

        // 添加挂载参数
        op.appendArgs(cGroupPrefix, cGroupKV);
        LOG.info("Mounting controller " + controller.getName() + " at " +
                requestedMountPath);
        // 执行特权挂载操作
        privilegedOperationExecutor.executePrivilegedOperation(op, false);

        // 挂载成功更新控制器路径缓存
        controllerPaths.put(controller, requestedMountPath);
      } catch (PrivilegedOperationException e) {
        LOG.error("Failed to mount controller: " + controller.getName());
        throw new ResourceHandlerException("Failed to mount controller: "
                + controller.getName());
      } finally {
        // 释放写锁
        rwLock.writeLock().unlock();
      }
    } else {
      // 已挂载到正确路径，无需操作
      LOG.info("CGroup controller already mounted at: " + existingMountPath);
    }
  }

  @Override
  protected void updateEnabledControllersInHierarchy(
      File yarnHierarchy, CGroupController controller) {
    // cgroup v1不需要此操作，留空实现
  }
}