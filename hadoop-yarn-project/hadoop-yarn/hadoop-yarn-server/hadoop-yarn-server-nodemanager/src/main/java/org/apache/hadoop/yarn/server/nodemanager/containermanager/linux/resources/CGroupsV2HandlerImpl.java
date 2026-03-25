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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * cgroup v2 子系统交互处理器，线程安全，负责处理YARN NodeManager上容器的cgroup v2资源控制
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
class CGroupsV2HandlerImpl extends AbstractCGroupsHandler {
  private static final Logger LOG =
          LoggerFactory.getLogger(CGroupsV2HandlerImpl.class);

  // cgroup v2 文件系统类型标识
  private static final String CGROUP2_FSTYPE = "cgroup2";

  /**
   * 构造cgroup v2处理器对象
   * @param conf YARN配置对象
   * @param privilegedOperationExecutor 特权操作执行器，用于执行需要root权限的操作
   * @param mtab 挂载表文件路径
   * @throws ResourceHandlerException 初始化失败时抛出
   */
  CGroupsV2HandlerImpl(Configuration conf, PrivilegedOperationExecutor
          privilegedOperationExecutor, String mtab)
          throws ResourceHandlerException {
    super(conf, privilegedOperationExecutor, mtab);
  }

  /**
   * 构造cgroup v2处理器对象，使用默认mtab路径
   * @param conf YARN配置对象
   * @param privilegedOperationExecutor 特权操作执行器，用于执行需要root权限的操作
   * @throws ResourceHandlerException 初始化失败时抛出
   */
  CGroupsV2HandlerImpl(Configuration conf, PrivilegedOperationExecutor
          privilegedOperationExecutor) throws ResourceHandlerException {
    this(conf, privilegedOperationExecutor, MTAB_FILE);
  }

  @Override
  public Set<String> getValidCGroups() {
    // 返回YARN支持的所有cgroup v2控制器名称
    return CGroupController.getValidV2CGroups();
  }

  @Override
  protected List<CGroupController> getCGroupControllers() {
    // 过滤出所有支持v2的cgroup控制器，返回列表
    return Arrays.stream(CGroupController.values()).filter(CGroupController::isInV2)
            .collect(Collectors.toList());
  }

  @Override
  protected Map<String, Set<String>> parsePreConfiguredMountPath() {
    // 存储挂载路径到控制器集合的映射
    Map<String, Set<String>> controllerMappings = new HashMap<>();
    try {
      // 读取预配置挂载路径下的控制器文件，添加到映射
      controllerMappings.put(this.cGroupsMountConfig.getV2MountPath(),
          readControllersFile(this.cGroupsMountConfig.getV2MountPath()));
    } catch (IOException e) {
      // 读取预配置路径下的cgroup.controllers失败，可能是当前节点使用cgroup v1或未挂载v2
      // 在ResourceHandlerModule.initializeCGroupHandlers会自动回退到v1，都不存在才会最终失败
      LOG.info("Failed to read the cgroup controllers file in the preconfigured directory: {}. " +
          "The cgroup v2 hierarchy is not mounted under the specified path, or the node" +
          " might be using cgroup v1.", this.cGroupsMountConfig.getV2MountPath());
      LOG.debug("Exception while reading the cgroup.controllers file: ", e);
    }
    return controllerMappings;
  }

  @Override
  protected Set<String> handleMtabEntry(String path, String type, String options)
      throws IOException {
    // 如果是cgroup2类型的挂载项，读取该路径下可用控制器
    if (type.equals(CGROUP2_FSTYPE)) {
      return readControllersFile(path);
    }

    return null;
  }

  @Override
  protected void mountCGroupController(CGroupController controller) {
    // cgroup v2统一挂载，不支持单独挂载单个控制器，抛出不支持操作异常
    throw new UnsupportedOperationException("Mounting cgroup controllers is not supported in " +
        "cgroup v2");
  }

  /**
   * 解析cgroup.controllers文件，获取已启用且YARN支持的控制器集合
   * @param cgroupPath cgroup根目录路径
   * @return 已启用且YARN支持的控制器名称集合
   * @throws IOException 文件不存在或读取失败时抛出
   */
  public Set<String> readControllersFile(String cgroupPath) throws IOException {
    // 构造cgroup.controllers文件对象
    File cgroupControllersFile = new File(cgroupPath + Path.SEPARATOR + CGROUP_CONTROLLERS_FILE);
    if (!cgroupControllersFile.exists()) {
      throw new IOException("No cgroup controllers file found in the directory specified: " +
              cgroupPath);
    }

    // 读取文件内容，按空格分割得到所有已启用控制器
    String enabledControllers = FileUtils.readFileToString(cgroupControllersFile,
        StandardCharsets.UTF_8);
    Set<String> validCGroups = getValidCGroups();
    Set<String> controllerSet =
            new HashSet<>(Arrays.asList(enabledControllers.split(" ")));
    // 仅保留YARN支持的控制器
    controllerSet.retainAll(validCGroups);
    if (controllerSet.isEmpty()) {
      LOG.warn("The following cgroup directory doesn't contain any supported controllers: " +
              cgroupPath);
    }

    return controllerSet;
  }

  /**
   * 更新YARN cgroup层级中的subtree_control文件，启用子树控制器
   * cgroup v2中，cgroup.subtree_control用于控制子cgroup可用的控制器，YARN为每个容器创建子cgroup
   * 需要提前在父层级启用对应控制器，容器才能使用资源限制功能
   * @param yarnHierarchy YARN根cgroup目录路径，容器cgroup都会创建在此目录下
   * @param controller 需要启用的cgroup控制器
   * @throws ResourceHandlerException 更新失败时抛出
   */
  @Override
  protected void updateEnabledControllersInHierarchy(
      File yarnHierarchy, CGroupController controller) throws ResourceHandlerException {
    try {
      // 读取当前层级已启用的控制器列表
      Set<String> enabledControllers = readControllersFile(yarnHierarchy.getAbsolutePath());
      if (!enabledControllers.contains(controller.getName())) {
        // 目标控制器未启用，抛出异常提示用户配置
        String errorMsg = String.format(
            "The controller %s is not enabled in the cgroup hierarchy: %s. Please enable it in " +
                "in the %s/cgroup.subtree_control file.",
            controller.getName(), yarnHierarchy.getAbsolutePath(),
            yarnHierarchy.getParentFile().getAbsolutePath());

        throw new ResourceHandlerException(getErrorWithDetails(
            errorMsg, controller.getName(),
            yarnHierarchy.getAbsolutePath()));
      }

      // 构造subtree_control文件对象
      File subtreeControlFile = new File(yarnHierarchy.getAbsolutePath()
          + Path.SEPARATOR + CGROUP_SUBTREE_CONTROL_FILE);
      if (!subtreeControlFile.exists()) {
        String errorMsg = "No subtree control file found in the cgroup hierarchy: " +
            yarnHierarchy.getAbsolutePath();
        throw new ResourceHandlerException(getErrorWithDetails(
            errorMsg, controller.getName(),
            yarnHierarchy.getAbsolutePath()));
      }
      if (!subtreeControlFile.canWrite()) {
        String errorMsg = "Cannot write the cgroup.subtree_control file in the " +
            "cgroup hierarchy: " + yarnHierarchy.getAbsolutePath();
        throw new ResourceHandlerException(getErrorWithDetails(
            errorMsg, controller.getName(),
            yarnHierarchy.getAbsolutePath()));
      }

      // 以追加模式打开文件，准备写入
      Writer w = new OutputStreamWriter(Files.newOutputStream(subtreeControlFile.toPath(),
          StandardOpenOption.APPEND), StandardCharsets.UTF_8);
      try(PrintWriter pw = new PrintWriter(w)) {
        LOG.info("Appending the following controller to the cgroup.subtree_control file: {}, " +
                "for the cgroup hierarchy: {}", controller.getName(),
            yarnHierarchy.getAbsolutePath());
        // 写入+控制器名称，表示启用该控制器到子树
        pw.write("+" + controller.getName());
        if (pw.checkError()) {
          String errorMsg = "Failed to add the controller to the " +
              "cgroup.subtree_control file in the cgroup hierarchy: " +
              yarnHierarchy.getAbsolutePath();
          throw new ResourceHandlerException(getErrorWithDetails(
              errorMsg, controller.getName(),
              yarnHierarchy.getAbsolutePath()));
        }
      }
    } catch (IOException e) {
      String errorMsg = "Failed to update the cgroup.subtree_control file in the " +
          "cgroup hierarchy: " + yarnHierarchy.getAbsolutePath();
      throw new ResourceHandlerException(getErrorWithDetails(
          errorMsg, controller.getName(),
          yarnHierarchy.getAbsolutePath()));
    }
  }
}