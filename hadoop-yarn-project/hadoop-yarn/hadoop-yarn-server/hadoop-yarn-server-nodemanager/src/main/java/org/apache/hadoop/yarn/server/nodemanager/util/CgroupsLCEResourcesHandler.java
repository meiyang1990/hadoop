// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsMountConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsCpuResourceHandlerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.SystemClock;

/**
 * 基于cgroups的Linux容器执行器资源处理器，用于实现CPU隔离。
 * 已废弃，请使用{@link ResourceHandlerModule}和{@link CGroupsCpuResourceHandlerImpl}，
 * 新实现支持使用cgroups隔离多种资源。
 * Deprecated - please look at ResourceHandlerModule and
 * CGroupsCpuResourceHandlerImpl
 */
@Deprecated
public class CgroupsLCEResourcesHandler implements LCEResourcesHandler {

  final static Logger LOG =
       LoggerFactory.getLogger(CgroupsLCEResourcesHandler.class);

  private Configuration conf;
  private String cgroupPrefix;
  private CGroupsMountConfig cGroupsMountConfig;

  private boolean cpuWeightEnabled = true;
  private boolean strictResourceUsageMode = false;

  private final String MTAB_FILE = "/proc/mounts";
  private final String CGROUPS_FSTYPE = "cgroup";
  private final String CONTROLLER_CPU = "cpu";
  private final String CPU_PERIOD_US = "cfs_period_us";
  private final String CPU_QUOTA_US = "cfs_quota_us";
  private final int CPU_DEFAULT_WEIGHT = 1024; // set by kernel
  private final Map<String, String> controllerPaths; // Controller -> path

  private long deleteCgroupTimeout;
  private long deleteCgroupDelay;
  @VisibleForTesting
  Clock clock;

  private float yarnProcessors;
  private int nodeVCores;

  public CgroupsLCEResourcesHandler() {
    this.controllerPaths = new HashMap<String, String>();
    clock = SystemClock.getInstance();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * 从配置中初始化参数。
   * @throws IOException 初始化失败时抛出IO异常
   */
  @VisibleForTesting
  void initConfig() throws IOException {

    this.cgroupPrefix = conf.get(YarnConfiguration.
            NM_LINUX_CONTAINER_CGROUPS_HIERARCHY, "/hadoop-yarn");
    this.cGroupsMountConfig = new CGroupsMountConfig(conf);
    this.deleteCgroupTimeout = conf.getLong(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT,
        YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT);
    this.deleteCgroupDelay =
        conf.getLong(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_DELETE_DELAY,
            YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_DELAY);
    // 去除cgroup前缀首尾多余的斜杠
    if (cgroupPrefix.charAt(0) == '/') {
      cgroupPrefix = cgroupPrefix.substring(1);
    }

    this.strictResourceUsageMode =
        conf
          .getBoolean(
            YarnConfiguration
                .NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE,
            YarnConfiguration
                .DEFAULT_NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE);

    int len = cgroupPrefix.length();
    if (cgroupPrefix.charAt(len - 1) == '/') {
      cgroupPrefix = cgroupPrefix.substring(0, len - 1);
    }
  }
  
  /**
   * 初始化cgroups资源处理器。
   * @param lce Linux容器执行器实例
   * @throws IOException 初始化失败时抛出IO异常
   */
  public void init(LinuxContainerExecutor lce) throws IOException {
    this.init(lce,
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf));
  }

  /**
   * 初始化cgroups资源处理器，支持传入硬件资源计算器。
   * @param lce Linux容器执行器实例
   * @param plugin 硬件资源计算器插件
   * @throws IOException 初始化失败时抛出IO异常
   */
  @VisibleForTesting
  void init(LinuxContainerExecutor lce, ResourceCalculatorPlugin plugin)
      throws IOException {
    initConfig();

    // 如果配置要求，挂载cgroups
    if (cGroupsMountConfig.mountEnabledAndMountPathDefined()) {
      ArrayList<String> cgroupKVs = new ArrayList<String>();
      cgroupKVs.add(CONTROLLER_CPU + "=" +
          cGroupsMountConfig.getMountPath() + "/" + CONTROLLER_CPU);
      lce.mountCgroups(cgroupKVs, cgroupPrefix);
    }

    // 初始化各cgroup控制器路径
    initializeControllerPaths();

    // 获取节点总vcore数量
    nodeVCores = NodeManagerHardwareUtils.getVCores(plugin, conf);

    // 获取分配给YARN容器的总CPU核数
    yarnProcessors = NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
    int systemProcessors = NodeManagerHardwareUtils.getNodeCPUs(plugin, conf);
    if (systemProcessors != (int) yarnProcessors) {
      LOG.info("YARN containers restricted to " + yarnProcessors + " cores");
      int[] limits = getOverallLimits(yarnProcessors);
      updateCgroup(CONTROLLER_CPU, "", CPU_PERIOD_US,
          String.valueOf(limits[0]));
      updateCgroup(CONTROLLER_CPU, "", CPU_QUOTA_US,
          String.valueOf(limits[1]));
    } else if (CGroupsCpuResourceHandlerImpl.checkCgroupV1CPULimitExists(
        pathForCgroup(CONTROLLER_CPU, ""))) {
      LOG.info("Removing CPU constraints for YARN containers.");
      updateCgroup(CONTROLLER_CPU, "", CPU_QUOTA_US, String.valueOf(-1));
    }
  }

  int[] getOverallLimits(float yarnProcessorsArg) {
    return CGroupsCpuResourceHandlerImpl.getOverallLimits(yarnProcessorsArg);
  }


  boolean isCpuWeightEnabled() {
    return this.cpuWeightEnabled;
  }

  /*
   * Next four functions are for an individual cgroup.
   */

  /**
   * 构造指定控制器和分组的cgroup完整路径。
   * @param controller cgroup控制器名称
   * @param groupName cgroup分组名称
   * @return 完整的cgroup文件系统路径
   */
  private String pathForCgroup(String controller, String groupName) {
    String controllerPath = controllerPaths.get(controller);
    return controllerPath + "/" + cgroupPrefix + "/" + groupName;
  }

  /**
   * 创建指定控制器下的cgroup分组。
   * @param controller cgroup控制器名称
   * @param groupName cgroup分组名称
   * @throws IOException 创建失败时抛出IO异常
   */
  private void createCgroup(String controller, String groupName)
        throws IOException {
    String path = pathForCgroup(controller, groupName);

    LOG.debug("createCgroup: {}", path);

    if (!new File(path).mkdir()) {
      throw new IOException("Failed to create cgroup at " + path);
    }
  }

  /**
   * 更新cgroup参数值。
   * @param controller cgroup控制器名称
   * @param groupName cgroup分组名称
   * @param param 参数名称
   * @param value 参数值
   * @throws IOException 更新失败时抛出IO异常
   */
  private void updateCgroup(String controller, String groupName, String param,
                            String value) throws IOException {
    String path = pathForCgroup(controller, groupName);
    param = controller + "." + param;

    LOG.debug("updateCgroup: {}: {}={}", path, param, value);

    PrintWriter pw = null;
    try {
      File file = new File(path + "/" + param);
      Writer w = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8);
      pw = new PrintWriter(w);
      pw.write(value);
    } catch (IOException e) {
      throw new IOException("Unable to set " + param + "=" + value +
          " for cgroup at: " + path, e);
    } finally {
      if (pw != null) {
        boolean hasError = pw.checkError();
        pw.close();
        if(hasError) {
          throw new IOException("Unable to set " + param + "=" + value +
                " for cgroup at: " + path);
        }
        if(pw.checkError()) {
          throw new IOException("Error while closing cgroup file " + path);
        }
      }
    }
  }

  /**
   * 打印cgroup tasks文件的第一行到调试日志，用于问题排查。
   * @param cgf cgroup目录对象
   */
  private void logLineFromTasksFile(File cgf) {
    String str;
    if (LOG.isDebugEnabled()) {
      try (BufferedReader inl =
            new BufferedReader(new InputStreamReader(new FileInputStream(cgf
              + "/tasks"), StandardCharsets.UTF_8))) {
        str = inl.readLine();
        if (str != null) {
          LOG.debug("First line in cgroup tasks file: {} {}", cgf, str);
        }
      } catch (IOException e) {
        LOG.warn("Failed to read cgroup tasks file. ", e);
      }
    }
  }

  /**
   * 如果tasks文件为空，则删除该cgroup。
   *
   * @param cgf 待删除的cgroup目录对象
   * @return 是否成功删除cgroup
   * @throws InterruptedException 等待删除时线程中断抛出异常
   */
  @VisibleForTesting
  boolean checkAndDeleteCgroup(File cgf) throws InterruptedException {
    boolean deleted = false;
    // FileInputStream in = null;
    try (FileInputStream in = new FileInputStream(cgf + "/tasks")) {
      if (in.read() == -1) {
        /*
         * "tasks" file is empty, sleep a bit more and then try to delete the
         * cgroup. Some versions of linux kernel have a race condition
         * that may cause panic, hence the delay and retry.
         */
        Thread.sleep(deleteCgroupDelay);
        deleted = cgf.delete();
        if (!deleted) {
          LOG.warn("Failed attempt to delete cgroup: " + cgf);
        }
      } else {
        logLineFromTasksFile(cgf);
      }
    } catch (IOException e) {
      LOG.warn("Failed to read cgroup tasks file. ", e);
    }
    return deleted;
  }

  /**
   * 重复尝试删除指定路径的cgroup，直到成功或超时。
   * @param cgroupPath 待删除的cgroup路径
   * @return 是否成功删除cgroup
   */
  @VisibleForTesting
  boolean deleteCgroup(String cgroupPath) {
    boolean deleted = false;

    LOG.debug("deleteCgroup: {}", cgroupPath);
    long start = clock.getTime();
    do {
      try {
        deleted = checkAndDeleteCgroup(new File(cgroupPath));
        if (!deleted) {
          Thread.sleep(deleteCgroupDelay);
        }
      } catch (InterruptedException ex) {
        // NOP
      }
    } while (!deleted && (clock.getTime() - start) < deleteCgroupTimeout);

    if (!deleted) {
      LOG.warn("Unable to delete cgroup at: " + cgroupPath +
          ", tried to delete for " + deleteCgroupTimeout + "ms");
    }
    return deleted;
  }

  /*
   * Next three functions operate on all the resources we are enforcing.
   */

  /**
   * 为容器设置cgroup资源限制。
   * @param containerId 容器ID
   * @param containerResource 容器资源配置
   * @throws IOException 设置限制失败时抛出IO异常
   */
  private void setupLimits(ContainerId containerId,
                           Resource containerResource) throws IOException {
    String containerName = containerId.toString();

    if (isCpuWeightEnabled()) {
      int containerVCores = containerResource.getVirtualCores();
      createCgroup(CONTROLLER_CPU, containerName);
      int cpuShares = CPU_DEFAULT_WEIGHT * containerVCores;
      updateCgroup(CONTROLLER_CPU, containerName, "shares",
          String.valueOf(cpuShares));
      if (strictResourceUsageMode) {
        if (nodeVCores != containerVCores) {
          float containerCPU =
              (containerVCores * yarnProcessors) / (float) nodeVCores;
          int[] limits = getOverallLimits(containerCPU);
          updateCgroup(CONTROLLER_CPU, containerName, CPU_PERIOD_US,
              String.valueOf(limits[0]));
          updateCgroup(CONTROLLER_CPU, containerName, CPU_QUOTA_US,
              String.valueOf(limits[1]));
        }
      }
    }
  }

  /**
   * 容器退出后清理容器对应的cgroup。
   * @param containerId 容器ID
   */
  private void clearLimits(ContainerId containerId) {
    if (isCpuWeightEnabled()) {
      deleteCgroup(pathForCgroup(CONTROLLER_CPU, containerId.toString()));
    }
  }

  /*
   * LCE Resources Handler interface
   */

  @Override
  public void preExecute(ContainerId containerId, Resource containerResource)
              throws IOException {
    setupLimits(containerId, containerResource);
  }

  @Override
  public void postExecute(ContainerId containerId) {
    clearLimits(containerId);
  }

  @Override
  public String getResourcesOption(ContainerId containerId) {
    String containerName = containerId.toString();

    StringBuilder sb = new StringBuilder("cgroups=");

    if (isCpuWeightEnabled()) {
      sb.append(pathForCgroup(CONTROLLER_CPU, containerName) + "/tasks");
      sb.append(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR);
    }

    if (sb.charAt(sb.length() - 1) ==
        PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR) {
      sb.deleteCharAt(sb.length() - 1);
    }

    return sb.toString();
  }

  /* We are looking for entries of the form:
   * none /cgroup/path/mem cgroup rw,memory 0 0
   *
   * Use a simple pattern that splits on the five spaces, and
   * grabs the 2, 3, and 4th fields.
   */

  private static final Pattern MTAB_FILE_FORMAT = Pattern.compile(
      "^[^\\s]+\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s[^\\s]+\\s[^\\s]+$");

  /*
   * Returns a map: path -> mount options
   * for mounts with type "cgroup". Cgroup controllers will
   * appear in the list of options for a path.
   */
  /**