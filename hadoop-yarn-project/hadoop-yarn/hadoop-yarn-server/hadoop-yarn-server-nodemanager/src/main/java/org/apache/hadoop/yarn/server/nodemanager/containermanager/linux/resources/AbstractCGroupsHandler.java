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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 抽象cgroups处理器基类，封装Linux cgroups操作的通用逻辑，为不同版本cgroups提供统一抽象接口
 * 负责管理cgroups挂载路径、创建/删除cgroup、读写cgroup参数等通用操作
 */
public abstract class AbstractCGroupsHandler implements CGroupsHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractCGroupsHandler.class);
  protected static final String MTAB_FILE = "/proc/mounts";

  // 删除cgroup操作的超时时间
  private final long deleteCGroupTimeout;
  // 删除cgroup操作前的延迟等待时间
  private final long deleteCGroupDelay;
  // 时钟实例，用于计时
  private final Clock clock;

  // 挂载信息文件路径（默认/proc/mounts）
  protected final String mtabFile;
  // cgroups挂载配置
  protected final CGroupsMountConfig cGroupsMountConfig;
  // 读写锁，保护controllerPaths并发访问
  protected final ReadWriteLock rwLock;
  // 存储各cgroup控制器对应的挂载路径
  protected Map<CGroupController, String> controllerPaths;
  // 存储解析后的挂载信息：路径 -> 该挂载点包含的控制器集合
  protected Map<String, Set<String>> parsedMtab;
  // 特权操作执行器，用于执行需要root权限的操作
  protected final PrivilegedOperationExecutor privilegedOperationExecutor;
  // YARN使用的cgroup层级前缀路径
  protected final String cGroupPrefix;

  /**
   * 构造抽象cgroups处理器，加载配置并初始化基础信息
   *
   * @param conf                        YARN配置
   * @param privilegedOperationExecutor 特权操作执行器，用于执行需要权限的操作
   * @param mtab                        挂载文件路径
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  AbstractCGroupsHandler(Configuration conf, PrivilegedOperationExecutor
      privilegedOperationExecutor, String mtab)
      throws ResourceHandlerException {
    // 移除路径首尾的斜杠，统一格式
    this.cGroupPrefix = conf.get(YarnConfiguration.
            NM_LINUX_CONTAINER_CGROUPS_HIERARCHY, "/hadoop-yarn")
        .replaceAll("^/+", "").replaceAll("/+$", "");
    this.cGroupsMountConfig = new CGroupsMountConfig(conf);
    // 计算总超时时间，加上配置的sigkill延迟和额外1秒缓冲
    this.deleteCGroupTimeout = conf.getLong(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT,
        YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT) +
        conf.getLong(YarnConfiguration.NM_SLEEP_DELAY_BEFORE_SIGKILL_MS,
            YarnConfiguration.DEFAULT_NM_SLEEP_DELAY_BEFORE_SIGKILL_MS) + 1000;
    this.deleteCGroupDelay =
        conf.getLong(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_DELETE_DELAY,
            YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_DELAY);
    this.controllerPaths = new HashMap<>();
    this.parsedMtab = new HashMap<>();
    this.rwLock = new ReentrantReadWriteLock();
    this.privilegedOperationExecutor = privilegedOperationExecutor;
    this.clock = SystemClock.getInstance();
    mtabFile = mtab;
    init();
  }

  protected void init() throws ResourceHandlerException {
    initializeControllerPaths();
  }

  @Override
  public String getControllerPath(CGroupController controller) {
    rwLock.readLock().lock();
    try {
      return controllerPaths.get(controller);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * 初始化各cgroup控制器的挂载路径，从挂载文件或预配置路径解析
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  private void initializeControllerPaths() throws ResourceHandlerException {
    // Cluster admins may have some subsystems mounted in specific locations
    // We'll attempt to figure out mount points. We do this even if we plan
    // to mount cgroups into our own tree to control the path permissions or
    // to mount subsystems that are not mounted previously.
    // The subsystems for new and existing mount points have to match, and
    // the same hierarchy will be mounted at each mount point with the same
    // subsystem set.

    Map<String, Set<String>> newMtab = null;
    Map<CGroupController, String> cPaths;
    try {
      // 如果禁用自动挂载且已配置挂载路径，使用预配置路径解析
      if (this.cGroupsMountConfig.mountDisabledButMountPathDefined()) {
        newMtab = parsePreConfiguredMountPath();
      }

      // 否则从系统mtab文件解析挂载信息
      if (newMtab == null) {
        // parse mtab
        newMtab = parseMtab(mtabFile);
      }

      // 从挂载信息中提取各控制器的路径
      cPaths = initializeControllerPathsFromMtab(newMtab);
    } catch (IOException e) {
      LOG.warn("Failed to initialize controller paths! Exception: ", e);
      throw new ResourceHandlerException(
          "Failed to initialize controller paths!");
    }

    // 加写锁批量更新路径信息，避免并发访问时路径不一致
    rwLock.writeLock().lock();
    try {
      controllerPaths = cPaths;
      parsedMtab = newMtab;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  protected abstract Map<String, Set<String>> parsePreConfiguredMountPath() throws IOException;

  /**
   * 从解析后的挂载信息中提取各cgroup控制器的挂载路径
   * @param mtab 解析后的挂载信息
   * @return 控制器 -> 挂载路径 映射
   */
  protected Map<CGroupController, String> initializeControllerPathsFromMtab(
      Map<String, Set<String>> mtab) {
    Map<CGroupController, String> ret = new HashMap<>();

    // 遍历所有需要的控制器，查找对应挂载路径
    for (CGroupController controller : getCGroupControllers()) {
      String subsystemName = controller.getName();
      String controllerPath = findControllerInMtab(subsystemName, mtab);

      if (controllerPath != null) {
        ret.put(controller, controllerPath);
      }
    }
    return ret;
  }

  protected abstract List<CGroupController> getCGroupControllers();

  /* We are looking for entries of the form:
   * none /cgroup/path/mem cgroup rw,memory 0 0
   *
   * Use a simple pattern that splits on the five spaces, and
   * grabs the 2, 3, and 4th fields.
   */

  // 匹配mtab文件行格式的正则表达式，提取路径、类型、选项
  private static final Pattern MTAB_FILE_FORMAT = Pattern.compile(
      "^[^\\s]+\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s[^\\s]+\\s[^\\s]+$");

  /*
   * Returns a map: path -> mount options
   * for mounts with type "cgroup". Cgroup controllers will
   * appear in the list of options for a path.
   */
  /**
   * 解析mtab挂载文件，提取cgroup相关挂载信息
   * @param mtab mtab文件路径
   * @return 挂载路径 -> 该挂载点包含的控制器集合 映射
   * @throws IOException 读取文件失败抛出异常
   */
  protected Map<String, Set<String>> parseMtab(String mtab)
      throws IOException {
    Map<String, Set<String>> ret = new HashMap<>();
    BufferedReader in = null;

    try {
      FileInputStream fis = new FileInputStream(mtab);
      in = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

      // 逐行读取解析
      for (String str = in.readLine(); str != null;
           str = in.readLine()) {
        Matcher m = MTAB_FILE_FORMAT.matcher(str);
        boolean mat = m.find();
        if (mat) {
          String path = m.group(1);
          String type = m.group(2);
          String options = m.group(3);

          // 处理当前行，提取控制器集合
          Set<String> controllerSet = handleMtabEntry(path, type, options);
          if (controllerSet != null) {
            ret.put(path, controllerSet);
          }
        }
      }
    } catch (IOException e) {
      if (Shell.LINUX) {
        // Linux系统必须读取成功，抛出异常
        throw new IOException("Error while reading " + mtab, e);
      } else {
        // 非Linux系统忽略错误，仅打印警告（测试场景）
        LOG.warn("Error while reading " + mtab, e);
      }
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }

    return ret;
  }

  protected abstract Set<String> handleMtabEntry(String path, String type, String options)
      throws IOException;

  /**
   * 在解析后的mtab中查找指定控制器的挂载路径
   * 内核保证一个控制器只能属于一个层级，返回第一个找到的可访问路径
   *
   * @param controller 控制器名称（如cpu、cpuset等）
   * @param entries    解析后的挂载信息
   * @return 找到的控制器挂载路径，找不到返回null
   */
  protected String findControllerInMtab(String controller,
                                        Map<String, Set<String>> entries) {
    for (Map.Entry<String, Set<String>> e : entries.entrySet()) {
      if (e.getValue().contains(controller)) {
        // 检查路径是否可读
        if (new File(e.getKey()).canRead()) {
          return e.getKey();
        } else {
          LOG.warn(String.format(
              "Skipping inaccessible cgroup mount point %s", e.getKey()));
        }
      }
    }

    return null;
  }

  protected abstract void mountCGroupController(CGroupController controller)
      throws ResourceHandlerException;

  @Override
  public String getRelativePathForCGroup(String cGroupId) {
    return cGroupPrefix + Path.SEPARATOR + cGroupId;
  }

  @Override
  public String getPathForCGroup(CGroupController controller, String cGroupId) {
    return getControllerPath(controller) + Path.SEPARATOR + cGroupPrefix
        + Path.SEPARATOR + cGroupId;
  }

  @Override
  public String getPathForCGroupTasks(CGroupController controller,
                                      String cGroupId) {
    return getPathForCGroup(controller, cGroupId)
        + Path.SEPARATOR + CGROUP_PROCS_FILE;
  }

  @Override
  public String getPathForCGroupParam(CGroupController controller,
                                      String cGroupId, String param) {
    return getPathForCGroup(controller, cGroupId)
        + Path.SEPARATOR + controller.getName()
        + "." + param;
  }

  /**
   * 初始化cgroup控制器，根据配置选择自动挂载或使用已挂载的层级
   *
   * @param controller 需要初始化的控制器
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  @Override
  public void initializeCGroupController(CGroupController controller) throws
      ResourceHandlerException {
    if (this.cGroupsMountConfig.isMountEnabled() &&
        cGroupsMountConfig.ensureMountPathIsDefined()) {
      // 启用自动挂载，执行挂载操作
      mountCGroupController(controller);
    }

    // 检查并初始化YARN在已挂载层级中的目录
    initializePreMountedCGroupController(controller);
  }

  /**
   * 初始化预挂载cgroup控制器，检查YARN层级存在性和权限，不存在则创建
   * 处理两种场景：1. YARN层级已存在，检查权限；2. YARN层级不存在，创建
   *
   * @param controller 需要初始化的控制器
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  private void initializePreMountedCGroupController(CGroupController controller)
      throws ResourceHandlerException {
    // Check permissions to cgroup hierarchy and
    // create YARN cgroup if it does not exist, yet
    String controllerPath = getControllerPath(controller);

    if (controllerPath == null) {
      throw new ResourceHandlerException(
          String.format("Controller %s not mounted."
                  + " You either need to mount it with %s"
                  + " or mount cgroups before launching Yarn",
              controller.getName(), YarnConfiguration.
                  NM_LINUX_CONTAINER_CGROUPS_MOUNT));
    }

    File rootHierarchy = new File(controllerPath);
    File yarnHierarchy = new File(rootHierarchy, cGroupPrefix);
    String subsystemName = controller.getName();

    LOG.info("Initializing mounted controller " + controller.getName() + " " +
        "at " + yarnHierarchy);

    // 检查根挂载点是否存在
    if (!rootHierarchy.exists()) {
      throw new ResourceHandlerException(getErrorWithDetails(
          "Cgroups mount point does not exist or not accessible",
          subsystemName,
          rootHierarchy.getAbsolutePath()
      ));
    } else if (!yarnHierarchy.exists()) {
      // YARN层级不存在，创建
      LOG.info("Yarn control group does not exist. Creating " +
          yarnHierarchy.getAbsolutePath());
      try {
        if (yarnHierarchy.mkdir()) {
          // 创建成功后更新层级中启用的控制器（cgroup v2需要）
          updateEnabledControllersInHierarchy(rootHierarchy, controller);
        } else {
          // 创建失败抛出异常
          throw new ResourceHandlerException(getErrorWithDetails(
              "Unexpected: Cannot create yarn cgroup hierarchy",
              subsystemName,
              yarnHierarchy.getAbsolutePath()
          ));
        }
      } catch (SecurityException e) {
        throw new ResourceHandlerException(getErrorWithDetails(
            "No permissions to create yarn cgroup hierarchy",
            subsystemName,
            yarnHierarchy.getAbsolutePath()
        ), e);
      }
    } else if (!FileUtil.canWrite(yarnHierarchy)) {
      // 检查YARN层级是否可写
      throw new ResourceHandlerException(getErrorWithDetails(
          "Yarn control group not writable",
          subsystemName,
          yarnHierarchy.getAbsolutePath()
      ));
    }

    // 更新层级中启用的控制器
    updateEnabledControllersInHierarchy(yarnHierarchy, controller);
  }

  protected abstract void updateEnabledControllersInHierarchy(
      File yarnHierarchy, CGroupController controller)
      throws ResourceHandlerException;

  /**
   * 生成带详细上下文信息的错误信息，方便问题排查
   *
   * @param errorMessage   错误描述
   * @param subsystemName  cgroup子系统名称
   * @param yarnCgroupPath 出错的cgroup路径
   * @return 完整错误信息字符串
   */
  protected String getErrorWithDetails(
      String errorMessage,
      String subsystemName,
      String yarnCgroupPath) {
    return String.format("%s Subsystem:%s Mount points:%s User:%s Path:%s ",
        errorMessage, subsystemName, mtabFile, System.getProperty("user.name"),
        yarnCgroupPath);