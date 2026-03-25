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
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_ELASTIC_MEMORY_CONTROL_OOM_TIMEOUT_SEC;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_TIMEOUT_SEC;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_PMEM_CHECK_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_VMEM_CHECK_ENABLED;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_OOM_CONTROL;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_NO_LIMIT;

/**
 * 基于cgroups的弹性内存控制线程，负责监听整个节点YARN容器内存OOM事件，并选择容器杀死解决内存不足问题。
 * 容器选择算法可通过配置插件化实现。
 */
public class CGroupElasticMemoryController extends SubjectInheritingThread {
  protected static final Logger LOG = LoggerFactory
      .getLogger(CGroupElasticMemoryController.class);
  private final Clock clock = new MonotonicClock();
  private String yarnCGroupPath;
  private String oomListenerPath;
  private Runnable oomHandler;
  private CGroupsHandler cgroups;
  private boolean controlPhysicalMemory;
  private boolean controlVirtualMemory;
  private long limit;
  private Process process = null;
  private boolean stopped = false;
  private int timeoutMS;

  /**
   * 构造函数，用于测试可传入自定义OOM处理器。
   * @param conf Yarn配置
   * @param context NodeManager上下文
   * @param cgroups cgroups处理器
   * @param controlPhysicalMemory 是否监听物理内存OOM
   * @param controlVirtualMemory 是否监听虚拟内存OOM
   * @param limit 内存限制字节数
   * @param oomHandlerOverride 自定义OOM处理器
   * @exception YarnException 实例化失败时抛出
   */
  @VisibleForTesting
  CGroupElasticMemoryController(Configuration conf,
                                       Context context,
                                       CGroupsHandler cgroups,
                                       boolean controlPhysicalMemory,
                                       boolean controlVirtualMemory,
                                       long limit,
                                       Runnable oomHandlerOverride)
      throws YarnException {
    super("CGroupElasticMemoryController");
    boolean controlVirtual = controlVirtualMemory && !controlPhysicalMemory;
    Runnable oomHandlerTemp =
        getDefaultOOMHandler(conf, context, oomHandlerOverride, controlVirtual);
    if (controlPhysicalMemory && controlVirtualMemory) {
      LOG.warn(
          NM_ELASTIC_MEMORY_CONTROL_ENABLED + " is on. " +
          "We cannot control both virtual and physical " +
          "memory at the same time. Enforcing virtual memory. " +
          "If swapping is enabled set " +
          "only " + NM_PMEM_CHECK_ENABLED + " to true otherwise set " +
          "only " + NM_VMEM_CHECK_ENABLED + " to true.");
    }
    if (!controlPhysicalMemory && !controlVirtualMemory) {
      throw new YarnException(
          NM_ELASTIC_MEMORY_CONTROL_ENABLED + " is on. " +
              "We need either virtual or physical memory check requested. " +
              "If swapping is enabled set " +
              "only " + NM_PMEM_CHECK_ENABLED + " to true otherwise set " +
              "only " + NM_VMEM_CHECK_ENABLED + " to true.");
    }
    // 获取OOM处理超时时间，转换为毫秒
    this.timeoutMS =
        1000 * conf.getInt(NM_ELASTIC_MEMORY_CONTROL_OOM_TIMEOUT_SEC,
        DEFAULT_NM_ELASTIC_MEMORY_CONTROL_OOM_TIMEOUT_SEC);
    this.oomListenerPath = getOOMListenerExecutablePath(conf);
    this.oomHandler = oomHandlerTemp;
    this.cgroups = cgroups;
    this.controlPhysicalMemory = !controlVirtual;
    this.controlVirtualMemory = controlVirtual;
    this.yarnCGroupPath = this.cgroups
        .getPathForCGroup(CGroupsHandler.CGroupController.MEMORY, "");
    this.limit = limit;
  }

  /**
   * 根据配置获取OOM处理器实例。
   * @param conf 配置对象
   * @param context 上下文对象传递给构造函数
   * @param oomHandlerLocal 默认覆盖处理器
   * @param controlVirtual 是否控制虚拟内存
   * @return 配置好的OOM处理器实例
   * @throws YarnException 构造实例失败时抛出
   */
  private Runnable getDefaultOOMHandler(
      Configuration conf, Context context, Runnable oomHandlerLocal,
      boolean controlVirtual)
      throws YarnException {
    Class oomHandlerClass =
        conf.getClass(
            YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_HANDLER,
            DefaultOOMHandler.class);
    if (oomHandlerLocal == null) {
      try {
        Constructor constr = oomHandlerClass.getConstructor(
            Context.class, boolean.class);
        oomHandlerLocal = (Runnable)constr.newInstance(
            context, controlVirtual);
      } catch (Exception ex) {
        throw new YarnException(ex);
      }
    }
    return oomHandlerLocal;
  }

  /**
   * 公开构造函数。
   * @param conf Yarn配置
   * @param context NodeManager上下文
   * @param cgroups cgroups处理器
   * @param controlPhysicalMemory 是否监听物理内存OOM
   * @param controlVirtualMemory 是否监听虚拟内存OOM
   * @param limit 内存限制字节数
   * @exception YarnException 实例化失败时抛出
   */
  public CGroupElasticMemoryController(Configuration conf,
                                       Context context,
                                       CGroupsHandler cgroups,
                                       boolean controlPhysicalMemory,
                                       boolean controlVirtualMemory,
                                       long limit)
      throws YarnException {
    this(conf,
        context,
        cgroups,
        controlPhysicalMemory,
        controlVirtualMemory,
        limit,
        null);
  }

  /**
   * OOM无法解决时抛出的异常。
   */
  static private class OOMNotResolvedException extends YarnRuntimeException {
    OOMNotResolvedException(String message, Exception parent) {
      super(message, parent);
    }
  }

  /**
   * 停止监听cgroup OOM事件，销毁监听进程。
   */
  public synchronized void stopListening() {
    stopped = true;
    if (process != null) {
      process.destroyForcibly();
    } else {
      LOG.warn("Trying to stop listening, when listening is not running");
    }
  }

  /**
   * 检查当前系统是否支持弹性内存控制功能。
   * @return 支持返回true，否则返回false
   */
  public static boolean isAvailable() {
    try {
      if (!Shell.LINUX) {
        LOG.info("CGroupElasticMemoryController currently is supported only "
            + "on Linux.");
        return false;
      }
      if (ResourceHandlerModule.getCGroupsHandler() == null ||
          ResourceHandlerModule.getMemoryResourceHandler() == null) {
        LOG.info("CGroupElasticMemoryController requires enabling " +
            "memory CGroups with" +
            YarnConfiguration.NM_MEMORY_RESOURCE_ENABLED);
        return false;
      }
    } catch (SecurityException se) {
      LOG.info("Failed to get Operating System name. " + se);
      return false;
    }
    return true;
  }

  /**
   * 主线程工作函数，启动外部OOM监听进程，循环处理OOM事件。
   */
  @Override
  public void work() {
    ExecutorService executor = null;
    try {
      // 配置cgroup参数，设置内存限制并启用OOM通知
      setCGroupParameters();

      // 创建OOM监听进程构建器
      ProcessBuilder oomListener = new ProcessBuilder();
      oomListener.command(oomListenerPath, yarnCGroupPath);
      synchronized (this) {
        if (!stopped) {
          process = oomListener.start();
        } else {
          resetCGroupParameters();
          LOG.info("Listener stopped before starting");
          return;
        }
      }
      LOG.info(String.format("Listening on %s with %s",
          yarnCGroupPath,
          oomListenerPath));

      // 创建固定线程池，用于错误流读取和OOM处理看门狗
      executor = Executors.newFixedThreadPool(2);

      // 异步读取监听进程错误输出
      Future<String> errorListener =
          executor.submit(() -> IOUtils.toString(process.getErrorStream(), StandardCharsets.UTF_8));

      // 从监听进程读取OOM事件，每个事件占8字节
      InputStream events = process.getInputStream();
      byte[] event = new byte[8];
      int read;
      while ((read = events.read(event)) == event.length) {
        // 处理OOM事件
        resolveOOM(executor);
      }

      if (read != -1) {
        LOG.warn(String.format("Characters returned from event hander: %d",
            read));
      }

      // 等待监听进程退出，获取退出信息
      int exitCode = process.waitFor();
      String error = errorListener.get();
      process = null;
      LOG.info(String.format("OOM listener exited %d %s", exitCode, error));
    } catch (OOMNotResolvedException ex) {
      throw new YarnRuntimeException("Could not resolve OOM", ex);
    } catch (Exception ex) {
      synchronized (this) {
        if (!stopped) {
          LOG.warn("OOM Listener exiting.", ex);
        }
      }
    } finally {
      // 确保子进程被销毁，避免资源泄漏
      if (process != null && process.isAlive()) {
        process.destroyForcibly();
      }
      // 关闭线程池
      if (executor != null) {
        try {
          executor.awaitTermination(6, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          LOG.warn("Exiting without processing all OOM events.");
        }
        executor.shutdown();
      }
      // 还原cgroup配置
      resetCGroupParameters();
    }
  }

  /**
   * 处理OOM事件，启动看门狗监控处理超时。
   * @param executor 执行看门狗的线程池
   * @throws InterruptedException 线程中断
   * @throws java.util.concurrent.ExecutionException 看门狗执行异常
   */
  private void resolveOOM(ExecutorService executor)
      throws InterruptedException, java.util.concurrent.ExecutionException {
    // 记录OOM开始时间
    final long start = clock.getTime();
    // 提交看门狗任务
    Future<Boolean> watchdog =
        executor.submit(() -> watchAndLogOOMState(start));
    // 执行OOM处理逻辑
    try {
      oomHandler.run();
    } catch (RuntimeException ex) {
      watchdog.cancel(true);
      throw new OOMNotResolvedException("OOM handler failed", ex);
    }
    // 等待处理结果，超时返回false
    if (!watchdog.get()) {
      throw new OOMNotResolvedException("OOM handler timed out", null);
    }
  }

  /**
   * 持续监控OOM状态，每秒日志更新，超时未解决返回false。
   * @param start OOM开始时间
   * @return OOM成功解决返回true，超时返回false
   */
  private boolean watchAndLogOOMState(long start) {
    long lastLog = start;
    try {
      long end = start;
      // 在超时时间内持续检查OOM状态
      while(end - start < timeoutMS) {
        end = clock.getTime();
        // 读取当前OOM状态
        String underOOM = cgroups.getCGroupParam(
            CGroupsHandler.CGroupController.MEMORY,
            "",
            CGROUP_PARAM_MEMORY_OOM_CONTROL);
        if (underOOM.contains(CGroupsHandler.UNDER_OOM)) {
          // 每秒打印一次日志，避免刷屏
          if (end - lastLog > 1000) {
            LOG.warn(String.format(
                "OOM not resolved in %d ms", end - start));
            lastLog = end;
          }
        } else {
          // OOM已解决
          LOG.info(String.format(
              "Resolved OOM in %d ms", end - start));
          return true;
        }
        // 短暂休眠避免占满CPU
        Thread.sleep(10);
      }
    } catch (InterruptedException ex) {
      LOG.debug("Watchdog interrupted");
    } catch (Exception e) {
      LOG.warn("Exception running logging thread", e);
    }
    // 超时未解决OOM，停止监听
    LOG.warn(String.format("OOM was not resolved in %d ms",
        clock.getTime() - start));
    stopListening();
    return false;
  }

  /**
   * 设置根cgroup内存参数，配置内存限制并启用OOM通知。
   */
  private void setCGroupParameters() throws ResourceHandlerException {
    // 禁用内核自带OOM killer，由本组件处理OOM
    cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL, "1");
    if (controlPhysicalMemory && !controlVirtualMemory) {
      try {
        // 取消交换空间限制
        cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
            CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES, CGROUP_NO_LIMIT);
      } catch (ResourceHandlerException ex) {
        LOG.debug("Swap monitoring is turned off in the kernel");
      }
      // 设置物理内存硬限制
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES, Long.toString(limit));
    } else if (controlVirtualMemory && !controlPhysicalMemory) {
      // 取消交换空间限制
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES, CGROUP_NO_LIMIT);
      // 设置物理内存限制
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES, Long.toString(limit));
      // 设置虚拟内存(物理+交换)硬限制，必须在物理限制之后设置
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES, Long.toString(limit));
    } else {
      throw new ResourceHandlerException(
          String.format("Unsupported scenario physical:%b virtual:%b",
              controlPhysicalMemory, controlVirtualMemory));
    }
  }

  /**
   * 重置根cgroup配置，恢复默认限制，开启内核OOM killer。
   */
  private void resetCGroupParameters() {
    try {
      try {
        // 取消交换空间限制
        cgroups.updateCGroupParam(
            CGroupsHandler.CGroupController.MEMORY, "",
            CG