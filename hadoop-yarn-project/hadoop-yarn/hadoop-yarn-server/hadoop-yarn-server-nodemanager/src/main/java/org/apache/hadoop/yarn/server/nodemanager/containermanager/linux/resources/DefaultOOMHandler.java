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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
 container;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PROCS_FILE;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_OOM_CONTROL;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_USAGE_BYTES;

/**
 * Default OOM处理实现，用于YARN NodeManager节点内存不足时清理容器释放内存。
 * 当节点级OOM发生时，会按优先级杀死容器，直到OOM状态解除。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DefaultOOMHandler implements Runnable {
  protected static final Logger LOG = LoggerFactory
      .getLogger(DefaultOOMHandler.class);
  // NodeManager全局上下文
  private final Context context;
  // 要读取的内存统计文件路径，根据是否检查虚拟内存选择
  private final String memoryStatFile;
  // CGroups处理器，用于读取cgroups信息
  private final CGroupsHandler cgroups;

  /**
   * 构造OOM处理器，需要通过反射构造所以必须是public。
   * @param context NodeManager上下文
   * @param enforceVirtualMemory 是否检查虚拟内存，false则检查物理内存
   */
  public DefaultOOMHandler(Context context, boolean enforceVirtualMemory) {
    this.context = context;
    this.memoryStatFile = enforceVirtualMemory ?
        CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES :
        CGROUP_PARAM_MEMORY_USAGE_BYTES;
    this.cgroups = getCGroupsHandler();
  }

  @VisibleForTesting
  protected CGroupsHandler getCGroupsHandler() {
    return ResourceHandlerModule.getCGroupsHandler();
  }

  /**
   * 检查容器是否超出内存限制。
   * @param container 待检查容器
   * @return true表示超出限制，false表示正常
   */
  private boolean isContainerOutOfLimit(Container container) {
    boolean outOfLimit = false;

    String value = null;
    try {
      // 从cgroups读取当前内存使用量
      value = cgroups.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
          container.getContainerId().toString(), memoryStatFile);
      long usage = Long.parseLong(value);
      // 转换容器申请的内存为字节单位
      long request = container.getResource().getMemorySize() * 1024 * 1024;

      // 检查是否超出申请的内存限制
      if (usage > request) {
        outOfLimit = true;
        String message = String.format(
            "Container %s is out of its limits, using %d " +
                "when requested only %d",
            container.getContainerId(), usage, request);
        LOG.warn(message);
      }
    } catch (ResourceHandlerException ex) {
      LOG.warn(String.format("Could not access memory resource for %s",
          container.getContainerId()), ex);
    } catch (NumberFormatException ex) {
      LOG.warn(String.format("Could not parse %s in %s", value,
          container.getContainerId()));
    }
    return outOfLimit;
  }

  /**
   * 强制杀死容器内所有进程，处理OOM场景下的进程冻结问题。
   * 由于OOM时进程被cgroups冻结，无法响应SIGTERM，因此直接发送SIGKILL。
   * 循环读取cgroup中的进程列表逐个杀死，直到所有进程清理完成，避免进程泄漏。
   *
   * @param container 待杀死容器
   * @return 杀死成功返回true，否则false
   */
  private boolean sigKill(Container container) {
    boolean containerKilled = false;
    boolean finished = false;
    try {
      // 循环直到没有剩余进程需要杀死
      while (!finished) {
        // 读取当前容器cgroup中的所有进程ID
        String[] pids =
            cgroups.getCGroupParam(
                CGroupsHandler.CGroupController.MEMORY,
                container.getContainerId().toString(),
                CGROUP_PROCS_FILE)
                .split("\n");
        finished = true;
        // 遍历每个进程ID发送SIGKILL
        for (String pid : pids) {
          // 注意：当前仅支持杀死PGID
          if (pid != null && !pid.isEmpty()) {
            LOG.debug(String.format(
                "Terminating container %s Sending SIGKILL to -%s",
                container.getContainerId().toString(),
                pid));
            // 还有进程需要处理，标记未完成
            finished = false;
            try {
              // 通过容器执行器发送KILL信号
              context.getContainerExecutor().signalContainer(
                  new ContainerSignalContext.Builder().setContainer(container)
                      .setUser(container.getUser())
                      .setPid(pid).setSignal(ContainerExecutor.Signal.KILL)
                      .build());
            } catch (IOException ex) {
              LOG.warn(String.format("Cannot kill container %s pid -%s.",
                  container.getContainerId(), pid), ex);
            }
          }
        }
        try {
          // 等待10ms让系统回收进程，下次循环确认是否还有剩余进程
          Thread.sleep(10);
        } catch (InterruptedException e) {
          LOG.debug("Interrupted while waiting for processes to disappear");
        }
      }
      containerKilled = true;
    } catch (ResourceHandlerException ex) {
      // 容器可能还没启动完成，无法读取进程列表
      LOG.warn(String.format(
          "Cannot list more tasks in container %s to kill.",
          container.getContainerId()));
    }

    return containerKilled;
  }

  /**
   * OOM处理主入口，当节点触发OOM时被调用。
   * 循环检测OOM状态，不断杀死容器直到内核报告OOM解除。
   * 优先杀死机会容器，相同类型优先杀死最新启动且超内存的容器。
   */
  @Override
  public void run() {
    try {
      // 持续杀死容器直到内核报告OOM状态解除
      while (true) {
        // 读取根cgroup的OOM状态
        String status = cgroups.getCGroupParam(
            CGroupsHandler.CGroupController.MEMORY,
            "",
            CGROUP_PARAM_MEMORY_OOM_CONTROL);
        // 如果已经不在OOM状态，退出处理
        if (!status.contains(CGroupsHandler.UNDER_OOM)) {
          break;
        }

        // 尝试杀死一个容器释放内存
        boolean containerKilled = killContainer();

        if (!containerKilled) {
          // 找不到可杀死的容器，无法解决OOM，抛出异常终止处理
          throw new YarnRuntimeException(
              "Could not find any containers but CGroups " +
                  "reserved for containers ran out of memory. " +
                  "I am giving up");
        }
      }
    } catch (ResourceHandlerException ex) {
      // 节点关闭时无法读取OOM状态属于正常情况，直接退出
      LOG.warn("Could not fetch OOM status. " +
          "This is expected at shutdown. Exiting.", ex);
    }
  }

  /**
   * 按优先级选择并杀死一个容器释放内存。
   * 选择优先级：超内存的机会容器 > 普通机会容器 > 超内存的保障容器 > 普通保障容器
   * 同优先级选择最新启动的容器，因为其未提交数据最少，杀死代价最小。
   * @return 成功杀死容器返回true，否则false
   */
  protected boolean killContainer() {
    boolean containerKilled = false;

    ArrayList<ContainerCandidate> candidates = new ArrayList<>(0);
    // 遍历NodeManager上所有容器收集候选
    for (Container container : context.getContainers().values()) {
      if (!container.isRunning()) {
        // 跳过未运行容器，杀死它们不会释放内存
        continue;
        // 注意：即使container.isRunning返回true，实际进程可能还未完全启动
        // 对于NM来说，只要容器启动交给执行器就会标记为运行中
      }
      // 将容器包装为候选，记录是否超内存
      candidates.add(
          new ContainerCandidate(container, isContainerOutOfLimit(container)));
    }
    // 按优先级排序
    Collections.sort(candidates);
    if (candidates.isEmpty()) {
      LOG.warn(
          "Found no running containers to kill in order to release memory");
    }

    // 按优先级顺序尝试杀死容器，直到成功杀死一个
    for(int i = 0; !containerKilled && i < candidates.size(); i++) {
      ContainerCandidate candidate = candidates.get(i);
      if (sigKill(candidate.container)) {
        String message = String.format(
            "container %s killed by elastic cgroups OOM handler.",
            candidate.container.getContainerId());
        LOG.warn(message);
        containerKilled = true;
      }
    }
    return containerKilled;
  }

  /**
   * 容器候选包装类，用于OOM选择时排序。
   * 排序规则：机会容器优先于保障容器，超内存容器优先于未超内存，最新启动优先于早启动。
   * 注意：该类的自然排序与equals不一致，符合排序场景要求。
   */
  private static class ContainerCandidate
      implements Comparable<ContainerCandidate> {
    private final boolean outOfLimit;
    final Container container;

    ContainerCandidate(Container container, boolean outOfLimit) {
      this.outOfLimit = outOfLimit;
      this.container = container;
    }

    /**
     * 比较两个容器候选，确定杀死优先级。
     * 优先级顺序：
     * 1. 机会容器比保障容器优先杀死
     * 2. 同执行类型下，超内存比未超内存优先杀死
     * 3. 同状态下，启动时间越晚越优先杀死
     */
    @Override
    public int compareTo(ContainerCandidate o) {
      boolean isThisOpportunistic = isOpportunistic(container);
      boolean isOtherOpportunistic = isOpportunistic(o.container);
      // 机会容器排在前面，优先被杀死
      int ret = Boolean.compare(isOtherOpportunistic, isThisOpportunistic);
      if (ret == 0) {
        // 执行类型相同，按是否超内存排序，超内存的优先
        int outOfLimitRet = Boolean.compare(o.outOfLimit, outOfLimit);
        if (outOfLimitRet == 0) {
          // 状态相同，按启动时间排序，后启动的排在前面，优先杀死
          ret = Long.compare(o.container.getContainerLaunchTime(),
              this.container.getContainerLaunchTime());
        } else {
          ret = outOfLimitRet;
        }
      }
      return ret;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (this.getClass() != obj.getClass()) {
        return false;
      }
      ContainerCandidate other = (ContainerCandidate) obj;
      if (this.outOfLimit != other.outOfLimit) {
        return false;
      }
      if (this.container == null) {
        return other.container == null;
      } else {
        return this.container.equals(other.container);
      }
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(container).append(outOfLimit)
          .toHashCode();
    }

    /**
     * 检查容器是否为机会型容器。
     * @param container 待检查容器
     * @return 是机会型容器返回true，否则false
     */
    private static boolean isOpportunistic(Container container) {
      return container.getContainerTokenIdentifier() != null &&
          ExecutionType.OPPORTUNISTIC.equals(
              container.getContainerTokenIdentifier().getExecutionType());
    }
  }
}