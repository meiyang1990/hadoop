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
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 文件级注释：NameNode启动进度跟踪容器，为NameNode启动过程中各个阶段和步骤提供进度跟踪能力
 * 
 * StartupProgress is used in various parts of the namenode codebase to indicate
 * startup progress.  Its methods provide ways to indicate begin and end of a
 * {@link Phase} or {@link Step} within a phase.  Additional methods provide ways
 * to associate a step or phase with optional information, such as a file name or
 * file size.  It also provides counters, which can be incremented by the  caller
 * to indicate progress through a long-running task.
 * 
 * This class is thread-safe.  Any number of threads may call any methods, even
 * for the same phase or step, without risk of corrupting internal state.  For
 * all begin/end methods and set methods, the last one in wins, overwriting any
 * prior writes.  Instances of {@link Counter} provide an atomic increment
 * operation to prevent lost updates.
 * 
 * After startup completes, the tracked data is frozen.  Any subsequent updates
 * or counter increments are no-ops.
 * 
 * For read access, call {@link #createView()} to create a consistent view with
 * a clone of the data.
 */
@InterfaceAudience.Private
/**
 * 类级注释：NameNode启动进度管理器，负责跟踪记录NameNode启动全过程中各个阶段和子步骤的进度、耗时、计数信息
 * 核心职责：为Web UI和监控系统提供NameNode启动进度查询能力，支持多线程并发更新，启动完成后自动冻结数据
 */
public class StartupProgress {

  private static final Logger LOG = LoggerFactory.getLogger(StartupProgress.class);

  // 存储所有启动阶段的跟踪信息，包访问权限供StartupProgressView读取
  final Map<Phase, PhaseTracking> phases =
    new ConcurrentHashMap<Phase, PhaseTracking>();

  /**
   * 接口级注释：进度计数器接口，提供原子增量操作，允许调用方跟踪长时间任务的完成进度
   * Allows a caller to increment a counter for tracking progress.
   */
  public static interface Counter {
    /**
     * 方法级注释：原子自增计数器，当前值加1
     * Atomically increments this counter, adding 1 to the current value.
     */
    void increment();
  }

  /**
   * 构造方法级注释：初始化所有预定义启动阶段的跟踪数据结构，创建StartupProgress实例
   * Creates a new StartupProgress by initializing internal data structure for
   * tracking progress of all defined phases.
   */
  public StartupProgress() {
    for (Phase phase: EnumSet.allOf(Phase.class)) {
      phases.put(phase, new PhaseTracking());
    }
  }

  /**
   * 方法级注释：标记指定启动阶段开始执行，记录开始时间
   * 
   * @param phase 要开始的启动阶段
   */
  public void beginPhase(Phase phase) {
    if (!isComplete()) {
      phases.get(phase).beginTime = monotonicNow();
    }
    LOG.debug("Beginning of the phase: {}", phase);
  }

  /**
   * 方法级注释：标记指定阶段内的指定步骤开始执行，记录开始时间，如果阶段已完成则不操作
   * 
   * @param phase 步骤所属的启动阶段
   * @param step 要开始的步骤
   */
  public void beginStep(Phase phase, Step step) {
    if (!isComplete(phase)) {
      lazyInitStep(phase, step).beginTime = monotonicNow();
    }
    LOG.debug("Beginning of the step. Phase: {}, Step: {}", phase, step);
  }

  /**
   * 方法级注释：标记指定启动阶段执行完成，记录结束时间
   * 
   * @param phase 要结束的启动阶段
   */
  public void endPhase(Phase phase) {
    if (!isComplete()) {
      phases.get(phase).endTime = monotonicNow();
    }
    LOG.debug("End of the phase: {}", phase);
  }

  /**
   * 方法级注释：标记指定阶段内的指定步骤执行完成，记录结束时间，如果阶段已完成则不操作
   *
   * @param phase 步骤所属的启动阶段
   * @param step 要结束的步骤
   */
  public void endStep(Phase phase, Step step) {
    if (!isComplete(phase)) {
      lazyInitStep(phase, step).endTime = monotonicNow();
    }
    LOG.debug("End of the step. Phase: {}, Step: {}", phase, step);
  }

  /**
   * 方法级注释：获取指定启动阶段当前的状态
   * 
   * @param phase 要查询的启动阶段
   * @return 阶段的当前状态（等待/运行中/已完成）
   */
  public Status getStatus(Phase phase) {
    PhaseTracking tracking = phases.get(phase);
    if (tracking.beginTime == Long.MIN_VALUE) {
      return Status.PENDING;
    } else if (tracking.endTime == Long.MIN_VALUE) {
      return Status.RUNNING;
    } else {
      return Status.COMPLETE;
    }
  }

  /**
   * 方法级注释：获取指定阶段和步骤关联的进度计数器，支持多线程原子增量，适合在循环中重复递增
   * 调用方可以一次性获取计数器实例后重复使用，避免重复查找，提升性能
   * 
   * @param phase 计数器所属阶段
   * @param step 计数器所属步骤
   * @return 关联指定阶段和步骤的进度计数器
   */
  public Counter getCounter(Phase phase, Step step) {
    if (!isComplete(phase)) {
      final StepTracking tracking = lazyInitStep(phase, step);
      return new Counter() {
        @Override
        public void increment() {
          tracking.count.incrementAndGet();
        }
      };
    } else {
      return new Counter() {
        @Override
        public void increment() {
          // no-op, because startup has completed
        }
      };
    }
  }

  /**
   * 方法级注释：直接设置指定阶段和步骤的当前计数
   * 
   * @param phase 目标阶段
   * @param step 目标步骤
   * @param count 要设置的当前计数值
   */
  public void setCount(Phase phase, Step step, long count) {
    lazyInitStep(phase, step).count.set(count);
  }

  /**
   * 方法级注释：设置指定阶段关联的可选文件名，例如加载fsimage时设置fsimage的完整路径
   * 
   * @param phase 目标阶段
   * @param file 要设置的文件名
   */
  public void setFile(Phase phase, String file) {
    if (!isComplete()) {
      phases.get(phase).file = file;
    }
  }

  /**
   * 方法级注释：设置指定阶段关联的可选文件大小（字节），例如加载fsimage时设置fsimage文件大小
   * 
   * @param phase 目标阶段
   * @param size 要设置的文件大小（字节）
   */
  public void setSize(Phase phase, long size) {
    if (!isComplete()) {
      phases.get(phase).size = size;
    }
  }

  /**
   * 方法级注释：设置指定阶段和步骤的总任务数，例如加载edits时设置需要应用的操作总数
   * 
   * @param phase 目标阶段
   * @param step 目标步骤
   * @param total 要设置的总任务数
   */
  public void setTotal(Phase phase, Step step, long total) {
    if (!isComplete(phase)) {
      lazyInitStep(phase, step).total = total;
    }
  }

  /**
   * 方法级注释：创建当前启动进度的只读快照视图，克隆当前所有跟踪数据
   * 创建后原进度更新不会影响视图，为读取方提供一致不变的快照，避免计算过程中并发修改导致异常
   * 
   * @return 包含当前克隆数据的启动进度视图
   */
  public StartupProgressView createView() {
    return new StartupProgressView(this);
  }

  /**
   * 方法级注释：检查整个NameNode启动过程是否已经全部完成，通过检查所有阶段都已完成来判断
   * 
   * @return 整个启动过程全部完成返回true，否则返回false
   */
  private boolean isComplete() {
    return EnumSet.allOf(Phase.class).stream().allMatch(this::isComplete);
  }

  /**
   * 方法级注释：检查指定启动阶段是否已经完成
   *
   * @param phase 要检查的启动阶段
   * @return 指定阶段已完成返回true，否则返回false
   */
  private boolean isComplete(Phase phase) {
    return getStatus(phase) == Status.COMPLETE;
  }

  /**
   * 方法级注释：延迟初始化指定阶段和步骤的跟踪数据结构，如果已存在则直接返回
   * 初始化是原子操作，多线程同时初始化同一步骤也不会出现数据丢失问题
   * 
   * @param phase 目标阶段
   * @param step 目标步骤
   * @return 初始化完成或已存在的步骤跟踪对象
   */
  private StepTracking lazyInitStep(Phase phase, Step step) {
    ConcurrentMap<Step, StepTracking> steps = phases.get(phase).steps;
    if (!steps.containsKey(step)) {
      steps.putIfAbsent(step, new StepTracking());
    }
    return steps.get(step);
  }
}