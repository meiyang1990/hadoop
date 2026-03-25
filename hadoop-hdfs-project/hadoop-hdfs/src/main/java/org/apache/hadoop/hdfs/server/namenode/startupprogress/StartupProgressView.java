// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.Time;

/**
 * NameNode启动进度的不可变一致只读快照视图。
 * 调用者通过{@link StartupProgress#createView()}获取实例，克隆当前启动进度状态生成视图。
 * 后续启动进度的更新不会影响本视图，保证多次读取操作获取的数据一致稳定，
 * 避免其他线程并发修改导致计算（如整体完成度）过程中数据不一致问题。
 * 返回基本类型long的方法可能返回{@link Long#MIN_VALUE}作为标记值，表示该属性未定义。
 */
@InterfaceAudience.Private
public class StartupProgressView {

  private final Map<Phase, PhaseTracking> phases;

  /**
   * 获取指定阶段下所有步骤的计数器值总和。
   *
   * @param phase 目标启动阶段
   * @return 该阶段所有步骤的计数器值总和
   */
  public long getCount(Phase phase) {
    long sum = 0;
    for (Step step: getSteps(phase)) {
      sum += getCount(phase, step);
    }
    return sum;
  }

  /**
   * 获取指定阶段下指定步骤的计数器值。
   *
   * @param phase 目标启动阶段
   * @param step 目标步骤
   * @return 指定阶段步骤的计数器值，不存在则返回0
   */
  public long getCount(Phase phase, Step step) {
    StepTracking tracking = getStepTracking(phase, step);
    return tracking != null ? tracking.count.get() : 0;
  }

  /**
   * 获取NameNode启动总耗时，计算从加载fsimage开始到安全模式结束的时间差。
   *
   * @return 启动总耗时（毫秒）
   */
  public long getElapsedTime() {
    return getElapsedTime(phases.get(Phase.LOADING_FSIMAGE),
      phases.get(Phase.SAFEMODE));
  }

  /**
   * 获取指定阶段的耗时，已完成阶段返回结束时间减开始时间，运行中返回当前时间减开始时间，未启动返回0。
   *
   * @param phase 目标启动阶段
   * @return 指定阶段耗时（毫秒）
   */
  public long getElapsedTime(Phase phase) {
    return getElapsedTime(phases.get(phase));
  }

  /**
   * 获取指定阶段下指定步骤的耗时，已完成返回结束时间减开始时间，运行中返回当前时间减开始时间，未启动返回0。
   *
   * @param phase 目标启动阶段
   * @param step 目标步骤
   * @return 指定阶段步骤耗时（毫秒）
   */
  public long getElapsedTime(Phase phase, Step step) {
    return getElapsedTime(getStepTracking(phase, step));
  }

  /**
   * 获取指定阶段关联的文件名，可能为null。
   *
   * @param phase 目标启动阶段
   * @return 关联文件名，无则返回null
   */
  public String getFile(Phase phase) {
    return phases.get(phase).file;
  }

  /**
   * 获取NameNode启动整体完成百分比，通过对所有阶段完成百分比取平均计算得到。
   * 该计算假设所有阶段耗时权重相等，是近似值，因无法提前预测各阶段实际耗时比例。
   *
   * @return 整体完成百分比，范围[0.0, 1.0]
   */
  public float getPercentComplete() {
    // 安全模式已完成则代表启动全部完成
    if (getStatus(Phase.SAFEMODE) == Status.COMPLETE) {
      return 1.0f;
    } else {
      float total = 0.0f;
      int numPhases = 0;
      for (Phase phase: phases.keySet()) {
        ++numPhases;
        total += getPercentComplete(phase);
      }
      // 限制结果在合法范围内
      return getBoundedPercent(total / numPhases);
    }
  }

  /**
   * 获取指定阶段的完成百分比，聚合阶段内所有步骤的计数器和总数值计算得到。
   *
   * @param phase 目标启动阶段
   * @return 指定阶段完成百分比，范围[0.0, 1.0]
   */
  public float getPercentComplete(Phase phase) {
    if (getStatus(phase) == Status.COMPLETE) {
      return 1.0f;
    } else {
      long total = getTotal(phase);
      long count = 0;
      for (Step step: getSteps(phase)) {
        count += getCount(phase, step);
      }
      return total > 0 ? getBoundedPercent(1.0f * count / total) : 0.0f;
    }
  }

  /**
   * 获取指定阶段下指定步骤的完成百分比，通过计数器值除以总数值计算得到。
   *
   * @param phase 目标启动阶段
   * @param step 目标步骤
   * @return 指定阶段步骤完成百分比，范围[0.0, 1.0]
   */
  public float getPercentComplete(Phase phase, Step step) {
    if (getStatus(phase) == Status.COMPLETE) {
      return 1.0f;
    } else {
      long total = getTotal(phase, step);
      long count = getCount(phase, step);
      return total > 0 ? getBoundedPercent(1.0f * count / total) : 0.0f;
    }
  }

  /**
   * 获取所有启动阶段的迭代器。
   *
   * @return 包含所有启动阶段的迭代器
   */
  public Iterable<Phase> getPhases() {
    return EnumSet.allOf(Phase.class);
  }

  /**
   * 获取指定阶段下所有步骤的迭代器。
   *
   * @param phase 目标启动阶段
   * @return 包含指定阶段所有步骤的迭代器
   */
  public Iterable<Step> getSteps(Phase phase) {
    return new TreeSet<Step>(phases.get(phase).steps.keySet());
  }

  /**
   * 获取指定阶段关联的字节大小，未定义则返回Long.MIN_VALUE。
   *
   * @param phase 目标启动阶段
   * @return 关联字节大小，未定义则返回Long.MIN_VALUE
   */
  public long getSize(Phase phase) {
    return phases.get(phase).size;
  }

  /**
   * 获取指定阶段的当前运行状态。
   *
   * @param phase 目标启动阶段
   * @return 阶段运行状态（PENDING未开始/RUNNING运行中/COMPLETE已完成）
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
   * 获取指定阶段下所有步骤的总数值总和。
   *
   * @param phase 目标启动阶段
   * @return 指定阶段所有步骤总数值总和
   */
  public long getTotal(Phase phase) {
    long sum = 0;
    for (StepTracking tracking: phases.get(phase).steps.values()) {
      if (tracking.total != Long.MIN_VALUE) {
        sum += tracking.total;
      }
    }
    return sum;
  }

  /**
   * 获取指定阶段下指定步骤的总数值。
   *
   * @param phase 目标启动阶段
   * @param step 目标步骤
   * @return 指定阶段步骤总数值，不存在或未定义返回0
   */
  public long getTotal(Phase phase, Step step) {
    StepTracking tracking = getStepTracking(phase, step);
    return tracking != null && tracking.total != Long.MIN_VALUE ?
      tracking.total : 0;
  }

  /**
   * 通过克隆StartupProgress的当前状态构造启动进度快照视图。
   *
   * @param prog 源StartupProgress实例，用于克隆数据
   */
  StartupProgressView(StartupProgress prog) {
    phases = new HashMap<Phase, PhaseTracking>();
    for (Map.Entry<Phase, PhaseTracking> entry: prog.phases.entrySet()) {
      phases.put(entry.getKey(), entry.getValue().clone());
    }
  }

  /**
   * 根据单个AbstractTracking实例计算耗时，开始和结束时间来自同一实例。
   *
   * @param tracking 包含开始和结束时间的追踪对象
   * @return 计算得到的耗时（毫秒）
   */
  private long getElapsedTime(AbstractTracking tracking) {
    return getElapsedTime(tracking, tracking);
  }

  /**
   * 根据两个不同AbstractTracking实例计算耗时，开始时间来自第一个，结束时间来自第二个。
   * 已完成返回结束时间减开始时间，运行中返回当前时间减开始时间，未开始返回0，结果保证非负。
   *
   * @param beginTracking 包含开始时间的追踪对象
   * @param endTracking 包含结束时间的追踪对象
   * @return 计算得到的耗时（毫秒）
   */
  private long getElapsedTime(AbstractTracking beginTracking,
      AbstractTracking endTracking) {
    final long elapsed;
    // 开始和结束时间都已定义，计算总耗时
    if (beginTracking != null && beginTracking.beginTime != Long.MIN_VALUE &&
        endTracking != null && endTracking.endTime != Long.MIN_VALUE) {
      elapsed = endTracking.endTime - beginTracking.beginTime;
    } 
    // 只有开始时间，计算从开始到当前的耗时
    else if (beginTracking != null &&
        beginTracking.beginTime != Long.MIN_VALUE) {
      elapsed = Time.monotonicNow() - beginTracking.beginTime;
    } 
    // 未开始，耗时为0
    else {
      elapsed = 0;
    }
    return Math.max(0, elapsed);
  }

  /**
   * 获取指定阶段和步骤对应的StepTracking内部对象，不存在则返回null。
   *
   * @param phase 目标启动阶段
   * @param step 目标步骤
   * @return 对应的StepTracking对象，不存在则返回null
   */
  private StepTracking getStepTracking(Phase phase, Step step) {
    PhaseTracking phaseTracking = phases.get(phase);
    Map<Step, StepTracking> steps = phaseTracking != null ?
      phaseTracking.steps : null;
    return steps != null ? steps.get(step) : null;
  }

  /**
   * 将百分比限制在[0.0, 1.0]合法范围内，避免计算误差导致越界。
   *
   * @param percent 原始计算得到的百分比
   * @return 限制在合法范围内的百分比
   */
  private static float getBoundedPercent(float percent) {
    return Math.max(0.0f, Math.min(1.0f, percent));
  }
}