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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 文件级注释：NameNode启动阶段进度跟踪数据结构，用于跟踪单个启动阶段的整体进度信息
 * 内部数据结构，用于跟踪NameNode启动过程中单个{@link Phase}阶段的进度
 */
@InterfaceAudience.Private
/**
 * 类级注释：NameNode单个启动阶段的进度跟踪容器，存储阶段整体信息和所有子步骤进度
 * 核心职责：维护启动阶段的元数据、开始/结束时间，以及该阶段下所有子步骤的进度跟踪信息
 * 设计用于支持NameNode启动进度的可观测性，方便展示启动过程各阶段进度
 */
final class PhaseTracking extends AbstractTracking {
  // 当前阶段处理的文件路径（如镜像文件、编辑日志文件路径）
  String file;
  // 当前阶段处理文件的总大小，Long.MIN_VALUE表示未设置
  long size = Long.MIN_VALUE;
  // 当前阶段包含的所有子步骤进度跟踪表，key为步骤定义，value为步骤进度信息
  final ConcurrentMap<Step, StepTracking> steps =
    new ConcurrentHashMap<Step, StepTracking>();

  /**
   * 克隆当前PhaseTracking对象，生成深度拷贝的独立副本
   * @return 深度拷贝后的PhaseTracking新对象
   */
  @Override
  public PhaseTracking clone() {
    PhaseTracking clone = new PhaseTracking();
    // 复制父类的开始时间、结束时间等公共属性
    super.copy(clone);
    // 复制当前处理文件路径
    clone.file = file;
    // 复制当前处理文件大小
    clone.size = size;
    // 深度克隆每个子步骤的进度信息
    for (Map.Entry<Step, StepTracking> entry: steps.entrySet()) {
      clone.steps.put(entry.getKey(), entry.getValue().clone());
    }
    return clone;
  }

  /**
   * 生成包含所有进度信息的字符串，用于日志调试输出
   * @return 格式化后的阶段进度信息字符串
   */
  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("file", file)
        .append("size", size)
        .append("steps", steps)
        .append("beginTime", beginTime)
        .append("endTime", endTime)
        .toString();
  }
}