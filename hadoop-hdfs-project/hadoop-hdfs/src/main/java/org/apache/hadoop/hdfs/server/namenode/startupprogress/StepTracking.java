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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 文件级注释：NameNode启动进度跟踪模块，用于追踪单个启动步骤进度的内部数据结构
 *
 * 类级注释：跟踪单个NameNode启动步骤进度的数据结构，维护步骤处理的计数和总量信息，继承AbstractTracking实现基础时间跟踪能力
 * 用于支持NameNode启动过程中各个子步骤的进度统计，可被克隆用于快照展示。
 */
@InterfaceAudience.Private
final class StepTracking extends AbstractTracking {
  // 已完成处理的计数，使用原子类保证线程安全
  AtomicLong count = new AtomicLong();
  // 步骤需要处理的总数量，Long.MIN_VALUE表示未设置
  long total = Long.MIN_VALUE;

  /**
   * 方法级注释：克隆当前StepTracking对象，生成一个独立的深度拷贝副本，用于保存进度快照
   * @return 克隆生成的新StepTracking对象
   */
  @Override
  public StepTracking clone() {
    StepTracking clone = new StepTracking();
    super.copy(clone);
    clone.count = new AtomicLong(count.get());
    clone.total = total;
    return clone;
  }

  /**
   * 方法级注释：生成包含当前所有跟踪字段的字符串表示，用于调试日志输出
   * @return 格式化的对象状态字符串
   */
  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("count", count)
        .append("total", total)
        .append("beginTime", beginTime)
        .append("endTime", endTime)
        .toString();
  }
}