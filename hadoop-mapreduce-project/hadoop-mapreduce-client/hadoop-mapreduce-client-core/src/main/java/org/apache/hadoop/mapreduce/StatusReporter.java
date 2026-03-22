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
package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * MapReduce任务状态与指标报告抽象接口，供Map/Reduce任务向ApplicationMaster上报进度、计数器和自定义状态信息
 */
@InterfaceAudience.Private
public abstract class StatusReporter {
  /**
   * 根据枚举类型获取对应计数器
   * @param name 枚举类型定义的计数器名称
   * @return 对应计数器实例
   */
  public abstract Counter getCounter(Enum<?> name);
  /**
   * 根据分组和名称获取对应计数器
   * @param group 计数器分组名称
   * @param name 计数器名称
   * @return 对应计数器实例
   */
  public abstract Counter getCounter(String group, String name);
  /**
   * 上报任务进度，告知ApplicationMaster任务仍在正常运行，更新最后活动时间
   */
  public abstract void progress();
  /**
   * 获取当前任务的完成进度
   * @return 进度值，范围0.0到1.0之间（包含端点），表示任务完成比例
   */
  public abstract float getProgress();
  /**
   * 设置自定义任务状态信息，展示在Web UI和任务日志中
   * @param status 自定义状态描述文本
   */
  public abstract void setStatus(String status);
}