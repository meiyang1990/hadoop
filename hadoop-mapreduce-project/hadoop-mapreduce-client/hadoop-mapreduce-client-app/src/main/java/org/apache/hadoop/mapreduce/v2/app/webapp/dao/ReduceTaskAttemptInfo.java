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

package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.yarn.util.Times;

/**
 * Reduce任务尝试运行信息数据对象，用于WebUI展示Reduce任务各阶段的耗时与时间信息
 * 继承自TaskAttemptInfo，扩展了Shuffle、Merge、Reduce三个阶段的专项统计
 */
@XmlRootElement(name = "taskAttempt")
@XmlType(name = "")
public class ReduceTaskAttemptInfo extends TaskAttemptInfo {

  // Shuffle阶段完成时间
  protected long shuffleFinishTime;
  // Merge(Sort)阶段完成时间
  protected long mergeFinishTime;
  // Shuffle阶段已消耗时间
  protected long elapsedShuffleTime;
  // Merge阶段已消耗时间
  protected long elapsedMergeTime;
  // Reduce计算阶段已消耗时间
  protected long elapsedReduceTime;

  /**
   * 无参构造器，用于JAXB序列化反序列化
   */
  public ReduceTaskAttemptInfo() {
  }

  /**
   * 构造Reduce任务尝试信息，默认任务未运行标记
   * @param ta Reduce任务尝试对象
   */
  public ReduceTaskAttemptInfo(TaskAttempt ta) {
    this(ta, false);
  }

  /**
   * 构造Reduce任务尝试信息，计算各阶段耗时
   * @param ta Reduce任务尝试对象
   * @param isRunning 任务是否正在运行
   */
  public ReduceTaskAttemptInfo(TaskAttempt ta, Boolean isRunning) {
    super(ta, TaskType.REDUCE, isRunning);

    // 获取Shuffle阶段完成时间
    this.shuffleFinishTime = ta.getShuffleFinishTime();
    // 获取合并排序阶段完成时间
    this.mergeFinishTime = ta.getSortFinishTime();
    // 计算Shuffle阶段耗时
    this.elapsedShuffleTime = Times.elapsed(this.startTime,
        this.shuffleFinishTime, false);
    // 无效耗时设为0
    if (this.elapsedShuffleTime == -1) {
      this.elapsedShuffleTime = 0;
    }
    // 计算Merge阶段耗时
    this.elapsedMergeTime = Times.elapsed(this.shuffleFinishTime,
        this.mergeFinishTime, false);
    // 无效耗时设为0
    if (this.elapsedMergeTime == -1) {
      this.elapsedMergeTime = 0;
    }
    // 计算Reduce计算阶段耗时
    this.elapsedReduceTime = Times.elapsed(this.mergeFinishTime,
        this.finishTime, false);
    // 无效耗时设为0
    if (this.elapsedReduceTime == -1) {
      this.elapsedReduceTime = 0;
    }
  }

  /**
   * 获取Shuffle阶段完成时间
   * @return Shuffle完成时间戳
   */
  public long getShuffleFinishTime() {
    return this.shuffleFinishTime;
  }

  /**
   * 获取Merge阶段完成时间
   * @return Merge完成时间戳
   */
  public long getMergeFinishTime() {
    return this.mergeFinishTime;
  }

  /**
   * 获取Shuffle阶段已消耗时间
   * @return Shuffle阶段耗时（毫秒）
   */
  public long getElapsedShuffleTime() {
    return this.elapsedShuffleTime;
  }

  /**
   * 获取Merge阶段已消耗时间
   * @return Merge阶段耗时（毫秒）
   */
  public long getElapsedMergeTime() {
    return this.elapsedMergeTime;
  }

  /**
   * 获取Reduce计算阶段已消耗时间
   * @return Reduce计算阶段耗时（毫秒）
   */
  public long getElapsedReduceTime() {
    return this.elapsedReduceTime;
  }
}