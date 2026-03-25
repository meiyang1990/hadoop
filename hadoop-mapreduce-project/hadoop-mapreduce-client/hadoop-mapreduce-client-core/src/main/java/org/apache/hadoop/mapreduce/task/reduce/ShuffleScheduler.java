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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.TaskCompletionEvent;

/**
 * Shuffle阶段调度器接口，定义了Reduce任务拉取Map输出数据的核心调度契约
 * 负责管理Map输出数据的拉取任务调度，协调Shuffle阶段的执行流程
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ShuffleScheduler<K,V> {

  /**
   * 等待Shuffle阶段完成或超时，阻塞当前线程直到满足结束条件
   * @param millis 最大等待时间，单位毫秒
   * @return true 表示Shuffle阶段已完成，false 表示超时未完成
   * @throws InterruptedException 等待过程被中断时抛出
   */
  public boolean waitUntilDone(int millis) throws InterruptedException;

  /**
   * 处理Map任务完成事件，解析事件中的Map输出元数据，将该Map输出加入拉取队列
   * @param tce Map任务完成事件，包含Map输出的位置等元信息
   * @throws IOException 解析事件或处理IO时抛出
   * @throws InterruptedException 处理过程被中断时抛出
   */
  public void resolve(TaskCompletionEvent tce)
    throws IOException, InterruptedException;

  /**
   * 关闭调度器，清理已分配资源，中断正在执行的拉取任务
   * @throws InterruptedException 关闭过程被中断时抛出
   */
  public void close() throws InterruptedException;

}