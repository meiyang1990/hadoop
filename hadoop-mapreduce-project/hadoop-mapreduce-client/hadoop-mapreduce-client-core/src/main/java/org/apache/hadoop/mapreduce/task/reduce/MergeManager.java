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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.IOException;

/**
 * MergeManager 合并管理器接口，定义了Reduce端合并阶段与默认Shuffle实现交互的统一契约
 * 负责管理Map输出合并过程中的资源分配与结果获取，是合并逻辑的抽象接口
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface MergeManager<K, V> {
  /**
   * 等待合并阶段释放出可用资源，用于接收新的Shuffle数据
   * 在建立网络连接获取Map输出之前调用，确保资源足够后再拉取数据
   * @throws InterruptedException 等待过程中被中断时抛出
   */
  public void waitForResource() throws InterruptedException;

  /**
   * 为即将拉取的Shuffle数据预留资源
   * 在建立网络连接之后调用，预分配资源用于存放即将拉取的Map输出
   * @param mapId 本次拉取数据所属的Map任务尝试ID
   * @param requestedSize 本次拉取数据的字节大小
   * @param fetcher 执行本次拉取的Fetcher编号
   * @return 预留资源对应的MapOutput对象，如果无法立即预留资源则返回null
   * @throws IOException 资源预留过程中发生IO错误时抛出
   */
  public MapOutput<K, V> reserve(TaskAttemptID mapId, long requestedSize,
                                 int fetcher) throws IOException;

  /**
   * Shuffle阶段结束时调用，完成所有合并操作并返回最终迭代器
   * @return 可遍历合并后所有键值对的原始迭代器
   * @throws Throwable 合并过程发生任何错误时抛出
   */
  public RawKeyValueIterator close() throws Throwable;
}