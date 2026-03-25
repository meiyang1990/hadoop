// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode.checker;

import java.util.Optional;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * HDFS DataNode 磁盘块检查异步调度接口，用于对可检查对象调度异步健康检查任务，
 * 异步执行检查避免阻塞DataNode主业务线程，支持获取检查结果。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface AsyncChecker<K, V> {

  /**
   * 为指定可检查对象调度一个异步健康检查任务
   * @param target 待检查的目标可检查对象
   * @param context 检查上下文，具体含义由目标对象决定
   * @return 包含可监听未来结果的Optional对象，如果调度成功返回结果未来对象，否则返回空
   */
  Optional<ListenableFuture<V>> schedule(Checkable<K, V> target, K context);

  /**
   * 关闭异步检查器并等待所有正在执行的检查任务完成，先尝试优雅取消所有任务，再强制取消，
   * 最后等待超时时间确保所有任务退出
   * @param timeout 等待终止的超时时间
   * @param timeUnit 超时时间单位
   * @throws InterruptedException 等待过程中被中断时抛出
   */
  void shutdownAndWait(long timeout, TimeUnit timeUnit)
      throws InterruptedException;
}