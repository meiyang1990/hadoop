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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.util.Canceler;

import org.apache.hadoop.util.Preconditions;

/**
 * 文件级注释：HDFS NameNode 保存命名空间操作的上下文容器，用于保存当前正在执行的SaveNamespace操作状态，
 * 支持操作取消，并负责收集执行过程中出现错误的存储目录，协调操作完成同步。
 *
 * 类级注释：正在执行的保存命名空间（SaveNamespace）操作上下文，用于管理操作生命周期、支持取消操作、
 * 收集失败存储目录，并实现操作完成的同步通知。
 */
@InterfaceAudience.Private
public class SaveNamespaceContext {
  // 源命名空间，提供要保存的元数据
  private final FSNamesystem sourceNamesystem;
  // 当前保存操作对应的事务ID
  private final long txid;
  // 存储执行过程中发生错误的存储目录列表，线程安全
  private final List<StorageDirectory> errorSDs =
    Collections.synchronizedList(new ArrayList<StorageDirectory>());
  
  // 取消处理器，用于检查操作是否被取消
  private final Canceler canceller;
  // 完成锁存器，用于等待操作完成
  private final CountDownLatch completionLatch = new CountDownLatch(1);

  /**
   * 构造保存命名空间上下文，初始化各状态变量。
   * @param sourceNamesystem 源命名空间，提供待保存的元数据
   * @param txid 当前保存操作对应的事务ID
   * @param canceller 取消处理器，用于检查操作取消状态
   */
  SaveNamespaceContext(
      FSNamesystem sourceNamesystem,
      long txid,
      Canceler canceller) {
    this.sourceNamesystem = sourceNamesystem;
    this.txid = txid;
    this.canceller = canceller;
  }

  /**
   * 获取提供待保存元数据的源FSNamesystem。
   * @return 源命名空间对象
   */
  FSNamesystem getSourceNamesystem() {
    return sourceNamesystem;
  }

  /**
   * 获取当前保存操作对应的事务ID。
   * @return 事务ID
   */
  long getTxId() {
    return txid;
  }

  /**
   * 上报一个存储目录保存失败，将其添加到错误目录列表。
   * @param sd 发生错误的存储目录
   */
  void reportErrorOnStorageDirectory(StorageDirectory sd) {
    errorSDs.add(sd);
  }

  /**
   * 获取所有保存失败的存储目录列表。
   * @return 错误存储目录列表
   */
  List<StorageDirectory> getErrorSDs() {
    return errorSDs;
  }

  /**
   * 标记保存命名空间操作已完成，释放等待线程。
   */
  void markComplete() {
    // 检查状态，确保上下文只被标记完成一次
    Preconditions.checkState(completionLatch.getCount() == 1,
        "Context already completed!");
    completionLatch.countDown();
  }

  /**
   * 检查当前操作是否已被取消，如果已取消则抛出异常。
   * @throws SaveNamespaceCancelledException 如果操作已被取消，抛出该异常
   */
  public void checkCancelled() throws SaveNamespaceCancelledException {
    if (canceller.isCancelled()) {
      throw new SaveNamespaceCancelledException(
          canceller.getCancellationReason());
    }
  }
}