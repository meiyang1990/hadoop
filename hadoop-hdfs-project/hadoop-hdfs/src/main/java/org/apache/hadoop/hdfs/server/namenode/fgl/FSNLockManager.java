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
package org.apache.hadoop.hdfs.server.namenode.fgl;

import org.apache.hadoop.classification.VisibleForTesting;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.apache.hadoop.hdfs.util.RwLockMode;

/**
 * NameNode读写锁管理器接口，定义了不同锁模式下的锁获取、释放和监控能力，
 * 为NameNode的细粒度锁管理提供统一抽象，支持分离读和写操作的锁管理与指标统计。
 */
public interface FSNLockManager {

  /**
   * 根据锁模式获取对应读锁，阻塞等待直到获取成功。
   * @param lockMode 锁模式
   */
  void readLock(RwLockMode lockMode);

  /**
   * 根据锁模式获取对应读锁，可被中断，等待过程中线程中断会抛出异常。
   * @param lockMode 锁模式
   * @throws InterruptedException 如果线程在等待过程中被中断则抛出该异常
   */
  void readLockInterruptibly(RwLockMode lockMode) throws InterruptedException;

  /**
   * 根据锁模式释放对应读锁。
   * @param lockMode 锁模式
   * @param opName 当前操作名称，用于日志和监控
   */
  void readUnlock(RwLockMode lockMode, String opName);

  /**
   * 根据锁模式释放对应读锁，支持额外自定义锁报告信息。
   * @param lockMode 锁模式
   * @param opName 当前操作名称，用于日志和监控
   * @param lockReportInfoSupplier 用于生成锁报告额外信息的提供者
   */
  void readUnlock(RwLockMode lockMode, String opName,
      Supplier<String> lockReportInfoSupplier);

  /**
   * 根据锁模式获取对应写锁，阻塞等待直到获取成功。
   * @param lockMode 锁模式
   */
  void writeLock(RwLockMode lockMode);

  /**
   * 根据锁模式释放对应写锁。
   * @param lockMode 锁模式
   * @param opName 当前操作名称，用于日志和监控
   */
  void writeUnlock(RwLockMode lockMode, String opName);

  /**
   * 根据锁模式释放对应写锁，可选择抑制长时间占用写锁的报告。
   * @param lockMode 锁模式
   * @param opName 当前操作名称，用于日志和监控
   * @param suppressWriteLockReport 当为false时，会记录长时间占用写锁的事件到日志和指标
   */
  void writeUnlock(RwLockMode lockMode, String opName,
      boolean suppressWriteLockReport);

  /**
   * 根据锁模式释放对应写锁，支持额外自定义锁报告信息。
   * @param lockMode 锁模式
   * @param opName 当前操作名称，用于日志和监控
   * @param lockReportInfoSupplier 用于生成锁报告额外信息的提供者
   */
  void writeUnlock(RwLockMode lockMode, String opName,
      Supplier<String> lockReportInfoSupplier);

  /**
   * 根据锁模式获取对应写锁，可被中断，等待过程中线程中断会抛出异常。
   * @param lockMode 锁模式
   * @throws InterruptedException 如果线程在等待过程中被中断则抛出该异常
   */
  void writeLockInterruptibly(RwLockMode lockMode) throws InterruptedException;

  /**
   * 检查当前线程是否持有对应锁模式的写锁。
   * @param lockMode 锁模式
   * @return 当前线程持有该写锁返回true，否则返回false
   */
  boolean hasWriteLock(RwLockMode lockMode);

  /**
   * 检查当前线程是否持有对应锁模式的读锁。
   * @param lockMode 锁模式
   * @return 当前线程持有该读锁返回true，否则返回false
   */
  boolean hasReadLock(RwLockMode lockMode);

  /**
   * 获取当前线程对对应锁模式读锁的重入持有次数。
   * 每次获取锁未释放都会增加计数，每个匹配的解锁会减少计数。
   * @param lockMode 锁模式
   * @return 当前线程持有该读锁的次数，未持有则返回0
   */
  int getReadHoldCount(RwLockMode lockMode);

  /**
   * 获取当前等待获取该锁的线程队列长度，数值越大表示锁竞争越激烈。
   * @param lockMode 锁模式
   * @return 等待该锁的线程数量
   */
  int getQueueLength(RwLockMode lockMode);

  /**
   * 获取读锁持有时间超过阈值的总次数。
   * @param lockMode 锁模式
   * @return 读锁长时间持有次数
   */
  long getNumOfReadLockLongHold(RwLockMode lockMode);

  /**
   * 获取写锁持有时间超过阈值的总次数。
   * @param lockMode 锁模式
   * @return 写锁长时间持有次数
   */
  long getNumOfWriteLockLongHold(RwLockMode lockMode);

  /**
   * 检查锁指标统计是否已启用。
   * @return 启用返回true，否则返回false
   */
  boolean isMetricsEnabled();

  /**
   * 设置锁指标统计是否启用。
   * @param metricsEnabled 是否启用指标统计
   */
  void setMetricsEnabled(boolean metricsEnabled);

  /**
   * 设置读锁长时间持有报告阈值，超过该阈值会记录为一次长时间持有。
   * @param readLockReportingThresholdMs 报告阈值，单位毫秒
   */
  void setReadLockReportingThresholdMs(long readLockReportingThresholdMs);

  /**
   * 获取读锁长时间持有报告阈值。
   * @return 报告阈值，单位毫秒
   */
  long getReadLockReportingThresholdMs();

  /**
   * 设置写锁长时间持有报告阈值，超过该阈值会记录为一次长时间持有。
   * @param writeLockReportingThresholdMs 报告阈值，单位毫秒
   */
  void setWriteLockReportingThresholdMs(long writeLockReportingThresholdMs);

  /**
   * 获取写锁长时间持有报告阈值。
   * @return 报告阈值，单位毫秒
   */
  long getWriteLockReportingThresholdMs();

  /**
   * 仅用于测试：设置测试用的锁实例。
   * @param lock 测试锁实例
   */
  @VisibleForTesting
  void setLockForTests(ReentrantReadWriteLock lock);

  /**
   * 仅用于测试：获取当前管理的锁实例。
   * @return 当前锁实例
   */
  @VisibleForTesting
  ReentrantReadWriteLock getLockForTests();
}