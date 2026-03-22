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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystemLock;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * 全局FSNamesystem锁管理器实现，采用单全局锁方案管理NameNode文件系统元数据访问
 * 职责是包装统一的全局读写锁，代理所有锁操作给内部的FSNamesystemLock实例，
 * 适用于传统单全局锁的NameNode元数据访问并发控制场景
 */
public class GlobalFSNamesystemLock implements FSNLockManager {

  /** 被代理的全局FSNamesystem读写锁实例 */
  private final FSNamesystemLock lock;

  /**
   * 构造全局锁管理器，初始化内部的FSNamesystem锁实例
   * @param conf Hadoop配置对象，用于锁相关参数配置
   * @param aggregation 聚合指标统计对象，用于锁操作的性能指标收集
   */
  public GlobalFSNamesystemLock(Configuration conf, MutableRatesWithAggregation aggregation) {
    this.lock = new FSNamesystemLock(conf, "FSN", aggregation);
  }

  /**
   * 获取读锁
   * @param lockMode 读写锁模式，本实现中参数未使用，统一使用全局锁
   */
  @Override
  public void readLock(RwLockMode lockMode) {
    this.lock.readLock();
  }

  /**
   * 可中断方式获取读锁
   * @param lockMode 读写锁模式，本实现中参数未使用，统一使用全局锁
   * @throws InterruptedException 获取锁过程中被中断时抛出
   */
  public void readLockInterruptibly(RwLockMode lockMode) throws InterruptedException  {
    this.lock.readLockInterruptibly();
  }

  /**
   * 释放读锁
   * @param lockMode 读写锁模式，本实现中参数未使用，统一使用全局锁
   * @param opName 当前操作名称，用于锁日志统计
   */
  @Override
  public void readUnlock(RwLockMode lockMode, String opName) {
    this.lock.readUnlock(opName);
  }

  /**
   * 带额外锁报告信息的读锁释放方法
   * @param lockMode 读写锁模式，本实现中参数未使用，统一使用全局锁
   * @param opName 当前操作名称，用于锁日志统计
   * @param lockReportInfoSupplier 额外锁报告信息供应商，用于诊断日志
   */
  public void readUnlock(RwLockMode lockMode, String opName,
      Supplier<String> lockReportInfoSupplier) {
    this.lock.readUnlock(opName, lockReportInfoSupplier);
  }

  /**
   * 获取写锁
   * @param lockMode 读写锁模式，本实现中参数未使用，统一使用全局锁
   */
  @Override
  public void writeLock(RwLockMode lockMode) {
    this.lock.writeLock();
  }

  /**
   * 释放写锁
   * @param lockMode 读写锁模式，本实现中参数未使用，统一使用全局锁
   * @param opName 当前操作名称，用于锁日志统计
   */
  @Override
  public void writeUnlock(RwLockMode lockMode, String opName) {
    this.lock.writeUnlock(opName);
  }

  /**
   * 释放写锁，可指定是否抑制锁报告
   * @param lockMode 读写锁模式，本实现中参数未使用，统一使用全局锁
   * @param opName 当前操作名称，用于锁日志统计
   * @param suppressWriteLockReport 是否抑制写锁超时报告
   */
  @Override
  public void writeUnlock(RwLockMode lockMode, String opName,
      boolean suppressWriteLockReport) {
    this.lock.writeUnlock(opName, suppressWriteLockReport);
  }

  /**
   * 带额外锁报告信息的写锁释放方法
   * @param lockMode 读写锁模式，本实现中参数未使用，统一使用全局锁
   * @param opName 当前操作名称，用于锁日志统计
   * @param lockReportInfoSupplier 额外锁报告信息供应商，用于诊断日志
   */
  public void writeUnlock(RwLockMode lockMode, String opName,
      Supplier<String> lockReportInfoSupplier) {
    this.lock.writeUnlock(opName, lockReportInfoSupplier);
  }

  /**
   * 可中断方式获取写锁
   * @param lockMode 读写锁模式，本实现中参数未使用，统一使用全局锁
   * @throws InterruptedException 获取锁过程中被中断时抛出
   */
  @Override
  public void writeLockInterruptibly(RwLockMode lockMode)
      throws InterruptedException {
    this.lock.writeLockInterruptibly();
  }

  /**
   * 检查当前线程是否持有写锁
   * @param lockMode 读写锁模式，本实现中参数未使用，统一使用全局锁
   * @return 当前线程持有写锁返回true，否则返回false
   */
  @Override
  public boolean hasWriteLock(RwLockMode lockMode) {
    return this.lock.isWriteLockedByCurrentThread();
  }

  /**
   * 检查当前线程是否持有读锁（包含持有写锁的情况，因为写锁隐含读权限）
   * @param lockMode 读写锁模式，本实现中参数未使用，统一使用全局锁
   * @return 当前线程持有读锁或写锁返回true，否则返回false
   */
  @Override
  public boolean hasReadLock(RwLockMode lockMode) {
    return this.lock.getReadHoldCount() > 0 || hasWriteLock(lockMode);
  }

  /**
   * 获取当前线程的读锁持有计数
   * @param lockMode 读写锁模式，本实现中参数未使用，统一使用全局锁
   * @return 当前线程持有的读锁数量
   */
  @Override
  public int getReadHoldCount(RwLockMode lockMode) {
    return this.lock.getReadHoldCount();
  }

  /**
   * 获取锁等待队列长度
   * @param lockMode 读写锁模式，本实现中参数未使用，统一使用全局锁
   * @return 等待获取锁的线程数
   */
  @Override
  public int getQueueLength(RwLockMode lockMode) {
    return this.lock.getQueueLength();
  }

  /**
   * 获取长期持有读锁的统计数量
   * @param lockMode 读写锁模式，本实现中参数未使用，统一使用全局锁
   * @return 长期持有读锁的次数
   */
  @Override
  public long getNumOfReadLockLongHold(RwLockMode lockMode) {
    return this.lock.getNumOfReadLockLongHold();
  }

  /**
   * 获取长期持有写锁的统计数量
   * @param lockMode 读写锁模式，本实现中参数未使用，统一使用全局锁
   * @return 长期持有写锁的次数
   */
  @Override
  public long getNumOfWriteLockLongHold(RwLockMode lockMode) {
    return this.lock.getNumOfWriteLockLongHold();
  }

  /**
   * 检查锁指标采集是否启用
   * @return 指标采集启用返回true，否则返回false
   */
  @Override
  public boolean isMetricsEnabled() {
    return this.lock.isMetricsEnabled();
  }

  /**
   * 设置是否启用锁指标采集
   * @param metricsEnabled true表示启用，false表示禁用
   */
  public void setMetricsEnabled(boolean metricsEnabled) {
    this.lock.setMetricsEnabled(metricsEnabled);
  }

  /**
   * 设置读锁持有超时报告阈值
   * @param readLockReportingThresholdMs 阈值，单位毫秒，超过该阈值会输出警告日志
   */
  @Override
  public void setReadLockReportingThresholdMs(long readLockReportingThresholdMs) {
    this.lock.setReadLockReportingThresholdMs(readLockReportingThresholdMs);
  }

  /**
   * 获取当前读锁持有超时报告阈值
   * @return 阈值，单位毫秒
   */
  @Override
  public long getReadLockReportingThresholdMs() {
    return this.lock.getReadLockReportingThresholdMs();
  }

  /**
   * 设置写锁持有超时报告阈值
   * @param writeLockReportingThresholdMs 阈值，单位毫秒，超过该阈值会输出警告日志
   */
  @Override
  public void setWriteLockReportingThresholdMs(long writeLockReportingThresholdMs) {
    this.lock.setWriteLockReportingThresholdMs(writeLockReportingThresholdMs);
  }

  /**
   * 获取当前写锁持有超时报告阈值
   * @return 阈值，单位毫秒
   */
  @Override
  public long getWriteLockReportingThresholdMs() {
    return this.lock.getWriteLockReportingThresholdMs();
  }

  /**
   * 为单元测试设置自定义锁实例，用于测试
   * @param testLock 测试用的读写锁实例
   */
  @Override
  public void setLockForTests(ReentrantReadWriteLock testLock) {
    this.lock.setLockForTests(testLock);
  }

  /**
   * 获取当前用于测试的锁实例
   * @return 测试用读写锁实例
   */
  @Override
  public ReentrantReadWriteLock getLockForTests() {
    return this.lock.getLockForTests();
  }
}