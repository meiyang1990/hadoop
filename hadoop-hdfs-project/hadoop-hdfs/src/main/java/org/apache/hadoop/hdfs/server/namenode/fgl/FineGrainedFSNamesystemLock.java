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
 * 文件系统细粒度锁管理器，将NameNode全局锁拆分为文件系统目录树锁和块/数据节点锁两个独立锁
 * 通过拆分全局锁提升并发性能：文件目录操作和块管理操作可以并行执行，降低锁竞争
 * 要求加锁顺序必须是：先获取FSLock文件系统锁，再获取BMLock块管理锁，避免死锁
 */
public class FineGrainedFSNamesystemLock implements FSNLockManager {
  private final FSNamesystemLock fsLock;
  private final FSNamesystemLock bmLock;

  /**
   * 构造细粒度锁管理器，初始化文件系统锁和块管理锁
   * @param conf Hadoop配置对象
   * @param aggregation 指标聚合容器，用于统计锁相关指标
   */
  public FineGrainedFSNamesystemLock(Configuration conf, MutableRatesWithAggregation aggregation) {
    this.fsLock = new FSNamesystemLock(conf, "FS", aggregation);
    this.bmLock = new FSNamesystemLock(conf, "BM", aggregation);
  }

  /**
   * 根据锁模式获取对应读锁
   * @param lockMode 锁模式：GLOBAL全局/FSM文件系统/BM块管理
   */
  @Override
  public void readLock(RwLockMode lockMode) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.fsLock.readLock();
      this.bmLock.readLock();
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.readLock();
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.readLock();
    }
  }

  /**
   * 可中断方式获取对应读锁，响应线程中断
   * @param lockMode 锁模式：GLOBAL全局/FSM文件系统/BM块管理
   * @throws InterruptedException 线程被中断时抛出
   */
  public void readLockInterruptibly(RwLockMode lockMode) throws InterruptedException  {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.fsLock.readLockInterruptibly();
      try {
        this.bmLock.readLockInterruptibly();
      } catch (InterruptedException e) {
        // 获取BMLock被中断时，释放已经拿到的FSLock避免锁泄漏
        this.fsLock.readUnlock("BMReadLockInterruptiblyFailed");
        throw e;
      }
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.readLockInterruptibly();
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.readLockInterruptibly();
    }
  }

  /**
   * 根据锁模式释放对应读锁
   * @param lockMode 锁模式：GLOBAL全局/FSM文件系统/BM块管理
   * @param opName 操作名称，用于日志和指标统计
   */
  @Override
  public void readUnlock(RwLockMode lockMode, String opName) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.bmLock.readUnlock(opName);
      this.fsLock.readUnlock(opName);
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.readUnlock(opName);
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.readUnlock(opName);
    }
  }

  /**
   * 根据锁模式释放对应读锁，支持自定义锁报告信息
   * @param lockMode 锁模式：GLOBAL全局/FSM文件系统/BM块管理
   * @param opName 操作名称，用于日志和指标统计
   * @param lockReportInfoSupplier 锁报告信息生成器
   */
  public void readUnlock(RwLockMode lockMode, String opName,
      Supplier<String> lockReportInfoSupplier) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.bmLock.readUnlock(opName, lockReportInfoSupplier);
      this.fsLock.readUnlock(opName, lockReportInfoSupplier);
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.readUnlock(opName, lockReportInfoSupplier);
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.readUnlock(opName, lockReportInfoSupplier);
    }
  }

  /**
   * 根据锁模式获取对应写锁
   * @param lockMode 锁模式：GLOBAL全局/FSM文件系统/BM块管理
   */
  @Override
  public void writeLock(RwLockMode lockMode) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.fsLock.writeLock();
      this.bmLock.writeLock();
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.writeLock();
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.writeLock();
    }
  }

  /**
   * 根据锁模式释放对应写锁
   * @param lockMode 锁模式：GLOBAL全局/FSM文件系统/BM块管理
   * @param opName 操作名称，用于日志和指标统计
   */
  @Override
  public void writeUnlock(RwLockMode lockMode, String opName) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.bmLock.writeUnlock(opName);
      this.fsLock.writeUnlock(opName);
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.writeUnlock(opName);
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.writeUnlock(opName);
    }
  }

  /**
   * 根据锁模式释放对应写锁，可抑制写锁持有过长报告
   * @param lockMode 锁模式：GLOBAL全局/FSM文件系统/BM块管理
   * @param opName 操作名称，用于日志和指标统计
   * @param suppressWriteLockReport 是否抑制写锁持有过长报告
   */
  @Override
  public void writeUnlock(RwLockMode lockMode, String opName,
      boolean suppressWriteLockReport) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.bmLock.writeUnlock(opName, suppressWriteLockReport);
      this.fsLock.writeUnlock(opName, suppressWriteLockReport);
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.writeUnlock(opName, suppressWriteLockReport);
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.writeUnlock(opName, suppressWriteLockReport);
    }
  }

  /**
   * 根据锁模式释放对应写锁，支持自定义锁报告信息
   * @param lockMode 锁模式：GLOBAL全局/FSM文件系统/BM块管理
   * @param opName 操作名称，用于日志和指标统计
   * @param lockReportInfoSupplier 锁报告信息生成器
   */
  public void writeUnlock(RwLockMode lockMode, String opName,
      Supplier<String> lockReportInfoSupplier) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.bmLock.writeUnlock(opName, lockReportInfoSupplier);
      this.fsLock.writeUnlock(opName, lockReportInfoSupplier);
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.writeUnlock(opName, lockReportInfoSupplier);
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.writeUnlock(opName, lockReportInfoSupplier);
    }
  }

  /**
   * 可中断方式获取对应写锁，响应线程中断
   * @param lockMode 锁模式：GLOBAL全局/FSM文件系统/BM块管理
   * @throws InterruptedException 线程被中断时抛出
   */
  @Override
  public void writeLockInterruptibly(RwLockMode lockMode)
      throws InterruptedException {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.fsLock.writeLockInterruptibly();
      try {
        this.bmLock.writeLockInterruptibly();
      } catch (InterruptedException e) {
        // 获取BMLock被中断时，释放已经拿到的FSLock避免锁泄漏
        this.fsLock.writeUnlock("BMWriteLockInterruptiblyFailed");
        throw e;
      }
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.writeLockInterruptibly();
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.writeLockInterruptibly();
    }
  }

  /**
   * 检查当前线程是否持有对应模式的写锁
   * @param lockMode 锁模式：GLOBAL全局/FSM文件系统/BM块管理
   * @return 当前线程持有写锁返回true，否则返回false
   */
  @Override
  public boolean hasWriteLock(RwLockMode lockMode) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      return this.fsLock.isWriteLockedByCurrentThread()
          && this.bmLock.isWriteLockedByCurrentThread();
    } else if (lockMode.equals(RwLockMode.FS)) {
      return this.fsLock.isWriteLockedByCurrentThread();
    } else if (lockMode.equals(RwLockMode.BM)) {
      return this.bmLock.isWriteLockedByCurrentThread();
    }
    return false;
  }

  /**
   * 检查当前线程是否持有对应模式的读锁（写锁隐式包含读锁权限）
   * @param lockMode 锁模式：GLOBAL全局/FSM文件系统/BM块管理
   * @return 当前线程持有读锁或写锁返回true，否则返回false
   */
  @Override
  public boolean hasReadLock(RwLockMode lockMode) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      return hasWriteLock(RwLockMode.GLOBAL) ||
          (this.fsLock.getReadHoldCount() > 0 && this.bmLock.getReadHoldCount() > 0);
    } else if (lockMode.equals(RwLockMode.FS)) {
      return this.fsLock.getReadHoldCount() > 0 || this.fsLock.isWriteLockedByCurrentThread();
    } else if (lockMode.equals(RwLockMode.BM)) {
      return this.bmLock.getReadHoldCount() > 0 || this.bmLock.isWriteLockedByCurrentThread();
    }
    return false;
  }

  /**
   * 获取当前线程对应锁模式的读锁持有计数，仅用于目录内容汇总统计场景
   * 全局模式下只返回FSLock的读锁计数
   * @param lockMode 锁模式：GLOBAL全局/FSM文件系统/BM块管理
   * @return 读锁持有计数
   */
  @Override
  public int getReadHoldCount(RwLockMode lockMode) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      return this.fsLock.getReadHoldCount();
    } else if (lockMode.equals(RwLockMode.FS)) {
      return this.fsLock.getReadHoldCount();
    } else if (lockMode.equals(RwLockMode.BM)) {
      return this.bmLock.getReadHoldCount();
    }
    return -1;
  }

  /**
   * 获取对应锁模式的等待队列长度
   * @param lockMode 锁模式：GLOBAL全局/FSM文件系统/BM块管理
   * @return 等待队列长度，全局模式返回-1表示不支持
   */
  @Override
  public int getQueueLength(RwLockMode lockMode) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      return -1;
    } else if (lockMode.equals(RwLockMode.FS)) {
      return this.fsLock.getQueueLength();
    } else if (lockMode.equals(RwLockMode.BM)) {
      return this.bmLock.getQueueLength();
    }
    return -1;
  }

  /**
   * 获取对应锁模式中长期持有读锁的数量
   * @param lockMode 锁模式：GLOBAL全局/FSM文件系统/BM块管理
   * @return 长期持有读锁数量，全局模式返回-1表示不支持
   */
  @Override
  public long getNumOfReadLockLongHold(RwLockMode lockMode) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      return -1;
    } else if (lockMode.equals(RwLockMode.FS)) {
      return this.fsLock.getNumOfReadLockLongHold();
    } else if (lockMode.equals(RwLockMode.BM)) {
      return this.bmLock.getNumOfReadLockLongHold();
    }
    return -1;
  }

  /**
   * 获取对应锁模式中长期持有写锁的数量
   * @param lockMode 锁模式：GLOBAL全局/FSM文件系统/BM块管理
   * @return 长期持有写锁数量，全局模式返回-1表示不支持
   */
  @Override
  public long getNumOfWriteLockLongHold(RwLockMode lockMode) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      return -1;
    } else if (lockMode.equals(RwLockMode.FS)) {
      return this.fsLock.getNumOfWriteLockLongHold();
    } else if (lockMode.equals(RwLockMode.BM)) {
      return this.bmLock.getNumOfWriteLockLongHold();
    }
    return -1;
  }

  /**
   * 检查锁指标采集是否开启
   * @return 开启返回true，否则返回false
   */
  @Override
  public boolean isMetricsEnabled() {
    return this.fsLock.isMetricsEnabled();
  }

  /**
   * 设置是否开启锁指标采集
   * @param metricsEnabled 是否开启指标采集
   */
  public void setMetricsEnabled(boolean metricsEnabled) {
    this.fsLock.setMetricsEnabled(metricsEnabled);
    this.bmLock.setMetricsEnabled(metricsEnabled);
  }

  /**
   * 设置读锁持有过长报告阈值
   * @param readLockReportingThresholdMs 阈值，单位毫秒
   */
  @Override
  public void setReadLockReportingThresholdMs(long readLockReportingThresholdMs) {
    this.fsLock.setReadLockReportingThresholdMs(readLockReportingThresholdMs);
    this.bmLock.setReadLockReportingThresholdMs(readLockReportingThresholdMs);
  }

  /**
   * 获取读锁持有过长报告阈值
   * @return 阈值，单位毫秒
   */
  @Override
  public long getReadLockReportingThresholdMs() {
    return this.fsLock.getReadLockReportingThresholdMs();
  }

  /**
   * 设置写锁持有过长报告阈值
   * @param writeLockReportingThresholdMs 阈值，单位毫秒
   */
  @Override
  public void setWriteLockReportingThresholdMs(long writeLockReportingThresholdMs) {
    this.fsLock.setWriteLockReportingThresholdMs(writeLockReportingThresholdMs);
    this.bmLock.setWriteLockReportingThresholdMs(writeLockReportingThresholdMs);
  }

  /**
   * 获取写锁持有过长报告阈值
   * @return 阈值，单位毫秒
   */
  @Override
  public long getWriteLockReportingThresholdMs() {
    return this.fsLock.getWriteLockReportingThresholdMs();
  }

  /**
   * 为测试设置自定义锁，该实现不支持此操作
   * @param lock 用于测试的锁对象
   */
  @Override
  public void setLockForTests(ReentrantReadWriteLock lock) {
    throw new UnsupportedOperationException("SetLockTests is unsupported");
  }

  /**
   * 获取测试用锁，该实现不支持此操作
   * @return 永远抛出异常不返回
   */
  @Override
  public ReentrantReadWriteLock getLockForTests() {
    throw new UnsupportedOperationException("SetLockTests is unsupported");
  }
}