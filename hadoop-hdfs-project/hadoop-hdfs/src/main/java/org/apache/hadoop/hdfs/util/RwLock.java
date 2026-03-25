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
package org.apache.hadoop.hdfs.util;

/**
 * 文件级注释：HDFS文件系统命名空间使用的读写锁接口，定义了不同模式读写锁的统一操作规范
 * 为FSNamesystem提供可扩展的读写锁抽象，支持全局锁和分段锁等多种模式，提升并发访问性能
 * 
 * 读写锁接口，专门为HDFS的FSNamesystem设计，提供读写分离的并发控制能力
 */
public interface RwLock {
  /**
   * 获取默认全局模式的读锁
   */
  default void readLock() {
    readLock(RwLockMode.GLOBAL);
  }

  /**
   * 根据指定锁模式获取读锁
   * @param lockMode 要获取的读锁模式
   */
  void readLock(RwLockMode lockMode);

  /**
   * 可中断方式获取默认全局模式读锁，等待过程中可响应中断
   * @throws InterruptedException 等待过程中线程被中断时抛出
   */
  default void readLockInterruptibly() throws InterruptedException {
    readLockInterruptibly(RwLockMode.GLOBAL);
  }

  /**
   * 可中断方式获取指定模式读锁，等待过程中可响应中断
   * @param lockMode 要获取的读锁模式
   * @throws InterruptedException 等待过程中线程被中断时抛出
   */
  void readLockInterruptibly(RwLockMode lockMode) throws InterruptedException;

  /**
   * 释放默认全局模式读锁，使用默认操作名
   */
  default void readUnlock() {
    readUnlock(RwLockMode.GLOBAL, "OTHER");
  }

  /**
   * 释放默认全局模式读锁，指定当前操作名称
   * @param opName 当前操作名称，用于日志和监控
   */
  default void readUnlock(String opName) {
    readUnlock(RwLockMode.GLOBAL, opName);
  }

  /**
   * 释放指定模式读锁，指定当前操作名称
   * @param lockMode 要释放的读锁模式
   * @param opName 当前操作名称，用于日志和监控
   */
  void readUnlock(RwLockMode lockMode, String opName);

  /**
   * 检查当前线程是否持有默认全局模式读锁
   * @return true如果当前线程持有读锁，否则返回false
   */
  default boolean hasReadLock() {
    return hasReadLock(RwLockMode.GLOBAL);
  }

  /**
   * 检查当前线程是否持有指定模式读锁
   * @param lockMode 要检查的读锁模式
   * @return true如果当前线程持有对应读锁，否则返回false
   */
  boolean hasReadLock(RwLockMode lockMode);

  /**
   * 获取默认全局模式写锁
   */
  default void writeLock() {
    writeLock(RwLockMode.GLOBAL);
  }

  /**
   * 根据指定锁模式获取写锁
   * @param lockMode 要获取的写锁模式
   */
  void writeLock(RwLockMode lockMode);
  
  /**
   * 可中断方式获取默认全局模式写锁，等待过程中可响应中断
   * @throws InterruptedException 等待过程中线程被中断时抛出
   */
  default void writeLockInterruptibly() throws InterruptedException {
    writeLockInterruptibly(RwLockMode.GLOBAL);
  }

  /**
   * 可中断方式获取指定模式写锁，等待过程中可响应中断
   * @param lockMode 要获取的写锁模式
   * @throws InterruptedException 等待过程中线程被中断时抛出
   */
  void writeLockInterruptibly(RwLockMode lockMode) throws InterruptedException;

  /**
   * 释放默认全局模式写锁，使用默认操作名
   */
  default void writeUnlock() {
    writeUnlock(RwLockMode.GLOBAL, "OTHER");
  }

  /**
   * 释放默认全局模式写锁，指定当前操作名称
   * @param opName 当前操作名称，用于日志和监控
   */
  default void writeUnlock(String opName) {
    writeUnlock(RwLockMode.GLOBAL, opName);
  }

  /**
   * 释放指定模式写锁，指定当前操作名称
   * @param lockMode 要释放的写锁模式
   * @param opName 当前操作名称，用于日志和监控
   */
  void writeUnlock(RwLockMode lockMode, String opName);

  /**
   * 检查当前线程是否持有默认全局模式写锁
   * @return true如果当前线程持有写锁，否则返回false
   */
  default boolean hasWriteLock() {
    return hasWriteLock(RwLockMode.GLOBAL);
  }

  /**
   * 检查当前线程是否持有指定模式写锁
   * @param lockMode 要检查的写锁模式
   * @return true如果当前线程持有对应写锁，否则返回false
   */
  boolean hasWriteLock(RwLockMode lockMode);
}