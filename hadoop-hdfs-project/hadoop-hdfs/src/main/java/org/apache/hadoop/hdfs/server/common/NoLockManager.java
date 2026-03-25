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

package org.apache.hadoop.hdfs.server.common;

import java.util.concurrent.locks.Lock;

/**
 * 空锁管理器实现，用于单元测试或不需要实际加锁的临时副本映射场景，
 * 实现了DataNodeLockManager接口，所有加锁解锁操作都是空实现，不进行实际锁操作。
 */
public class NoLockManager implements DataNodeLockManager<AutoCloseDataSetLock> {
  // 单例空锁实例，所有锁请求都返回该实例
  private final NoDataSetLock lock = new NoDataSetLock(null);

  /**
   * 空锁实现，所有加锁、关闭操作都是空操作，不执行实际加锁逻辑。
   */
  private static final class NoDataSetLock extends AutoCloseDataSetLock {

    private NoDataSetLock(Lock lock) {
      super(lock);
    }

    @Override
    public void lock() {
    }

    @Override
    public void close() {
    }
  }

  /**
   * 构造空锁管理器实例。
   */
  public NoLockManager() {
  }

  /**
   * 获取读锁，直接返回空锁单例，不进行实际加锁。
   * @param level 锁级别
   * @param resources 要加锁的资源
   * @return 空锁实例
   */
  @Override
  public AutoCloseDataSetLock readLock(LockLevel level, String... resources) {
    return lock;
  }

  /**
   * 获取写锁，直接返回空锁单例，不进行实际加锁。
   * @param level 锁级别
   * @param resources 要加锁的资源
   * @return 空锁实例
   */
  @Override
  public AutoCloseDataSetLock writeLock(LockLevel level, String... resources) {
    return lock;
  }

  /**
   * 添加锁，空实现不执行任何操作。
   * @param level 锁级别
   * @param resources 要加锁的资源
   */
  @Override
  public void addLock(LockLevel level, String... resources) {
  }

  /**
   * 移除锁，空实现不执行任何操作。
   * @param level 锁级别
   * @param resources 要解锁的资源
   */
  @Override
  public void removeLock(LockLevel level, String... resources) {
  }

  /**
   * 钩子方法，空实现不执行任何操作。
   */
  @Override
  public void hook() {
  }
}