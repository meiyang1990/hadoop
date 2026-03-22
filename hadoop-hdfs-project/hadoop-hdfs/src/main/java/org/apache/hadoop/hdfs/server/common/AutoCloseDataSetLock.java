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

import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.StringUtils;

import java.util.concurrent.locks.Lock;

import static org.apache.hadoop.hdfs.server.datanode.DataSetLockManager.LOG;

/**
 * HDFS DataNode数据集锁的可自动关闭实现，支持try-with-resources语法，支持嵌套锁结构与锁管理器钩子回调。
 * 继承AutoCloseableLock，用于管理DataNode数据集操作的锁生命周期，自动释放锁避免死锁。
 */
public class AutoCloseDataSetLock extends AutoCloseableLock {
  // 持有的底层JUC锁对象
  private Lock lock;
  // 父级锁，用于嵌套锁场景，当前锁释放后自动释放父锁
  private AutoCloseDataSetLock parentLock;
  // 所属的DataNode锁管理器，用于锁释放后调用钩子
  private DataNodeLockManager<AutoCloseDataSetLock> dataNodeLockManager;

  /**
   * 构造方法，基于给定的底层JUC锁创建数据集锁对象。
   * @param lock 底层JUC锁实例
   */
  public AutoCloseDataSetLock(Lock lock) {
    this.lock = lock;
  }

  /**
   * 关闭锁，自动释放持有的锁并处理父级锁。
   * 实现AutoCloseable接口，在try-with-resources结束时自动调用。
   */
  @Override
  public void close() {
    if (lock != null) {
      // 释放底层锁
      lock.unlock();
      // 调用锁管理器钩子，完成锁释放后的后置处理
      if (dataNodeLockManager != null) {
        dataNodeLockManager.hook();
      }
    } else {
      // 锁对象为空，记录错误日志
      LOG.error("Try to unlock null lock" +
          StringUtils.getStackTrace(Thread.currentThread()));
    }
    // 如果存在父级锁，自动释放父锁
    if (parentLock != null) {
      parentLock.close();
    }
  }

  /**
   * 实际获取锁的方法。
   */
  public void lock() {
    if (lock != null) {
      // 获取底层锁
      lock.lock();
      return;
    }
    // 锁对象为空，记录错误日志
    LOG.error("Try to lock null lock" +
        StringUtils.getStackTrace(Thread.currentThread()));
  }

  /**
   * 设置父级锁，仅当父锁未设置时生效，支持嵌套锁场景。
   * @param parent 父级锁实例
   */
  public void setParentLock(AutoCloseDataSetLock parent) {
    if (parentLock == null) {
      this.parentLock = parent;
    }
  }

  /**
   * 设置所属的DataNode锁管理器，用于锁释放后触发钩子回调。
   * @param dataNodeLockManager DataNode锁管理器实例
   */
  public void setDataNodeLockManager(DataNodeLockManager<AutoCloseDataSetLock>
      dataNodeLockManager) {
    this.dataNodeLockManager = dataNodeLockManager;
  }
}