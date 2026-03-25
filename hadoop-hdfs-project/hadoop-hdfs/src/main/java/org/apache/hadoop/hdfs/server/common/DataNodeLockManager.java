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

/**
 * 文件所属模块：HDFS服务端核心模块
 * 核心职责：定义DataNode层级锁管理器的通用接口，规范DataNode多粒度锁的获取、添加、移除管理，保证DataNode上数据块操作的并发安全性
 */
public interface DataNodeLockManager<T extends AutoCloseDataSetLock> {

  /**
   * 锁层级枚举，定义了锁的粒度和加锁顺序，用于避免死锁，保证加锁顺序一致
   * 支持三种加锁顺序模式：
   * 1. 仅块池级加锁
   * 2. 块池 -> 卷级加锁
   * 3. 块池 -> 卷 -> 目录级加锁
   */
  enum LockLevel {
    BLOCK_POOl,
    VOLUME,
    DIR
  }

  /**
   * 获取指定层级的读锁，按层级顺序加锁
   * @param level 目标锁层级
   * @param resources 需要加锁的资源名称列表
   * @return 已加锁的数据集锁对象，可用于自动释放锁
   */
  T readLock(LockLevel level, String... resources);

  /**
   * 获取指定层级的写锁，按层级顺序加锁
   * @param level 目标锁层级
   * @param resources 需要加锁的资源名称列表
   * @return 已加锁的数据集锁对象，可用于自动释放锁
   */
  T writeLock(LockLevel level, String... resources);

  /**
   * 向锁管理器添加指定层级的锁
   * @param level 锁层级
   * @param resources 对应资源名称列表
   */
  void addLock(LockLevel level, String... resources);

  /**
   * 从锁管理器移除指定层级的锁
   * @param level 锁层级
   * @param resources 对应资源名称列表
   */
  void removeLock(LockLevel level, String... resources);

  /**
   * 锁管理器的钩子方法，用于锁管理器扩展后置处理逻辑
   */
  void hook();
}