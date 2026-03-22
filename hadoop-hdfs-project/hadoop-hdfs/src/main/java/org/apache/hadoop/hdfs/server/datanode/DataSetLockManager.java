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

package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.AutoCloseDataSetLock;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager;

import java.util.HashMap;
import java.util.Stack;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：DataNode 数据块存储数据集的锁管理器，负责分层管理数据集各级读写锁，
 * 支持按块池、卷、目录层级加锁，提供锁泄漏检测和追踪能力，保障数据并发访问的线程安全。
 * 
 * Class for maintain a set of lock for fsDataSetImpl.
 */
/**
 * DataNode数据集锁管理器，实现DataNodeLockManager接口，管理分层读写锁，支持锁追踪和泄漏检测
 */
public class DataSetLockManager implements DataNodeLockManager<AutoCloseDataSetLock> {
  public static final Logger LOG = LoggerFactory.getLogger(DataSetLockManager.class);
  // 线程锁追踪信息映射表，key为线程标识，value为线程的锁持有记录
  private final HashMap<String, TrackLog> threadCountMap = new HashMap<>();
  // 读写锁存储容器
  private final LockMap lockMap = new LockMap();
  // ReentrantReadWriteLock是否启用公平锁模式
  private boolean isFair = true;
  // 是否开启锁追踪功能
  private final boolean openLockTrace;
  // 最后一次检测到锁泄漏时保存的异常信息
  private Exception lastException;
  // 所属DataNode实例，用于指标统计
  private DataNode datanode;

  /**
   * 线程安全的读写锁存储容器，统一管理所有已注册的读锁和写锁
   * Class for maintain lockMap and is thread safe.
   */
  private class LockMap {
    // 读锁存储映射表，key为锁名称，value为对应的可关闭读锁实例
    private final HashMap<String, AutoCloseDataSetLock> readlockMap = new HashMap<>();
    // 写锁存储映射表，key为锁名称，value为对应的可关闭写锁实例
    private final HashMap<String, AutoCloseDataSetLock> writeLockMap = new HashMap<>();

    /**
     * 向锁容器添加指定名称的读写锁对
     * @param name 锁名称
     * @param lock 底层可重入读写锁实例
     */
    public synchronized void addLock(String name, ReentrantReadWriteLock lock) {
      AutoCloseDataSetLock readLock = new AutoCloseDataSetLock(lock.readLock());
      AutoCloseDataSetLock writeLock = new AutoCloseDataSetLock(lock.writeLock());
      if (openLockTrace) {
        readLock.setDataNodeLockManager(DataSetLockManager.this);
        writeLock.setDataNodeLockManager(DataSetLockManager.this);
      }
      readlockMap.putIfAbsent(name, readLock);
      writeLockMap.putIfAbsent(name, writeLock);
    }

    /**
     * 从锁容器移除指定名称的读写锁对
     * @param name 要移除的锁名称
     */
    public synchronized void removeLock(String name) {
      if (!readlockMap.containsKey(name) || !writeLockMap.containsKey(name)) {
        LOG.error("The lock " + name + " is not in LockMap");
      }
      readlockMap.remove(name);
      writeLockMap.remove(name);
    }

    /**
     * 根据锁名称获取读锁实例
     * @param name 锁名称
     * @return 对应的读锁实例，不存在则返回null
     */
    public synchronized AutoCloseDataSetLock getReadLock(String name) {
      return readlockMap.get(name);
    }

    /**
     * 根据锁名称获取写锁实例
     * @param name 锁名称
     * @return 对应的写锁实例，不存在则返回null
     */
    public synchronized AutoCloseDataSetLock getWriteLock(String name) {
      return writeLockMap.get(name);
    }
  }

  /**
   * 根据锁层级和资源名称生成唯一的锁名称字符串，校验参数合法性
   * Generate lock order string concatenates with lock name.
   * @param level which level lock want to acquire.
   * @param resources lock name by lock order.
   * @return lock order string concatenates with lock name.
   */
  private String generateLockName(LockLevel level, String... resources) {
    if (resources.length == 1 && level == LockLevel.BLOCK_POOl) {
      if (resources[0] == null) {
        throw new IllegalArgumentException("acquire a null block pool lock");
      }
      return resources[0];
    } else if (resources.length == 2 && level == LockLevel.VOLUME) {
      if (resources[0] == null || resources[1] == null) {
        throw new IllegalArgumentException("acquire a null bp lock : "
            + resources[0] + "volume lock :" + resources[1]);
      }
      return resources[0] + resources[1];
    } else if (resources.length == 3 && level == LockLevel.DIR) {
      if (resources[0] == null || resources[1] == null || resources[2] == null) {
        throw new IllegalArgumentException("acquire a null dataset lock : "
            + resources[0] + ",volume lock :" + resources[1]
        + ",subdir lock :" + resources[2]);
      }
      return resources[0] + resources[1] + resources[2];
    } else {
      throw new IllegalArgumentException("lock level do not match resource");
    }
  }

  /**
   * 线程锁持有信息追踪类，记录线程获取锁的调用栈和锁计数，用于锁泄漏检测
   * Class for record thread acquire lock stack trace and count.
   */
  private static class TrackLog {
    // 锁获取调用栈异常栈，用于定位锁获取位置
    private final Stack<Exception> logStack = new Stack<>();
    // 当前线程持有的锁总数
    private int lockCount = 0;
    // 被追踪的线程名称
    private final String threadName;

    TrackLog(String threadName) {
      this.threadName = threadName;
      incrLockCount();
    }

    /**
     * 增加锁计数，压入当前获取锁的调用栈
     */
    public void incrLockCount() {
      logStack.push(new Exception("lock stack trace"));
      lockCount += 1;
    }

    /**
     * 减少锁计数，弹出已释放锁的调用栈
     */
    public void decrLockCount() {
      logStack.pop();
      lockCount -= 1;
    }

    /**
     * 打印当前线程持有的锁信息和调用栈，用于锁泄漏排查
     */
    public void showLockMessage() {
      LOG.error("hold lock thread name is:" + threadName +
          " hold count is:" + lockCount);
      while (!logStack.isEmpty()) {
        Exception e = logStack.pop();
        LOG.error("lock stack ", e);
      }
    }

    /**
     * 判断当前线程锁计数为1，释放后是否可以清理该追踪记录
     * @return true 表示可以清理，false表示还有锁持有
     */
    public boolean shouldClear() {
      return lockCount == 1;
    }
  }

  /**
   * 无参构造函数，默认开启锁追踪功能
   */
  public DataSetLockManager() {
    this.openLockTrace = true;
  }

  /**
   * 带配置的构造函数，从配置中读取公平锁和锁追踪配置，绑定所属DataNode
   * @param conf Hadoop配置对象
   * @param dn 所属DataNode实例
   */
  public DataSetLockManager(Configuration conf, DataNode dn) {
    this.isFair = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_LOCK_FAIR_KEY,
        DFSConfigKeys.DFS_DATANODE_LOCK_FAIR_DEFAULT);
    this.openLockTrace = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_LOCKMANAGER_TRACE,
        DFSConfigKeys.DFS_DATANODE_LOCKMANAGER_TRACE_DEFAULT);
    this.datanode = dn;
  }

  @Override
  /**
   * 获取指定层级的读锁，按照层级从高到低顺序加锁，建立父锁关联
   * @param level 锁层级（块池/卷/目录）
   * @param resources 对应层级的资源名称数组
   * @return 已加锁的可关闭读锁，解锁时自动关闭父锁
   */
  public AutoCloseDataSetLock readLock(LockLevel level, String... resources) {
    if (level == LockLevel.BLOCK_POOl) {
      return getReadLock(level, resources[0]);
    } else if (level == LockLevel.VOLUME){
      AutoCloseDataSetLock bpLock = getReadLock(LockLevel.BLOCK_POOl, resources[0]);
      AutoCloseDataSetLock volLock = getReadLock(level, resources);
      volLock.setParentLock(bpLock);
      if (openLockTrace) {
        LOG.info("Sub lock " + resources[0] + resources[1] + " parent lock " +
            resources[0]);
      }
      return volLock;
    } else {
      AutoCloseDataSetLock bpLock = getReadLock(LockLevel.BLOCK_POOl, resources[0]);
      AutoCloseDataSetLock volLock = getReadLock(LockLevel.VOLUME, resources[0], resources[1]);
      volLock.setParentLock(bpLock);
      AutoCloseDataSetLock dirLock = getReadLock(level, resources);
      dirLock.setParentLock(volLock);
      if (openLockTrace) {
        LOG.debug("Sub lock " + resources[0] + resources[1] + resources[2] + " parent lock " +
            resources[0] + resources[1]);
      }
      return dirLock;
    }
  }

  @Override
  /**
   * 获取指定层级的写锁，按照层级从高到低顺序加锁，上层加读锁，当前层加写锁，建立父锁关联
   * @param level 锁层级（块池/卷/目录）
   * @param resources 对应层级的资源名称数组
   * @return 已加锁的可关闭写锁，解锁时自动关闭父锁
   */
  public AutoCloseDataSetLock writeLock(LockLevel level, String... resources) {
    if (level == LockLevel.BLOCK_POOl) {
      return getWriteLock(level, resources[0]);
    } else if (level == LockLevel.VOLUME) {
      AutoCloseDataSetLock bpLock = getReadLock(LockLevel.BLOCK_POOl, resources[0]);
      AutoCloseDataSetLock volLock = getWriteLock(level, resources);
      volLock.setParentLock(bpLock);
      if (openLockTrace) {
        LOG.info("Sub lock " + resources[0] + resources[1] + " parent lock " +
            resources[0]);
      }
      return volLock;
    } else {
      AutoCloseDataSetLock bpLock = getReadLock(LockLevel.BLOCK_POOl, resources[0]);
      AutoCloseDataSetLock volLock = getReadLock(LockLevel.VOLUME, resources[0], resources[1]);
      volLock.setParentLock(bpLock);
      AutoCloseDataSetLock dirLock = getWriteLock(level, resources);
      dirLock.setParentLock(volLock);
      if (openLockTrace) {
        LOG.debug("Sub lock " + resources[0] + resources[1] + resources[2] + " parent lock " +
            resources[0] + resources[1]);
      }
      return dirLock;
    }
  }

  /**
   * 内部方法，获取并加锁指定层级的读锁，不存在则自动创建，统计加锁耗时
   * Return a not null ReadLock.
   */
  private AutoCloseDataSetLock getReadLock(LockLevel level, String... resources) {
    long startTimeNanos = Time.monotonicNowNanos();
    String lockName = generateLockName(level, resources);
    AutoCloseDataSetLock lock = lockMap.getReadLock(lockName);
    if (lock == null) {
      LOG.warn("Ignore this error during dn restart: Not existing readLock "
          + lockName);
      lockMap.addLock(lockName, new ReentrantReadWriteLock(isFair));
      lock = lockMap.getReadLock(lockName);
    }
    lock.lock();
    if (openLockTrace) {
      putThreadName(getThreadName());
    }
    if (datanode != null) {
      datanode.metrics.addAcquireDataSetReadLock(Time.monotonicNowNanos() - startTimeNanos);
    }
    return lock;
  }

  /**
   * 内部方法，获取并加锁指定层级的写锁，不存在则自动创建，统计加锁耗时
   * Return a not null WriteLock.
   */
  private AutoCloseDataSetLock getWriteLock(LockLevel level, String... resources) {
    long startTimeNanos = Time.monotonicNowNanos();
    String lockName = generateLockName(level, resources);
    AutoCloseDataSetLock lock = lockMap.getWriteLock(lockName);
    if (lock == null) {
      LOG.warn("Ignore this error during dn restart: Not existing writeLock"
          + lockName);
      lockMap.addLock(lockName, new ReentrantReadWriteLock(isFair));
      lock = lockMap.getWriteLock(lockName);
    }
    lock.lock();
    if (openLockTrace) {
      putThreadName(getThreadName());
    }
    if (datanode != null) {
      datanode.metrics.addAcquireDataSetWriteLock(Time.monotonicNowNanos() - startTimeNanos);
    }
    return lock;
  }

  @Override
  /**
   * 提前添加指定层级的所有锁，包括上级层级的锁，用于DataNode启动时预初始化锁
   * @param level 锁层级（块池/卷/目录）
   * @param resources 对应层级的资源名称数组
   */
  public void addLock(LockLevel level, String... resources) {
    String lockName = generateLockName(level, resources);
    if (level == LockLevel.BLOCK_POOl) {
      lockMap.addLock(lockName, new ReentrantReadWriteLock(isFair));
    } else if (level == LockLevel.VOLUME) {
      lockMap.addLock(resources[0], new ReentrantReadWriteLock(isFair));
      lockMap.addLock(lockName, new ReentrantReadWriteLock(isFair));
    } else {
      lockMap.addLock(resources[0], new ReentrantReadWriteLock(isFair));
      lockMap.addLock(generateLockName(LockLevel.VOLUME, resources[0], resources[1]),
          new ReentrantReadWriteLock(isFair));
      lockMap.addLock(lockName, new ReentrantReadWriteLock(isFair));
    }
  }

  @Override
  /**
   * 移除指定层级的锁，加写锁后再移除保证线程安全
   * @param level 锁层级（块池/卷/目录）
   * @param resources 对应层级的资源名称数组
   */
  public void removeLock(LockLevel level, String... resources) {
    String lockName = generateLockName(level, resources);
    try (AutoCloseDataSetLock lock = writeLock(level, resources)) {
      lockMap.removeLock(lockName);
    }
  }

  @Override
  /**
   * 锁释放钩子，锁释放后调用，更新线程追踪记录
   */
  public void hook() {
    if (openLockTrace) {
      removeThreadName(getThreadName());
    }
  }

  /**
   * 线程获取锁后，更新线程追踪记录，增加锁计数
   * Add thread name when lock a lock.
   */
  private synchronized void putThreadName(String thread) {
    if (threadCountMap.containsKey(thread)) {
      TrackLog trackLog = threadCountMap.get(thread);
      trackLog.incrLockCount();
    }
    threadCountMap.putIfAbsent(thread, new TrackLog(thread));
  }

  /**
   * 检测是否存在未释放的锁泄漏，输出所有持有锁的线程和调用栈信息
   */
  public synchronized void lockLeakCheck() {
    if (!openLockTrace) {
      LOG.warn("not open lock leak check func");
      return;
    }
    if (threadCountMap.isEmpty()) {