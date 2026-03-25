// 这个文件已经全部加上中文注释
/*
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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager.DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager.DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS_DEFAULT;

/**
 * 已删除快照的后台垃圾回收器，定期清理标记为已删除的快照数据
 * 负责异步清理删除快照后残留的元数据，避免一次性删除阻塞NameNode主流程
 */
public class SnapshotDeletionGc {
  public static final Logger LOG = LoggerFactory.getLogger(
      SnapshotDeletionGc.class);

  private final FSNamesystem namesystem;
  private final long deletionOrderedGcPeriodMs;
  private final AtomicReference<Timer> timer = new AtomicReference<>();

  /**
   * 构造已删除快照垃圾回收器，初始化配置参数
   * @param namesystem 文件系统命名空间管理器
   * @param conf Hadoop配置对象
   */
  public SnapshotDeletionGc(FSNamesystem namesystem, Configuration conf) {
    this.namesystem = namesystem;

    // 从配置中读取垃圾回收周期，使用默认值兜底
    this.deletionOrderedGcPeriodMs = conf.getLong(
        DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS,
        DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS_DEFAULT);
    LOG.info("{} = {}", DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS,
        deletionOrderedGcPeriodMs);
  }

  /**
   * 启动定期垃圾回收任务，保证只启动一个定时器实例
   */
  public void schedule() {
    if (timer.get() != null) {
      return;
    }
    // 创建守护线程定时器，避免阻塞JVM退出
    final Timer t = new Timer(getClass().getSimpleName(), true);
    // CAS原子操作保证只启动一个定时器
    if (timer.compareAndSet(null, t)) {
      LOG.info("Schedule at fixed rate {}",
          StringUtils.formatTime(deletionOrderedGcPeriodMs));
      t.scheduleAtFixedRate(new GcTask(),
          deletionOrderedGcPeriodMs, deletionOrderedGcPeriodMs);
    }
  }

  /**
   * 取消并停止垃圾回收任务，释放定时器资源
   */
  public void cancel() {
    final Timer t = timer.getAndSet(null);
    if (t != null) {
      LOG.info("cancel");
      t.cancel();
    }
  }

  /**
   * 执行一次已删除快照的垃圾回收流程
   * @param name 当前回收任务标识，用于日志区分
   */
  private void gcDeletedSnapshot(String name) {
    final Snapshot.Root deleted;
    // 获取FS命名空间读锁，保证快照选择过程中元数据一致性
    namesystem.readLock(RwLockMode.FS);
    try {
      // 从快照管理器中获取一个待清理的已删除快照
      deleted = namesystem.getSnapshotManager().chooseDeletedSnapshot();
    } catch (Throwable e) {
      LOG.error("Failed to chooseDeletedSnapshot", e);
      throw e;
    } finally {
      // 释放读锁
      namesystem.readUnlock(RwLockMode.FS, "gcDeletedSnapshot");
    }
    // 没有待清理快照直接返回
    if (deleted == null) {
      LOG.trace("{}: no snapshots are marked as deleted.", name);
      return;
    }

    // 获取待清理快照的路径信息，用于日志和清理调用
    final String snapshotRoot = deleted.getRootFullPathName();
    final String snapshotName = deleted.getLocalName();
    LOG.info("{}: delete snapshot {} from {}",
        name, snapshotName, snapshotRoot);

    try {
      // 调用FSNamesystem执行实际的快照垃圾回收
      namesystem.gcDeletedSnapshot(snapshotRoot, snapshotName);
    } catch (Throwable e) {
      LOG.error("Failed to gcDeletedSnapshot " + deleted.getFullPathName(), e);
    }
  }

  /**
   * 定时器执行任务，封装单次垃圾回收调用
   */
  private class GcTask extends TimerTask {
    private final AtomicInteger count = new AtomicInteger();

    @Override
    public void run() {
      // 递增任务编号，用于日志区分不同执行轮次
      final int id = count.incrementAndGet();
      gcDeletedSnapshot(getClass().getSimpleName() + " #" + id);
    }
  }
}