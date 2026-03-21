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

package org.apache.hadoop.yarn.server.timeline;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timeline.*;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents.EventsOfOneEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager.CheckAcl;
import org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils;
import org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils.KeyBuilder;
import org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils.KeyParser;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.*;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.readReverseOrderedLong;
import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.writeReverseOrderedLong;
import static org.apache.hadoop.yarn.server.timeline.TimelineDataManager.DEFAULT_DOMAIN_ID;
import static org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils.prefixMatches;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

/**
 * <p>基于LevelDB实现的应用时间线存储服务，用于存储YARN应用历史数据。</p>
 *
 * <p>LevelDB分为三个存储区域：开始时间查询区、实体区、索引实体区。</p>
 *
 * <p>开始时间查询区用于查询指定实体的唯一开始时间，存储结构为：</p>
 * <pre>
 *   START_TIME_LOOKUP_PREFIX + 实体类型 + 实体ID</pre>
 *   值存储开始时间，用于快速定位实体。
 *
 * <p>实体区按键排序：实体类型 -> 降序排列的实体开始时间 -> 实体ID，包含五个子部分：
 * 事件、主过滤器、关联实体、其他信息、域ID、反向不可见关联实体。事件条目的值是序列化的事件信息，
 * 其他信息条目的值对应名称/值对中的值，其他条目的值为空。键结构如下：</p>
 * <pre>
 *   ENTITY_ENTRY_PREFIX + 实体类型 + 逆序开始时间 + 实体ID
 *
 *   ENTITY_ENTRY_PREFIX + 实体类型 + 逆序开始时间 + 实体ID +
 *     EVENTS_COLUMN + 逆序事件时间戳 + 事件类型
 *
 *   ENTITY_ENTRY_PREFIX + 实体类型 + 逆序开始时间 + 实体ID +
 *     PRIMARY_FILTERS_COLUMN + 过滤器名称 + 过滤器值
 *
 *   ENTITY_ENTRY_PREFIX + 实体类型 + 逆序开始时间 + 实体ID +
 *     OTHER_INFO_COLUMN + 信息名称
 *
 *   ENTITY_ENTRY_PREFIX + 实体类型 + 逆序开始时间 + 实体ID +
 *     RELATED_ENTITIES_COLUMN + 关联实体类型 + 关联实体ID
 *
 *   ENTITY_ENTRY_PREFIX + 实体类型 + 逆序开始时间 + 实体ID +
 *     DOMAIN_ID_COLUMN
 *
 *   ENTITY_ENTRY_PREFIX + 实体类型 + 逆序开始时间 + 实体ID +
 *     INVISIBLE_REVERSE_RELATED_ENTITIES_COLUMN + 关联实体类型 +
 *     关联实体ID</pre>
 *
 * <p>索引实体区以主过滤器名称和主过滤器值作为前缀，在对应前缀下存储完整实体条目，
 * 结构和实体区一致，格式为：</p>
 * <pre>
 *   INDEXED_ENTRY_PREFIX + 主过滤器名称 + 主过滤器值 +
 *     实体区键</pre>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class LeveldbTimelineStore extends AbstractService
    implements TimelineStore {
  private static final org.slf4j.Logger LOG = LoggerFactory
      .getLogger(LeveldbTimelineStore.class);

  @Private
  @VisibleForTesting
  static final String FILENAME = "leveldb-timeline-store.ldb";

  @VisibleForTesting
  //LevelDB恢复时备份文件的扩展名
  static final String BACKUP_EXT = ".backup-";

  private static final byte[] START_TIME_LOOKUP_PREFIX = "k".getBytes(StandardCharsets.UTF_8);
  private static final byte[] ENTITY_ENTRY_PREFIX = "e".getBytes(StandardCharsets.UTF_8);
  private static final byte[] INDEXED_ENTRY_PREFIX = "i".getBytes(StandardCharsets.UTF_8);

  private static final byte[] EVENTS_COLUMN = "e".getBytes(StandardCharsets.UTF_8);
  private static final byte[] PRIMARY_FILTERS_COLUMN = "f".getBytes(StandardCharsets.UTF_8);
  private static final byte[] OTHER_INFO_COLUMN = "i".getBytes(StandardCharsets.UTF_8);
  private static final byte[] RELATED_ENTITIES_COLUMN = "r".getBytes(StandardCharsets.UTF_8);
  private static final byte[] INVISIBLE_REVERSE_RELATED_ENTITIES_COLUMN =
      "z".getBytes(StandardCharsets.UTF_8);
  private static final byte[] DOMAIN_ID_COLUMN = "d".getBytes(StandardCharsets.UTF_8);

  private static final byte[] DOMAIN_ENTRY_PREFIX = "d".getBytes(StandardCharsets.UTF_8);
  private static final byte[] OWNER_LOOKUP_PREFIX = "o".getBytes(StandardCharsets.UTF_8);
  private static final byte[] DESCRIPTION_COLUMN = "d".getBytes(StandardCharsets.UTF_8);
  private static final byte[] OWNER_COLUMN = "o".getBytes(StandardCharsets.UTF_8);
  private static final byte[] READER_COLUMN = "r".getBytes(StandardCharsets.UTF_8);
  private static final byte[] WRITER_COLUMN = "w".getBytes(StandardCharsets.UTF_8);
  private static final byte[] TIMESTAMP_COLUMN = "t".getBytes(StandardCharsets.UTF_8);

  private static final byte[] EMPTY_BYTES = new byte[0];
  
  private static final String TIMELINE_STORE_VERSION_KEY = "timeline-store-version";
  
  private static final Version CURRENT_VERSION_INFO = Version
      .newInstance(1, 0);

  @Private
  @VisibleForTesting
  static final FsPermission LEVELDB_DIR_UMASK = FsPermission
      .createImmutable((short) 0700);

  // 开始时间写缓存
  private Map<EntityIdentifier, StartAndInsertTime> startTimeWriteCache;
  // 开始时间读缓存
  private Map<EntityIdentifier, Long> startTimeReadCache;

  /**
   * 写入时获取每个实体的独占锁，避免并发写入冲突.
   */
  private final LockMap<EntityIdentifier> writeLocks =
      new LockMap<EntityIdentifier>();

  // 删除操作读写锁，删除时加写锁，写入时加读锁
  private final ReentrantReadWriteLock deleteLock =
      new ReentrantReadWriteLock();

  // LevelDB实例
  private DB db;

  // 过期实体删除线程
  private Thread deletionThread;

  public LeveldbTimelineStore() {
    super(LeveldbTimelineStore.class.getName());
  }

  private JniDBFactory factory;

  @VisibleForTesting
  void setFactory(JniDBFactory fact) {
    this.factory = fact;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void serviceInit(Configuration conf) throws Exception {
    // 校验配置参数合法性
    Preconditions.checkArgument(conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_TTL_MS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_TTL_MS) > 0,
        "%s property value should be greater than zero",
        YarnConfiguration.TIMELINE_SERVICE_TTL_MS);
    Preconditions.checkArgument(conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS) > 0,
        "%s property value should be greater than zero",
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS);
    Preconditions.checkArgument(conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE) >= 0,
        "%s property value should be greater than or equal to zero",
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE);
    Preconditions.checkArgument(conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE) > 0,
        " %s property value should be greater than zero",
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE);
    Preconditions.checkArgument(conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE) > 0,
        "%s property value should be greater than zero",
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE);

    // 初始化LevelDB配置
    Options options = new Options();
    options.createIfMissing(true);
    options.cacheSize(conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE));
    if(factory == null) {
      factory = new JniDBFactory();
    }

    // 计算LevelDB存储路径
    Path dbPath = new Path(
        conf.get(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH), FILENAME);
    FileSystem localFS = null;
    try {
      // 创建本地存储目录并设置权限
      localFS = FileSystem.getLocal(conf);
      if (!localFS.exists(dbPath)) {
        if (!localFS.mkdirs(dbPath)) {
          throw new IOException("Couldn't create directory for leveldb " +
              "timeline store " + dbPath);
        }
        localFS.setPermission(dbPath, LEVELDB_DIR_UMASK);
      }
    } finally {
      IOUtils.cleanupWithLogger(LOG, localFS);
    }
    LOG.info("Using leveldb path " + dbPath);
    // 加载或修复LevelDB
    db = LeveldbUtils.loadOrRepairLevelDb(factory, dbPath, options);
    // 检查存储版本兼容性
    checkVersion();
    // 初始化开始时间读写缓存，使用LRU缓存淘汰策略
    startTimeWriteCache =
        Collections.synchronizedMap(new LRUMap(getStartTimeWriteCacheSize(
            conf)));
    startTimeReadCache =
        Collections.synchronizedMap(new LRUMap(getStartTimeReadCacheSize(
            conf)));

    // 如果开启TTL清理，启动过期实体删除线程
    if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE, true)) {
      deletionThread = new EntityDeletionThread(conf);
      deletionThread.start();
    }

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    // 中断并等待删除线程退出
    if (deletionThread != null) {
      deletionThread.interrupt();
      LOG.info("Waiting for deletion thread to complete its current action");
      try {
        deletionThread.join();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for deletion thread to complete," +
            " closing db now", e);
      }
    }
    // 关闭LevelDB
    IOUtils.cleanupWithLogger(LOG, db);
    super.serviceStop();
  }

  /**
   * 封装实体开始时间和插入时间.
   */
  private static class StartAndInsertTime {
    final long startTime;
    final long insertTime;

    public StartAndInsertTime(long startTime, long insertTime) {
      this.startTime = startTime;
      this.insertTime = insertTime;
    }
  }

  /**
   * 过期实体清理线程，定期清理超过TTL的实体数据.
   */
  private class EntityDeletionThread extends SubjectInheritingThread {
    private final long ttl;
    private final long ttlInterval;

    public EntityDeletionThread(Configuration conf) {
      ttl  = conf.getLong(YarnConfiguration.TIMELINE_SERVICE_TTL_MS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_TTL_MS);
      ttlInterval = conf.getLong(
          YarnConfiguration.TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS);
      LOG.info("Starting deletion thread with ttl " + ttl + " and cycle " +
          "interval " + ttlInterval);
    }

    @Override
    public void work() {
      while (true) {
        // 计算过期时间阈值
        long timestamp = System.currentTimeMillis() - ttl;
        try {
          // 删除所有早于该阈值的实体
          discardOldEntities(timestamp);
          // 休眠等待下一轮清理
          Thread.sleep(ttlInterval);
        } catch (IOException e) {
          LOG.error(e.toString());
        } catch (InterruptedException e) {
          LOG.info("Deletion thread received interrupt, exiting");
          break;
        }
      }
    }
  }

  /**
   * 带计数的锁映射表，管理每个实体的写入锁，引用计数为0时自动回收锁对象.
   */
  private static class LockMap<K> {
    /**
     * 带引用计数的可重入锁.
     */
    private static class CountingReentrantLock<K> extends ReentrantLock {
      private static final long serialVersionUID = 1L;
      private int count;
      private K key;

      CountingReentrantLock(K key) {
        super();
        this.count = 0;
        this.key = key;
      }
    }

    private Map<K, CountingReentrantLock<K>> locks =
        new HashMap<K, CountingReentrantLock<K>>();

    /**
     * 获取对应key的锁，不存在则创建，引用计数+1.
     */
    synchronized CountingReentrantLock<K> getLock(K key) {
      CountingReentrantLock<K> lock = locks.get(key);
      if (lock == null) {
        lock = new CountingReentrantLock<K>(key);
        locks.put(key, lock);
      }

      lock.count++;
      return lock;
    }

    /**
     * 归还锁，引用计数-1，引用计数为0则从映射表中移除.
     */
    synchronized void returnLock(CountingReentrantLock<K> lock) {
      if (lock.count == 0) {
        throw new IllegalStateException("Returned lock more times than it " +
            "was retrieved");
      }
      lock.count--;

      if (lock.count == 0) {
        locks.remove(lock.key);
      }
    }
  }


  @Override
  public TimelineEntity getEntity(String entityId, String entityType,
      EnumSet<Field> fields) throws IOException {
    // 获取实体开始时间
    Long revStartTime = getStartTimeLong(entityId, entityType);
    if (revStartTime == null) {
      return null;
    }
    // 构造实体前缀键
    byte[] prefix = KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX)
        .add(entityType).add(writeReverseOrderedLong(revStartTime))
        .add(entityId