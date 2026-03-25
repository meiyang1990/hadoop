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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

// 这个文件已经全部加上中文注释
// LevelDB实现的键值对时间线存储。将实体哈希映射存储到LevelDB实例中，使用两个键空间分区：
// 1) 实体ID到开始时间的映射(i!ENTITY_ID!ENTITY_TYPE → ENTITY_START_TIME)
// 2) 实际数据存储(e!START_TIME!ENTITY_ID!ENTITY_TYPE → ENTITY_BYTES)
// 主要用于缓存用途，无垃圾回收机制
/**
 * 基于LevelDB实现的时间线存储服务，主要用于时间线数据缓存。
 * 该实现将实体哈希表存储到LevelDB实例中，键空间分为两个分区：
 * 1. 实体ID到启动时间的映射分区：键格式为 i!ENTITY_ID!ENTITY_TYPE，值为ENTITY_START_TIME
 * 2. 实际实体数据存储分区：键格式为 e!START_TIME!ENTITY_ID!ENTITY_TYPE，值为序列化后的实体数据
 * 
 * 该存储不包含垃圾回收机制，设计目标主要是缓存场景使用。
 */
@Private
@Unstable
public class LevelDBCacheTimelineStore extends KeyValueBasedTimelineStore {
  private static final Logger LOG
      = LoggerFactory.getLogger(LevelDBCacheTimelineStore.class);
  private static final String CACHED_LDB_FILE_PREFIX = "-timeline-cache.ldb";
  private String dbId;
  private DB entityDb;
  private Configuration configuration;
  private static final AtomicInteger DB_COUNTER = new AtomicInteger(0);
  private static final String CACHED_LDB_FILENAME = "db";

  /**
   * 构造函数，指定缓存ID和服务名称。
   * @param id 数据库ID
   * @param name 服务名称
   */
  public LevelDBCacheTimelineStore(String id, String name) {
    super(name);
    dbId = id;
    entityInsertTimes = new MemoryTimelineStore.HashMapStoreAdapter<>();
    domainById = new MemoryTimelineStore.HashMapStoreAdapter<>();
    domainsByOwner = new MemoryTimelineStore.HashMapStoreAdapter<>();
  }

  /**
   * 构造函数，指定缓存ID，使用默认服务名称。
   * @param id 数据库ID
   */
  public LevelDBCacheTimelineStore(String id) {
    this(id, LevelDBCacheTimelineStore.class.getName());
  }

  /**
   * 默认构造函数，自动生成唯一数据库ID。
   */
  public LevelDBCacheTimelineStore() {
    this(CACHED_LDB_FILENAME + String.valueOf(DB_COUNTER.getAndIncrement()),
        LevelDBCacheTimelineStore.class.getName());
  }

  @Override
  protected synchronized void serviceInit(Configuration conf) throws Exception {
    configuration = conf;
    // 创建LevelDB配置选项
    Options options = new Options();
    // 数据库不存在时自动创建
    options.createIfMissing(true);
    // 从配置读取读缓存大小，使用默认值如果未配置
    options.cacheSize(conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_CACHE_READ_CACHE_SIZE,
        YarnConfiguration.
            DEFAULT_TIMELINE_SERVICE_LEVELDB_CACHE_READ_CACHE_SIZE));
    // 创建LevelDB JNI工厂实例
    JniDBFactory factory = new JniDBFactory();
    // 拼接LevelDB存储路径
    Path dbPath = new Path(
        conf.get(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH),
        dbId + CACHED_LDB_FILE_PREFIX);
    FileSystem localFS = null;

    try {
      // 获取本地文件系统实例
      localFS = FileSystem.getLocal(conf);
      // 如果存储目录不存在则创建
      if (!localFS.exists(dbPath)) {
        if (!localFS.mkdirs(dbPath)) {
          throw new IOException("Couldn't create directory for leveldb " +
              "timeline store " + dbPath);
        }
        // 设置正确的目录权限
        localFS.setPermission(dbPath, LeveldbUtils.LEVELDB_DIR_UMASK);
      }
    } finally {
      // 关闭本地文件系统资源
      IOUtils.cleanupWithLogger(LOG, localFS);
    }
    LOG.info("Using leveldb path " + dbPath);
    // 打开LevelDB数据库实例
    entityDb = factory.open(new File(dbPath.toString()), options);
    // 将实体存储适配为LevelDB实现
    entities = new LevelDBMapAdapter<>(entityDb);

    super.serviceInit(conf);
  }

  @Override
  protected synchronized void serviceStop() throws Exception {
    // 关闭LevelDB数据库
    IOUtils.cleanupWithLogger(LOG, entityDb);
    // 获取数据库存储路径
    Path dbPath = new Path(
        configuration.get(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH),
        dbId + CACHED_LDB_FILE_PREFIX);
    FileSystem localFS = null;
    try {
      // 获取本地文件系统实例
      localFS = FileSystem.getLocal(configuration);
      // 删除整个LevelDB存储目录，因为缓存不需要持久化
      if (!localFS.delete(dbPath, true)) {
          throw new IOException("Couldn't delete data file for leveldb " +
              "timeline store " + dbPath);
      }
    } finally {
      // 关闭本地文件系统资源
      IOUtils.cleanupWithLogger(LOG, localFS);
    }
    super.serviceStop();
  }

  /**
   * 基于LevelDB实现的时间线存储映射适配器，将实体ID映射到时间线实体。
   * 实现了双层键结构，通过索引存储加速实体查找。
   * @param <K> 实体标识符类型
   * @param <V> 时间线实体类型
   */
  static class LevelDBMapAdapter<K extends EntityIdentifier,
      V extends TimelineEntity> implements TimelineStoreMapAdapter<K, V> {
    private static final String TIME_INDEX_PREFIX = "i";
    private static final String ENTITY_STORAGE_PREFIX = "e";
    DB entityDb;

    /**
     * 构造函数，绑定LevelDB实例。
     * @param currLevelDb LevelDB实例
     */
    public LevelDBMapAdapter(DB currLevelDb) {
      entityDb = currLevelDb;
    }

    @Override
    public V get(K entityId) {
      V result = null;
      // 从索引中读取实体启动时间
      byte[] startTimeBytes = entityDb.get(getStartTimeKey(entityId));
      if (startTimeBytes == null) {
        return null;
      }

      // 构造实体存储键并读取反序列化实体
      try {
        result = getEntityForKey(getEntityKey(entityId, startTimeBytes));
      } catch (IOException e) {
        LOG.error("GenericObjectMapper cannot read key from key "
            + entityId.toString()
            + " into an object. Read aborted! ");
        LOG.error(e.getMessage());
      }

      return result;
    }

    @Override
    public void put(K entityId, V entity) {
      Long startTime = entity.getStartTime();
      if (startTime == null) {
        startTime = System.currentTimeMillis();
      }
      // 将启动时间序列化为反向有序字节数组，保证按时间倒序排列
      byte[] startTimeBytes = GenericObjectMapper.writeReverseOrderedLong(
          startTime);
      try {
        // 序列化实体对象
        byte[] valueBytes = GenericObjectMapper.write(entity);
        // 写入实体数据到LevelDB
        entityDb.put(getEntityKey(entityId, startTimeBytes), valueBytes);
      } catch (IOException e) {
        LOG.error("GenericObjectMapper cannot write "
            + entity.getClass().getName()
            + " into a byte array. Write aborted! ");
        LOG.error(e.getMessage());
      }

      // 写入启动时间索引，用于构造实体键
      entityDb.put(getStartTimeKey(entityId), startTimeBytes);
    }

    @Override
    public void remove(K entityId) {
      // 从索引中读取启动时间，然后删除索引记录
      LeveldbUtils.KeyBuilder startTimeKeyBuilder
          = LeveldbUtils.KeyBuilder.newInstance();
      startTimeKeyBuilder.add(TIME_INDEX_PREFIX).add(entityId.getId())
          .add(entityId.getType());
      byte[] startTimeBytes = entityDb.get(startTimeKeyBuilder.getBytes());
      if (startTimeBytes == null) {
        return;
      }
      // 删除索引记录
      entityDb.delete(startTimeKeyBuilder.getBytes());

      // 删除实际的实体数据记录
      entityDb.delete(getEntityKey(entityId, startTimeBytes));
    }

    @Override
    public CloseableIterator<V> valueSetIterator() {
      return getIterator(null, Long.MAX_VALUE);
    }

    @Override
    public CloseableIterator<V> valueSetIterator(V minV) {
      return getIterator(
          new EntityIdentifier(minV.getEntityId(), minV.getEntityType()),
          minV.getStartTime());
    }

    /**
     * 创建符合条件的实体迭代器，按启动时间倒序遍历实体。
     * @param startId 起始实体ID，null表示从头开始
     * @param startTimeMax 最大启动时间，只返回不晚于该时间的实体
     * @return 闭合迭代器，遍历所有符合条件的实体
     */
    private CloseableIterator<V> getIterator(
        EntityIdentifier startId, long startTimeMax) {

      final DBIterator internalDbIterator = entityDb.iterator();

      // 构造实体存储前缀，用于过滤非实体数据键
      LeveldbUtils.KeyBuilder entityPrefixKeyBuilder
          = LeveldbUtils.KeyBuilder.newInstance();
      entityPrefixKeyBuilder.add(ENTITY_STORAGE_PREFIX);
      final byte[] prefixBytes = entityPrefixKeyBuilder.getBytesForLookup();
      // 构造反向有序的启动时间字节，实现倒序遍历
      final byte[] startTimeBytes
          = GenericObjectMapper.writeReverseOrderedLong(startTimeMax);
      entityPrefixKeyBuilder.add(startTimeBytes, true);
      // 如果指定了起始实体，添加实体ID到起始键
      if (startId != null) {
        entityPrefixKeyBuilder.add(startId.getId());
      }
      final byte[] startPrefixBytes
          = entityPrefixKeyBuilder.getBytesForLookup();
      // 定位到起始键位置
      internalDbIterator.seek(startPrefixBytes);

      return new CloseableIterator<V>() {
        @Override
        public boolean hasNext() {
          // 检查是否还有下一个元素
          if (!internalDbIterator.hasNext()) {
            return false;
          }
          // 检查下一个元素的前缀是否还是实体存储前缀
          Map.Entry<byte[], byte[]> nextEntry = internalDbIterator.peekNext();
          if (LeveldbUtils.prefixMatches(
              prefixBytes, prefixBytes.length, nextEntry.getKey())) {
            return true;
          }
          // 前缀不匹配说明已经遍历完所有实体，结束遍历
          return false;
        }

        @Override
        public V next() {
          if (hasNext()) {
            Map.Entry<byte[], byte[]> nextRaw = internalDbIterator.next();
            try {
              // 反序列化获取实体对象
              V result = getEntityForKey(nextRaw.getKey());
              return result;
            } catch (IOException e) {
              LOG.error("GenericObjectMapper cannot read key from key "
                  + nextRaw.getKey()
                  + " into an object. Read aborted! ");
              LOG.error(e.getMessage());
            }
          }
          return null;
        }

        // 不支持迭代过程中删除元素
        @Override
        public void remove() {
          LOG.error("LevelDB map adapter does not support iterate-and-remove"
              + " use cases. ");
        }

        @Override
        public void close() throws IOException {
          // 关闭底层迭代器
          internalDbIterator.close();
        }
      };
    }
    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @SuppressWarnings("unchecked")
    private V getEntityForKey(byte[] key) throws IOException {
      // 从LevelDB读取实体字节数据
      byte[] resultRaw = entityDb.get(key);
      if (resultRaw == null) {
        return null;
      }
      // JSON反序列化为TimelineEntity对象
      return (V) OBJECT_MAPPER.readValue(resultRaw, TimelineEntity.class);
    }

    /**
     * 构造启动时间索引的LevelDB键。
     * @param entityId 实体标识符
     * @return 启动时间索引键字节数组
     */
    private byte[] getStartTimeKey(K entityId) {
      LeveldbUtils.KeyBuilder startTimeKeyBuilder
          = LeveldbUtils.KeyBuilder.newInstance();
      startTimeKeyBuilder.add(TIME_INDEX_PREFIX).add(entityId.getId())
          .add(entityId.getType());
      return startTimeKeyBuilder.getBytes();
    }

    /**
     * 构造实体数据的LevelDB键。
     * @param entityId 实体标识符
     * @param startTimeBytes 序列化后的启动时间字节
     * @return 实体数据键字节数组
     */
    private byte[] getEntityKey(K entityId, byte[] startTimeBytes) {
      LeveldbUtils.KeyBuilder entityKeyBuilder
          = LeveldbUtils.KeyBuilder.newInstance();
      entityKeyBuilder.add(ENTITY_STORAGE_PREFIX).add(startTimeBytes, true)
          .add(entityId.getId()).add(entityId.getType());
      return entityKeyBuilder.getBytes();
    }
  }
}