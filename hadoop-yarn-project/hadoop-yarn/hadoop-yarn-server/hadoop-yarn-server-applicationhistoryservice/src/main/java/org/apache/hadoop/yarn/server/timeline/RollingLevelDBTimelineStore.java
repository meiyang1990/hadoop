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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents.EventsOfOneEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.timeline.RollingLevelDB.RollingWriteBatch;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager.CheckAcl;
import org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils;
import org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils.KeyBuilder;
import org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils.KeyParser;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTClazzNameRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.readReverseOrderedLong;
import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.writeReverseOrderedLong;
import static org.apache.hadoop.yarn.server.timeline.TimelineDataManager.DEFAULT_DOMAIN_ID;
import static org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils.prefixMatches;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BATCH_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_TTL_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_WRITE_BATCH_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_TTL_MS;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

/**
 * <p>
 * 基于LevelDB实现的应用时间线存储，支持按时间滚动删除旧数据
 * </p>
 *
 * <p>
 * LevelDB分为三个存储区域：起始时间区域、实体区域、索引实体区域
 * </p>
 *
 * <p>
 * 起始时间区域用于存储每个实体的唯一启动时间，键格式如下：
 * </p>
 *
 * <pre>
 *   START_TIME_LOOKUP_PREFIX + entity type + entity id
 * </pre>
 *
 * <p>
 * 实体区域按键排序：实体类型 -> 逆序启动时间 -> 实体ID。包含四个子区域：
 * 事件、主过滤器、关联实体、其他信息。事件条目将事件信息序列化存储；
 * 其他信息条目值对应具体数据（名称存在键中）；其他条目值为空。键结构如下：
 * </p>
 *
 * <pre>
 *   ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id
 *
 *   ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
 *     DOMAIN_ID_COLUMN
 *
 *   ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
 *     EVENTS_COLUMN + reveventtimestamp + eventtype
 *
 *   ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
 *     PRIMARY_FILTERS_COLUMN + name + value
 *
 *   ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
 *     OTHER_INFO_COLUMN + name
 *
 *   ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
 *     RELATED_ENTITIES_COLUMN + relatedentity type + relatedentity id
 * </pre>
 *
 * <p>
 * 索引实体区域以主过滤器名称和值作为前缀，前缀内存储完整实体条目格式同上述实体区域：
 * </p>
 *
 * <pre>
 *   INDEXED_ENTRY_PREFIX + primaryfilter name + primaryfilter value +
 *     key
 * </pre>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RollingLevelDBTimelineStore extends AbstractService implements
    TimelineStore {
  private static final Logger LOG = LoggerFactory
      .getLogger(RollingLevelDBTimelineStore.class);
  // 当前版本FST序列化配置
  private static FSTConfiguration fstConf =
      FSTConfiguration.createDefaultConfiguration();
  // 2.24版本兼容序列化配置，当新版本反序列化失败时降级使用
  private static FSTConfiguration fstConf224 =
      FSTConfiguration.createDefaultConfiguration();
  // 2.24版本中LinkedHashMap的静态类编号
  private static final int LINKED_HASH_MAP_224_CODE = 83;

  static {
    fstConf.setShareReferences(false);
    fstConf224.setShareReferences(false);
    // YARN-6654: FST 2.24和2.50对LinkedHashMap的编码不同，注册旧版本编号保证兼容性
    FSTClazzNameRegistry registry = fstConf224.getClassRegistry();
    registry.registerClass(
        LinkedHashMap.class, LINKED_HASH_MAP_224_CODE, fstConf224);
  }

  @Private
  @VisibleForTesting
  static final String FILENAME = "leveldb-timeline-store";
  static final String DOMAIN = "domain-ldb";
  static final String ENTITY = "entity-ldb";
  static final String INDEX = "indexes-ldb";
  static final String STARTTIME = "starttime-ldb";
  static final String OWNER = "owner-ldb";

  @VisibleForTesting
  // LevelDB恢复时备份文件扩展名
  static final String BACKUP_EXT = ".backup-";

  // 列类型标记
  private static final byte[] DOMAIN_ID_COLUMN = "d".getBytes(UTF_8);
  private static final byte[] EVENTS_COLUMN = "e".getBytes(UTF_8);
  private static final byte[] PRIMARY_FILTERS_COLUMN = "f".getBytes(UTF_8);
  private static final byte[] OTHER_INFO_COLUMN = "i".getBytes(UTF_8);
  private static final byte[] RELATED_ENTITIES_COLUMN = "r".getBytes(UTF_8);

  // 域信息列标记
  private static final byte[] DESCRIPTION_COLUMN = "d".getBytes(UTF_8);
  private static final byte[] OWNER_COLUMN = "o".getBytes(UTF_8);
  private static final byte[] READER_COLUMN = "r".getBytes(UTF_8);
  private static final byte[] WRITER_COLUMN = "w".getBytes(UTF_8);
  private static final byte[] TIMESTAMP_COLUMN = "t".getBytes(UTF_8);

  private static final byte[] EMPTY_BYTES = new byte[0];

  private static final String TIMELINE_STORE_VERSION_KEY =
      "timeline-store-version";

  private static final Version CURRENT_VERSION_INFO = Version.newInstance(1, 0);

  private static long writeBatchSize = 10000;

  @Private
  @VisibleForTesting
  static final FsPermission LEVELDB_DIR_UMASK = FsPermission
      .createImmutable((short) 0700);

  // 启动时间写缓存，写入前缓存
  private Map<EntityIdentifier, Long> startTimeWriteCache;
  // 启动时间读缓存，读取后缓存
  private Map<EntityIdentifier, Long> startTimeReadCache;

  // 各个功能对应的LevelDB实例
  private DB domaindb;
  private RollingLevelDB entitydb;
  private RollingLevelDB indexdb;
  private DB starttimedb;
  private DB ownerdb;

  // 旧数据删除线程
  private Thread deletionThread;

  /**
   * 构造函数，初始化服务。
   */
  public RollingLevelDBTimelineStore() {
    super(RollingLevelDBTimelineStore.class.getName());
  }

  private JniDBFactory factory;
  /**
   * 设置LevelDB工厂，仅用于测试。
   */
  @VisibleForTesting
  void setFactory(JniDBFactory fact) {
    this.factory = fact;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void serviceInit(Configuration conf) throws Exception {
    // 验证所有配置项合法性
    Preconditions
        .checkArgument(conf.getLong(TIMELINE_SERVICE_TTL_MS,
            DEFAULT_TIMELINE_SERVICE_TTL_MS) > 0,
            "%s property value should be greater than zero",
            TIMELINE_SERVICE_TTL_MS);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS) > 0,
        "%s property value should be greater than zero",
        TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE) >= 0,
        "%s property value should be greater than or equal to zero",
        TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE) > 0,
        " %s property value should be greater than zero",
        TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE) > 0,
        "%s property value should be greater than zero",
        TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES) > 0,
        "%s property value should be greater than zero",
        TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE) > 0,
        "%s property value should be greater than zero",
        TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE);

    // 初始化LevelDB配置
    Options options = new Options();
    options.createIfMissing(true);
    options.cacheSize(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE));
    if(factory == null) {
      factory = new JniDBFactory();
    }
    // 构建各级存储路径
    Path dbPath = new Path(
        conf.get(TIMELINE_SERVICE_LEVELDB_PATH), FILENAME);
    Path domainDBPath = new Path(dbPath, DOMAIN);
    Path starttimeDBPath = new Path(dbPath, STARTTIME);
    Path ownerDBPath = new Path(dbPath, OWNER);
    // 创建本地存储目录并设置权限
    try (FileSystem localFS = FileSystem.getLocal(conf)) {
      if (!localFS.exists(dbPath)) {
        if (!localFS.mkdirs(dbPath)) {
          throw new IOException("Couldn't create directory for leveldb "
              + "timeline store " + dbPath);
        }
        localFS.setPermission(dbPath, LEVELDB_DIR_UMASK);
      }
      if (!localFS.exists(domainDBPath)) {
        if (!localFS.mkdirs(domainDBPath)) {
          throw new IOException("Couldn't create directory for leveldb "
              + "timeline store " + domainDBPath);
        }
        localFS.setPermission(domainDBPath, LEVELDB_DIR_UMASK);
      }
      if (!localFS.exists(starttimeDBPath)) {
        if (!localFS.mkdirs(starttimeDBPath)) {
          throw new IOException("Couldn't create directory for leveldb "
              + "timeline store " + starttimeDBPath);
        }
        localFS.setPermission(starttimeDBPath, LEVELDB_DIR_UMASK);
      }
      if (!localFS.exists(ownerDBPath)) {
        if (!localFS.mkdirs(ownerDBPath)) {
          throw new IOException("Couldn't create directory for leveldb "
              + "timeline store " + ownerDBPath);
        }
        localFS.setPermission(ownerDBPath, LEVELDB_DIR_UMASK);
      }
    }
    // 设置LevelDB参数
    options.maxOpenFiles(conf.getInt(
        TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES));
    options.writeBufferSize(conf.getInt(
        TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE));
    LOG.info("Using leveldb path " + dbPath);
    // 加载/修复各个LevelDB
    domaindb = LeveldbUtils.loadOrRepairLevelDb(factory, domainDBPath, options);
    entitydb = new RollingLevelDB(ENTITY);
    entitydb.init(conf);
    indexdb = new RollingLevelDB(INDEX);
    indexdb.init(conf);
    starttimedb = LeveldbUtils.loadOrRepairLevelDb(factory, starttimeDBPath, options);
    ownerdb = LeveldbUtils.loadOrRepairLevelDb(factory, ownerDBPath, options);
    // 检查存储版本兼容性
    checkVersion();