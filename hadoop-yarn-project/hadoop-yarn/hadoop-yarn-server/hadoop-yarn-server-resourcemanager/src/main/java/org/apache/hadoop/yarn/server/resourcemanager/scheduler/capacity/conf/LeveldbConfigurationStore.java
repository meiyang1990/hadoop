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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.server.resourcemanager.DBManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

/**
 * YARN配置存储接口{@link YarnConfigurationStore}的LevelDB实现，
 * 用于持久化存储容量调度器的配置变更。
 */
public class LeveldbConfigurationStore extends YarnConfigurationStore {

  public static final Logger LOG =
      LoggerFactory.getLogger(LeveldbConfigurationStore.class);

  // 配置存储数据库目录名
  private static final String DB_NAME = "yarn-conf-store";
  // 变更日志key
  private static final String LOG_KEY = "log";
  // 版本key
  private static final String VERSION_KEY = "version";
  // 配置版本存储目录名
  private static final String CONF_VERSION_NAME = "conf-version-store";
  // 配置版本key
  private static final String CONF_VERSION_KEY = "conf-version";
  // 配置存储LevelDB实例
  private DB db;
  // 配置存储数据库管理器
  private DBManager dbManager;
  // 配置版本存储数据库管理器
  private DBManager versionDbManager;
  // 配置版本存储LevelDB实例
  private DB versionDb;
  // 最大保留变更日志数量
  private long maxLogs;
  // YARN配置对象
  private Configuration conf;
  // 初始化时的调度器配置
  private Configuration initSchedConf;
  @VisibleForTesting
  // 当前存储版本信息
  protected static final Version CURRENT_VERSION_INFO = Version
      .newInstance(0, 1);

  /**
   * 初始化LevelDB配置存储，创建数据库连接并启动压缩定时器。
   * @param config YARN配置
   * @param schedConf 初始化调度器配置
   * @param rmContext RM上下文
   * @throws IOException 初始化失败抛出异常
   */
  @Override
  public void initialize(Configuration config, Configuration schedConf,
      RMContext rmContext) throws IOException {
    this.conf = config;
    this.initSchedConf = schedConf;
    this.dbManager = new DBManager();
    this.versionDbManager = new DBManager();
    try {
      // 初始化两个LevelDB数据库
      initDatabase();
      // 读取最大日志配置
      this.maxLogs = config.getLong(
          YarnConfiguration.RM_SCHEDCONF_MAX_LOGS,
          YarnConfiguration.DEFAULT_RM_SCHEDCONF_LEVELDB_MAX_LOGS);
      // 读取压缩间隔配置
      long compactionIntervalMsec = config.getLong(
          YarnConfiguration.RM_SCHEDCONF_LEVELDB_COMPACTION_INTERVAL_SECS,
          YarnConfiguration
              .DEFAULT_RM_SCHEDCONF_LEVELDB_COMPACTION_INTERVAL_SECS) * 1000;
      // 启动自动压缩定时器
      dbManager.startCompactionTimer(compactionIntervalMsec,
          this.getClass().getSimpleName());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * 格式化存储，删除现有所有数据。
   * @throws Exception 格式化失败抛出异常
   */
  @Override
  public void format() throws Exception {
    close();
    FileSystem fs = FileSystem.getLocal(conf);
    fs.delete(getStorageDir(DB_NAME), true);
  }

  /**
   * 初始化配置版本和配置存储两个LevelDB实例。
   * @throws Exception 初始化失败抛出异常
   */
  private void initDatabase() throws Exception {
    // 创建配置版本存储目录
    Path confVersion = createStorageDir(CONF_VERSION_NAME);
    Options confOptions = new Options();
    // 如果不存在不创建，由初始化回调处理
    confOptions.createIfMissing(false);
    File confVersionFile = new File(confVersion.toString());

    // 初始化配置版本数据库
    versionDb = versionDbManager.initDatabase(confVersionFile, confOptions,
        this::initVersionDb);

    // 创建配置存储目录
    Path storeRoot = createStorageDir(DB_NAME);
    Options options = new Options();
    options.createIfMissing(false);
    // 自定义key排序规则，保证特殊key排在末尾
    options.comparator(new DBComparator() {
      @Override
      public int compare(byte[] key1, byte[] key2) {
        String key1Str = new String(key1, StandardCharsets.UTF_8);
        String key2Str = new String(key2, StandardCharsets.UTF_8);
        if (key1Str.equals(key2Str)) {
          return 0;
        } else if (key1Str.equals(VERSION_KEY)) {
          // 版本key排在最后
          return 1;
        } else if (key2Str.equals(VERSION_KEY)) {
          return -1;
        } else if (key1Str.equals(LOG_KEY)) {
          // 日志key排在配置项之后，版本key之前
          return 1;
        } else if (key2Str.equals(LOG_KEY)) {
          return -1;
        }
        // 其他key按字符串默认排序
        return key1Str.compareTo(key2Str);
      }

      @Override
      public String name() {
        return "keyComparator";
      }

      public byte[] findShortestSeparator(byte[] start, byte[] limit) {
        return start;
      }

      public byte[] findShortSuccessor(byte[] key) {
        return key;
      }
    });
    LOG.info("Using conf database at {}", storeRoot);
    File dbFile = new File(storeRoot.toString());
    // 初始化配置存储数据库
    db = dbManager.initDatabase(dbFile, options, this::initDb);
  }

  /**
   * 初始化版本数据库，设置初始版本号为0。
   * @param database 版本数据库实例
   */
  private void initVersionDb(DB database) {
    database.put(bytes(CONF_VERSION_KEY), bytes(String.valueOf(0)));
  }

  /**
   * 初始化配置数据库，写入初始调度配置并设置版本号。
   * @param database 配置数据库实例
   */
  private void initDb(DB database) {
    WriteBatch initBatch = database.createWriteBatch();
    // 批量写入初始调度配置
    for (Map.Entry<String, String> kv : initSchedConf) {
      initBatch.put(bytes(kv.getKey()), bytes(kv.getValue()));
    }
    database.write(initBatch);
    // 版本号自增
    increaseConfigVersion();
  }

  /**
   * 创建存储目录，设置700权限。
   * @param storageName 存储目录名
   * @return 存储目录路径
   * @throws IOException 创建目录失败抛出异常
   */
  private Path createStorageDir(String storageName) throws IOException {
    Path root = getStorageDir(storageName);
    FileSystem fs = FileSystem.getLocal(conf);
    fs.mkdirs(root, new FsPermission((short) 0700));
    return root;
  }

  /**
   * 获取完整存储路径。
   * @param storageName 存储目录名
   * @return 完整路径
   * @throws IOException 未配置存储路径抛出异常
   */
  private Path getStorageDir(String storageName) throws IOException {
    String storePath = conf.get(YarnConfiguration.RM_SCHEDCONF_STORE_PATH);
    if (storePath == null) {
      throw new IOException("No store location directory configured in " +
          YarnConfiguration.RM_SCHEDCONF_STORE_PATH);
    }
    return new Path(storePath, storageName);
  }

  /**
   * 关闭两个数据库连接。
   * @throws IOException 关闭失败抛出异常
   */
  @Override
  public void close() throws IOException {
    dbManager.close();
    versionDbManager.close();
  }

  /**
   * 记录配置变更日志，超过最大日志数量时删除最早的日志。
   * @param logMutation 配置变更日志
   * @throws IOException 写入LevelDB失败抛出异常
   */
  @Override
  public void logMutation(LogMutation logMutation) throws IOException {
    if (maxLogs > 0) {
      // 反序列化现有日志
      LinkedList<LogMutation> logs = deserLogMutations(db.get(bytes(LOG_KEY)));
      // 添加新变更日志
      logs.add(logMutation);
      // 超过最大数量，删除最早的日志
      if (logs.size() > maxLogs) {
        logs.removeFirst();
      }
      // 序列化后写回LevelDB
      db.put(bytes(LOG_KEY), serLogMutations(logs));
    }
  }

  /**
   * 确认配置变更，将变更持久化到配置存储。
   * @param pendingMutation 待确认的变更
   * @param isValid 变更是否有效
   */
  @Override
  public void confirmMutation(LogMutation pendingMutation,
      boolean isValid) {
    if (isValid) {
      WriteBatch updateBatch = db.createWriteBatch();
      // 批量处理所有配置变更
      for (Map.Entry<String, String> changes :
          pendingMutation.getUpdates().entrySet()) {
        if (changes.getValue() == null || changes.getValue().isEmpty()) {
          // 空值删除配置项
          updateBatch.delete(bytes(changes.getKey()));
        } else {
          // 非空值更新配置项
          updateBatch.put(bytes(changes.getKey()), bytes(changes.getValue()));
        }
      }
      // 配置版本号自增
      increaseConfigVersion();
      // 批量写入LevelDB
      db.write(updateBatch);
    }
  }

  /**
   * 将变更日志列表序列化为字节数组。
   * @param mutations 变更日志列表
   * @return 序列化后的字节数组
   * @throws IOException 序列化失败抛出异常
   */
  private byte[] serLogMutations(LinkedList<LogMutation> mutations) throws
      IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutput oos = new ObjectOutputStream(baos)) {
      oos.writeObject(mutations);
      oos.flush();
      return baos.toByteArray();
    }
  }

  /**
   * 将字节数组反序列化为变更日志列表。
   * @param mutations 序列化后的字节数组
   * @return 反序列化后的变更日志列表
   * @throws IOException 反序列化失败抛出异常
   */
  // Because of type erasure casting to LinkedList<LogMutation> will be
  // unchecked. A way around that would be to iterate over the logMutations
  // which is overkill in this case.
  @SuppressWarnings("unchecked")
  private LinkedList<LogMutation> deserLogMutations(byte[] mutations) throws
      IOException {
    if (mutations == null) {
      return new LinkedList<>();
    }

    try (ObjectInput input = new ObjectInputStream(
        new ByteArrayInputStream(mutations))) {
      return (LinkedList<LogMutation>) input.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * 从LevelDB读取并还原当前完整配置。
   * @return 当前调度器配置
   */
  @Override
  public synchronized Configuration retrieve() {
    DBIterator itr = db.iterator();
    itr.seekToFirst();
    Configuration config = new Configuration(false);
    // 遍历所有key，读取配置项
    while (itr.hasNext()) {
      Map.Entry<byte[], byte[]> entry = itr.next();
      String key = new String(entry.getKey(), StandardCharsets.UTF_8);
      String value = new String(entry.getValue(), StandardCharsets.UTF_8);
      // 遇到特殊key（日志/版本）停止遍历
      if (key.equals(LOG_KEY) || key.equals(VERSION_KEY)) {
        break;
      }
      config.set(key, value);
    }
    return config;
  }

  /**
   * 配置版本号自增并写回存储。
   */
  private void increaseConfigVersion() {
    long configVersion = getConfigVersion() + 1L;
    versionDb.put(bytes(CONF_VERSION_KEY),
        bytes(String.valueOf(configVersion)));
  }

  /**
   * 获取当前配置版本号。
   * @return 当前配置版本号
   */
  @Override
  public long getConfigVersion() {
    String version = new String(versionDb.get(bytes(CONF_VERSION_KEY)),
        StandardCharsets.UTF_8);
    return Long.parseLong(version);
  }

  @Override
  public List<LogMutation> getConfirmedConfHistory(long fromId) {
    return null; // unimplemented
  }

  /**
   * 获取配置存储的版本信息。
   * @return 存储版本信息
   * @throws Exception 读取失败抛出异常
   */
  @Override
  public Version getConfStoreVersion() throws Exception {
    return dbManager.loadVersion(VERSION_KEY);
  }

  @VisibleForTesting
  @Override
  protected LinkedList<LogMutation> getLogs() throws Exception {
    return deserLogMutations(db.get(bytes(LOG_KEY)));
  }

  @VisibleForTesting
  protected DB getDB() {
    return db;
  }

  /**
   * 存储当前版本信息到LevelDB。
   * @throws Exception 存储失败抛出异常
   */
  @Override
  public void storeVersion() throws Exception {
    try {
      storeVersion(CURRENT_VERSION_INFO);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  protected void storeVersion(Version version) {
    dbManager.storeVersion(VERSION_KEY, version);
  }

  /**
   * 获取当前代码定义的存储版本。
   * @return 当前版本信息
   */
  @Override
  public Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }
}