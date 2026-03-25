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

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 按时间周期滚动存储LevelDB实例的管理类，支持按时间查找数据，过期数据可通过删除目录高效清除。
 * 核心功能：按配置周期创建新LevelDB，自动淘汰过期数据，支持按时间戳查询对应DB。
 */
class RollingLevelDB {

  /** 当前类日志实例. */
  private static final Logger LOG = LoggerFactory.
      getLogger(RollingLevelDB.class);
  /** LevelDB工厂实例，用于创建打开LevelDB. */
  private static JniDBFactory factory = new JniDBFactory();
  /** 线程安全的日期格式化器，用于生成和解析文件名. */
  private FastDateFormat fdf;
  /** 日期解析器，用于从已有文件名解析时间. */
  private SimpleDateFormat sdf;
  /** 日历实例，用于计算当前和下一个滚动周期时间. */
  private GregorianCalendar cal = new GregorianCalendar(
      TimeZone.getTimeZone("GMT"));
  /** 当前所有活跃的滚动LevelDB实例，key为起始时间戳，value为DB实例. */
  private final TreeMap<Long, DB> rollingdbs;
  /** 待淘汰的滚动LevelDB实例，key为起始时间戳，value为DB实例. */
  private final TreeMap<Long, DB> rollingdbsToEvict;
  /** 当前滚动LevelDB集合的名称. */
  private final String name;
  /** 下一次触发滚动检查的时间戳. */
  private volatile long nextRollingCheckMillis = 0;
  /** 本地文件系统实例，用于操作LevelDB目录和文件. */
  private FileSystem lfs = null;
  /** 存储所有滚动LevelDB实例的根目录. */
  private Path rollingDBPath;
  /** Hadoop配置对象. */
  private Configuration conf;
  /** 当前配置的滚动周期类型. */
  private RollingPeriod rollingPeriod;
  /** 数据存活时间，过期数据会被淘汰. */
  private long ttl;
  /** 是否启用TTL过期淘汰功能. */
  private boolean ttlEnabled;

  /** 滚动周期枚举，定义不同周期对应的日期格式. */
  enum RollingPeriod {
    /** 按天滚动. */
    DAILY {
      @Override
      public String dateFormat() {
        return "yyyy-MM-dd";
      }
    },
    /** 按半天（12小时）滚动. */
    HALF_DAILY {
      @Override
      public String dateFormat() {
        return "yyyy-MM-dd-HH";
      }
    },
    /** 按四分之一天（6小时）滚动. */
    QUARTER_DAILY {
      @Override
      public String dateFormat() {
        return "yyyy-MM-dd-HH";
      }
    },
    /** 按小时滚动. */
    HOURLY {
      @Override
      public String dateFormat() {
        return "yyyy-MM-dd-HH";
      }
    },
    /** 按5分钟滚动. */
    MINUTELY {
      @Override
      public String dateFormat() {
        return "yyyy-MM-dd-HH-mm";
      }
    };
    public abstract String dateFormat();
  }

  /**
   * 批量写入包装类，关联批量操作和对应LevelDB实例. */
  public static class RollingWriteBatch {
    /** 关联的LevelDB实例. */
    private final DB db;
    /** LevelDB批量写入对象. */
    private final WriteBatch writeBatch;

    /**
     * 构造批量写入包装对象.
     * @param db 目标LevelDB实例
     * @param writeBatch 批量写入对象
     */
    public RollingWriteBatch(final DB db, final WriteBatch writeBatch) {
      this.db = db;
      this.writeBatch = writeBatch;
    }

    public DB getDB() {
      return db;
    }

    public WriteBatch getWriteBatch() {
      return writeBatch;
    }

    /** 提交批量写入到LevelDB. */
    public void write() {
      db.write(writeBatch);
    }

    /** 关闭批量写入对象，释放资源. */
    public void close() {
      IOUtils.cleanupWithLogger(LOG, writeBatch);
    }
  }

  /**
   * 构造滚动LevelDB管理器.
   * @param name 滚动LevelDB集合名称
   */
  RollingLevelDB(String name) {
    this.name = name;
    this.rollingdbs = new TreeMap<Long, DB>();
    this.rollingdbsToEvict = new TreeMap<Long, DB>();
  }

  protected String getName() {
    return name;
  }

  /**
   * 获取当前时间，可被子类重写用于测试. */
  protected long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  public long getNextRollingTimeMillis() {
    return nextRollingCheckMillis;
  }

  public long getTimeToLive() {
    return ttl;
  }

  public boolean getTimeToLiveEnabled() {
    return ttlEnabled;
  }

  /**
   * 设置下一次滚动时间，并记录日志.
   * @param timestamp 下一次滚动时间戳
   */
  protected void setNextRollingTimeMillis(final long timestamp) {
    this.nextRollingCheckMillis = timestamp;
    LOG.info("Next rolling time for " + getName() + " is "
        + fdf.format(nextRollingCheckMillis));
  }

  /**
   * 初始化滚动LevelDB管理器，加载配置和已有数据文件.
   * @param config Hadoop配置对象
   * @throws Exception 初始化过程中发生的异常
   */
  public void init(final Configuration config) throws Exception {
    LOG.info("Initializing RollingLevelDB for " + getName());
    this.conf = config;
    this.ttl = conf.getLong(YarnConfiguration.TIMELINE_SERVICE_TTL_MS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_TTL_MS);
    this.ttlEnabled = conf.getBoolean(
        YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE, true);
    this.rollingDBPath = new Path(
        conf.get(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH),
        RollingLevelDBTimelineStore.FILENAME);
    initFileSystem();
    initRollingPeriod();
    initHistoricalDBs();
  }

  /**
   * 初始化本地文件系统，创建存储根目录.
   * @throws IOException 创建目录失败抛出异常
   */
  protected void initFileSystem() throws IOException {
    lfs = FileSystem.getLocal(conf);
    boolean success = lfs.mkdirs(rollingDBPath,
        RollingLevelDBTimelineStore.LEVELDB_DIR_UMASK);
    if (!success) {
      throw new IOException("Failed to create leveldb root directory "
          + rollingDBPath);
    }
  }

  /**
   * 从配置加载滚动周期，初始化日期格式化工具. */
  protected synchronized void initRollingPeriod() {
    final String lcRollingPeriod = conf.get(
        YarnConfiguration.TIMELINE_SERVICE_ROLLING_PERIOD,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ROLLING_PERIOD);
    this.rollingPeriod = RollingPeriod.valueOf(lcRollingPeriod
        .toUpperCase(Locale.ENGLISH));
    fdf = FastDateFormat.getInstance(rollingPeriod.dateFormat(),
        TimeZone.getTimeZone("GMT"));
    sdf = new SimpleDateFormat(rollingPeriod.dateFormat());
    sdf.setTimeZone(fdf.getTimeZone());
  }

  /**
   * 扫描存储目录，加载已有的历史LevelDB实例并打开.
   * @throws IOException 文件扫描读取异常
   */
  protected synchronized void initHistoricalDBs() throws IOException {
    Path rollingDBGlobPath = new Path(rollingDBPath, getName() + ".*");
    FileStatus[] statuses = lfs.globStatus(rollingDBGlobPath);
    for (FileStatus status : statuses) {
      String dbName = FilenameUtils.getExtension(status.getPath().toString());
      try {
        Long dbStartTime = sdf.parse(dbName).getTime();
        initRollingLevelDB(dbStartTime, status.getPath());
      } catch (ParseException pe) {
        LOG.warn("Failed to initialize rolling leveldb " + dbName + " for "
            + getName());
      }
    }
  }

  /**
   * 初始化单个滚动LevelDB实例，打开并加入活跃列表.
   * @param dbStartTime DB实例的起始时间戳
   * @param rollingInstanceDBPath DB实例存储路径
   */
  private void initRollingLevelDB(Long dbStartTime,
      Path rollingInstanceDBPath) {
    if (rollingdbs.containsKey(dbStartTime)) {
      return;
    }
    Options options = new Options();
    options.createIfMissing(true);
    // 从配置读取读缓存大小配置
    options.cacheSize(conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE));
    // 从配置读取最大打开文件数配置
    options.maxOpenFiles(conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES));
    // 从配置读取写缓存大小配置
    options.writeBufferSize(conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE));
    LOG.info("Initializing rolling leveldb instance :" + rollingInstanceDBPath
        + " for start time: " + dbStartTime);
    DB db = null;
    try {
      db = factory.open(
          new File(rollingInstanceDBPath.toUri().getPath()), options);
      rollingdbs.put(dbStartTime, db);
      String dbName = fdf.format(dbStartTime);
      LOG.info("Added rolling leveldb instance " + dbName + " to " + getName());
    } catch (IOException ioe) {
      LOG.warn("Failed to open rolling leveldb instance :"
          + new File(rollingInstanceDBPath.toUri().getPath()), ioe);
    }
  }

  /**
   * 获取指定DB实例在列表中的前一个DB实例.
   * @param db 当前DB实例
   * @return 前一个DB实例，不存在返回null
   */
  synchronized DB getPreviousDB(DB db) {
    Iterator<DB> iterator = rollingdbs.values().iterator();
    DB prev = null;
    while (iterator.hasNext()) {
      DB cur = iterator.next();
      if (cur == db) {
        break;
      }
      prev = cur;
    }
    return prev;
  }

  /**
   * 获取指定DB实例的起始时间戳.
   * @param db DB实例
   * @return 起始时间戳，未找到返回-1
   */
  synchronized long getStartTimeFor(DB db) {
    long startTime = -1;
    for (Map.Entry<Long, DB> entry : rollingdbs.entrySet()) {
      if (entry.getValue() == db) {
        startTime = entry.getKey();
      }
    }
    return startTime;
  }

  /**
   * 根据起始时间戳获取对应DB实例，需要时触发滚动.
   * @param startTime 查询的起始时间戳
   * @return 对应的DB实例，不存在返回null
   */
  public synchronized DB getDBForStartTime(long startTime) {
    // 限制输入不能超过当前时间
    startTime = Math.min(startTime, currentTimeMillis());

    if (startTime >= getNextRollingTimeMillis()) {
      roll(startTime);
    }
    Entry<Long, DB> entry = rollingdbs.floorEntry(startTime);
    if (entry == null) {
      return null;
    }
    return entry.getValue();
  }

  /**
   * 触发滚动，创建新的DB实例，标记过期DB待淘汰.
   * @param startTime 触发滚动的时间戳
   */
  private void roll(long startTime) {
    LOG.info("Rolling new DB instance for " + getName());
    long currentStartTime = computeCurrentCheckMillis(startTime);
    setNextRollingTimeMillis(computeNextCheckMillis(currentStartTime));
    String currentRollingDBInstance = fdf.format(currentStartTime);
    String currentRollingDBName = getName() + "." + currentRollingDBInstance;
    Path currentRollingDBPath = new Path(rollingDBPath, currentRollingDBName);
    if (getTimeToLiveEnabled()) {
      scheduleOldDBsForEviction();
    }
    initRollingLevelDB(currentStartTime, currentRollingDBPath);
  }

  /**
   * 遍历活跃DB，将过期DB移动到待淘汰列表. */
  private synchronized void scheduleOldDBsForEviction() {
    // 计算淘汰阈值：当前时间减去TTL
    long evictionThreshold = computeCurrentCheckMillis(currentTimeMillis()
        - getTimeToLive());

    LOG.info("Scheduling " + getName() + " DBs older than "
        + fdf.format(evictionThreshold) + " for eviction");
    Iterator<Entry<Long, DB>> iterator = rollingdbs.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<Long, DB> entry = iterator.next();
      // 起始时间早于阈值则加入淘汰列表
      if (entry.getKey() < evictionThreshold) {
        LOG.info("Scheduling " + getName() + " eviction for "
            + fdf.format(entry.getKey()));
        iterator.remove();
        rollingdbsToEvict.put(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * 执行淘汰，关闭待淘汰DB并删除存储目录. */
  public synchronized void evictOldDBs() {
    LOG.info("Evicting " + getName() + " DBs scheduled for eviction");
    Iterator<Entry<Long, DB>> iterator = rollingdbsToEvict.entrySet()
        .iterator();
    while (iterator.hasNext()) {
      Entry<Long, DB> entry = iterator.next();
      IOUtils.cleanupWithLogger(LOG, entry.getValue());
      String dbName = fdf.format(entry.getKey());
      Path path = new Path(rollingDBPath, getName() + "." + dbName);
      try {
        LOG.info("Removing old db directory contents in " + path);
        lfs.delete(path, true);
      } catch (IOException ioe) {
        LOG.warn("Failed to evict old db " + path, ioe);
      }
      iterator.remove();
    }
  }

  /**
   * 停止滚动LevelDB管理器，关闭所有打开的DB和文件系统.
   * @throws Exception 关闭过程异常
   */
  public void stop() throws Exception {
    for (DB db : rollingdbs.values()) {
      IOUtils.cleanupWithLogger(LOG, db);
    }
    IOUtils.cleanupWithLogger(LOG, lfs);
  }

  private long computeNextCheckMillis(long now) {
    return computeCheckMillis(now, true);
  }

  public long computeCurrentCheckMillis(long now) {
    return computeCheckMillis(now, false);
  }

  /**
   * 根据当前时间和滚动周期，计算周期对齐后的时间戳.
   * 由于使用共享Calendar实例，需要同步调用.
   * @param now 输入时间戳
   * @param next 是否计算下一个周期的起始时间
   * @return 对齐后的时间戳
   */
  private synchronized long computeCheckMillis(long now, boolean next) {
    cal.setTimeInMillis(now);
    // 清零秒和毫秒，对齐周期
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);

    // 根据不同滚动周期做不同的对齐计算
    if (