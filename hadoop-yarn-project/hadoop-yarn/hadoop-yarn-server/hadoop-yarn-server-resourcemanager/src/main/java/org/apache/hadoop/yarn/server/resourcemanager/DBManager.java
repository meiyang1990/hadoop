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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Consumer;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

/**
 * LevelDB数据库管理器，为YARN ResourceManager提供持久化存储能力
 * 负责数据库初始化、版本存储加载、自动定时压缩和资源关闭管理
 */
public class DBManager implements Closeable {
  public static final Logger LOG =
      LoggerFactory.getLogger(DBManager.class);
  // LevelDB数据库实例
  private DB db;
  // 定时压缩任务定时器
  private Timer compactionTimer;

  /**
   * 初始化LevelDB数据库，处理不存在库时自动创建逻辑
   * @param configurationFile 数据库存储文件路径
   * @param options LevelDB配置选项
   * @param initMethod 数据库初始化完成后的回调方法，用于初始化数据结构
   * @return 初始化完成的LevelDB实例
   * @throws Exception 初始化过程中抛出的异常
   */
  public DB initDatabase(File configurationFile, Options options,
                         Consumer<DB> initMethod) throws Exception {
    try {
      // 尝试打开已有数据库
      db = JniDBFactory.factory.open(configurationFile, options);
    } catch (NativeDB.DBException e) {
      // 数据库不存在，需要新建
      if (e.isNotFound() || e.getMessage().contains(" does not exist ")) {
        LOG.info("Creating configuration version/database at {}",
            configurationFile);
        options.createIfMissing(true);
        try {
          // 创建并打开新数据库
          db = JniDBFactory.factory.open(configurationFile, options);
          // 执行初始化回调
          initMethod.accept(db);
        } catch (DBException dbErr) {
          throw new IOException(dbErr.getMessage(), dbErr);
        }
      } else {
        throw e;
      }
    }

    return db;
  }

  /**
   * 关闭数据库并释放所有资源
   * @throws IOException 关闭数据库时抛出IO异常
   */
  public void close() throws IOException {
    // 取消定时压缩任务
    if (compactionTimer != null) {
      compactionTimer.cancel();
      compactionTimer = null;
    }
    // 关闭数据库实例
    if (db != null) {
      db.close();
      db = null;
    }
  }

  /**
   * 存储版本信息到数据库
   * @param versionKey 版本信息键名
   * @param versionValue 版本对象
   */
  public void storeVersion(String versionKey, Version versionValue) {
    byte[] data = ((VersionPBImpl) versionValue).getProto().toByteArray();
    db.put(bytes(versionKey), data);
  }

  /**
   * 从数据库加载版本信息
   * @param versionKey 版本信息键名
   * @return 加载得到的版本对象，不存在则返回null
   * @throws Exception 加载过程中抛出异常
   */
  public Version loadVersion(String versionKey) throws Exception {
    Version version = null;
    try {
      byte[] data = db.get(bytes(versionKey));
      if (data != null) {
        // 将字节数据反序列化为版本对象
        version = new VersionPBImpl(YarnServerCommonProtos.VersionProto
            .parseFrom(data));
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
    return version;
  }

  @VisibleForTesting
  public void setDb(DB db) {
    this.db = db;
  }

  /**
   * 启动定时全量压缩任务，定期整理LevelDB空间
   * @param compactionIntervalMsec 压缩间隔，单位毫秒
   * @param className 所属类名，用于命名定时器线程
   */
  public void startCompactionTimer(long compactionIntervalMsec,
                                    String className) {
    if (compactionIntervalMsec > 0) {
      // 创建后台守护线程定时器
      compactionTimer = new Timer(
          className + " compaction timer", true);
      // 按固定间隔调度压缩任务
      compactionTimer.schedule(new CompactionTimerTask(),
          compactionIntervalMsec, compactionIntervalMsec);
    }
  }

  /**
   * LevelDB全量压缩定时任务
   */
  private class CompactionTimerTask extends TimerTask {
    @Override
    public void run() {
      long start = Time.monotonicNow();
      LOG.info("Starting full compaction cycle");
      try {
        // 对整个数据库执行全量压缩
        db.compactRange(null, null);
      } catch (DBException e) {
        LOG.error("Error compacting database", e);
      }
      long duration = Time.monotonicNow() - start;
      LOG.info("Full compaction cycle completed in " + duration + " msec");
    }
  }
}