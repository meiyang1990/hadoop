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
 * LevelDB 数据库管理器
 * 封装 LevelDB 的初始化、版本管理和定期压缩功能，用于 RM 状态存储
 * 主要功能：
 * - 数据库初始化（自动创建不存在的数据库）
 * - 版本信息存储和加载
 * - 定时触发数据库全量压缩以优化性能
 */
public class DBManager implements Closeable {
  public static final Logger LOG =
      LoggerFactory.getLogger(DBManager.class);
  private DB db;  // LevelDB 实例
  private Timer compactionTimer;  // 定时压缩任务的定时器

  /**
   * 初始化数据库：打开或创建 LevelDB 实例
   * @param configurationFile 数据库文件路径
   * @param options LevelDB 选项
   * @param initMethod 数据库初始化回调方法（首次创建时调用）
   * @return 打开的数据库实例
   */
  public DB initDatabase(File configurationFile, Options options,
                         Consumer<DB> initMethod) throws Exception {
    try {
      db = JniDBFactory.factory.open(configurationFile, options);
    } catch (NativeDB.DBException e) {
      // 数据库不存在时自动创建
      if (e.isNotFound() || e.getMessage().contains(" does not exist ")) {
        LOG.info("Creating configuration version/database at {}",
            configurationFile);
        options.createIfMissing(true);
        try {
          db = JniDBFactory.factory.open(configurationFile, options);
          initMethod.accept(db);  // 执行初始化逻辑（如写入初始版本）
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
   * 关闭数据库并停止压缩定时器
   */
  public void close() throws IOException {
    if (compactionTimer != null) {
      compactionTimer.cancel();
      compactionTimer = null;
    }
    if (db != null) {
      db.close();
      db = null;
    }
  }

  /**
   * 存储版本信息到数据库
   */
  public void storeVersion(String versionKey, Version versionValue) {
    byte[] data = ((VersionPBImpl) versionValue).getProto().toByteArray();
    db.put(bytes(versionKey), data);
  }

  /**
   * 从数据库加载版本信息
   */
  public Version loadVersion(String versionKey) throws Exception {
    Version version = null;
    try {
      byte[] data = db.get(bytes(versionKey));
      if (data != null) {
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
   * 启动定时压缩任务：定期触发 LevelDB 的全量压缩以优化性能
   * @param compactionIntervalMsec 压缩间隔（毫秒），0 表示禁用
   * @param className 用于命名压缩线程
   */
  public void startCompactionTimer(long compactionIntervalMsec,
                                    String className) {
    if (compactionIntervalMsec > 0) {
      compactionTimer = new Timer(
          className + " compaction timer", true);
      compactionTimer.schedule(new CompactionTimerTask(),
          compactionIntervalMsec, compactionIntervalMsec);
    }
  }

  /**
   * 压缩任务：调用 LevelDB 的 compactRange 执行全量压缩
   */
  private class CompactionTimerTask extends TimerTask {
    @Override
    public void run() {
      long start = Time.monotonicNow();
      LOG.info("Starting full compaction cycle");
      try {
        db.compactRange(null, null);  // null 参数表示压缩整个数据库
      } catch (DBException e) {
        LOG.error("Error compacting database", e);
      }
      long duration = Time.monotonicNow() - start;
      LOG.info("Full compaction cycle completed in " + duration + " msec");
    }
  }
}