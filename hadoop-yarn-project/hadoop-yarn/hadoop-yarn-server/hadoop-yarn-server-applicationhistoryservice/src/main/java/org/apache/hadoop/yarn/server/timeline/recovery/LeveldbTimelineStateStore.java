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

package org.apache.hadoop.yarn.server.timeline.recovery;

import static org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils.prefixMatches;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.timeline.recovery.records.TimelineDelegationTokenIdentifierData;
import org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils.KeyBuilder;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

/**
 * 基于LevelDB实现的时间线服务状态存储，支持持久化存储时间线服务的委托令牌等状态信息
 */
public class LeveldbTimelineStateStore extends
    TimelineStateStore {

  public static final Logger LOG =
      LoggerFactory.getLogger(LeveldbTimelineStateStore.class);

  // LevelDB数据库文件名
  private static final String DB_NAME = "timeline-state-store.ldb";
  // LevelDB存储目录权限，仅允许所有者读写执行
  private static final FsPermission LEVELDB_DIR_UMASK = FsPermission
      .createImmutable((short) 0700);

  // 委托令牌条目前缀键
  private static final byte[] TOKEN_ENTRY_PREFIX = bytes("t");
  // 委托令牌主密钥条目前缀键
  private static final byte[] TOKEN_MASTER_KEY_ENTRY_PREFIX = bytes("k");
  // 最新序列号存储键
  private static final byte[] LATEST_SEQUENCE_NUMBER_KEY = bytes("s");

  // 当前存储版本号
  private static final Version CURRENT_VERSION_INFO = Version.newInstance(1, 0);
  // 存储版本号存储键
  private static final byte[] TIMELINE_STATE_STORE_VERSION_KEY = bytes("v");

  // LevelDB实例引用
  private DB db;

  public LeveldbTimelineStateStore() {
    super(LeveldbTimelineStateStore.class.getName());
  }

  @Override
  protected void initStorage(Configuration conf) throws IOException {
  }

  @Override
  protected void startStorage() throws IOException {
    Options options = new Options();
    // 从配置获取存储路径，拼接数据库文件名
    Path dbPath =
        new Path(
            getConfig().get(
                YarnConfiguration.TIMELINE_SERVICE_LEVELDB_STATE_STORE_PATH),
            DB_NAME);
    FileSystem localFS = null;
    try {
      // 获取本地文件系统实例
      localFS = FileSystem.getLocal(getConfig());
      // 存储目录不存在则创建并设置权限
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
    JniDBFactory factory = new JniDBFactory();
    try {
      // 先尝试打开已存在的数据库，不自动创建
      options.createIfMissing(false);
      db = factory.open(new File(dbPath.toString()), options);
      LOG.info("Loading the existing database at th path: " + dbPath.toString());
      // 检查存储版本兼容性
      checkVersion();
    } catch (NativeDB.DBException e) {
      // 数据库不存在则创建新数据库
      if (e.isNotFound() || e.getMessage().contains(" does not exist ")) {
        try {
          options.createIfMissing(true);
          db = factory.open(new File(dbPath.toString()), options);
          LOG.info("Creating a new database at th path: " + dbPath.toString());
          // 存储当前版本号
          storeVersion(CURRENT_VERSION_INFO);
        } catch (DBException ex) {
          throw new IOException(ex);
        }
      } else {
        throw new IOException(e);
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void closeStorage() throws IOException {
    IOUtils.cleanupWithLogger(LOG, db);
  }

  @Override
  public TimelineServiceState loadState() throws IOException {
    LOG.info("Loading timeline service state from leveldb");
    TimelineServiceState state = new TimelineServiceState();
    // 加载所有主密钥
    int numKeys = loadTokenMasterKeys(state);
    // 加载所有委托令牌
    int numTokens = loadTokens(state);
    // 加载最新序列号
    loadLatestSequenceNumber(state);
    LOG.info("Loaded " + numKeys + " master keys and " + numTokens
        + " tokens from leveldb, and latest sequence number is "
        + state.getLatestSequenceNumber());
    return state;
  }

  @Override
  public void storeToken(TimelineDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException {
    DataOutputStream ds = null;
    WriteBatch batch = null;
    try {
      // 构造令牌存储键
      byte[] k = createTokenEntryKey(tokenId.getSequenceNumber());
      // 令牌已存在则抛出异常
      if (db.get(k) != null) {
        throw new IOException(tokenId + " already exists");
      }
      // 序列化令牌数据
      byte[] v = buildTokenData(tokenId, renewDate);
      ByteArrayOutputStream bs = new ByteArrayOutputStream();
      ds = new DataOutputStream(bs);
      ds.writeInt(tokenId.getSequenceNumber());
      // 批量写入：写入令牌和最新序列号
      batch = db.createWriteBatch();
      batch.put(k, v);
      batch.put(LATEST_SEQUENCE_NUMBER_KEY, bs.toByteArray());
      db.write(batch);
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      IOUtils.cleanupWithLogger(LOG, ds);
      IOUtils.cleanupWithLogger(LOG, batch);
    }
  }

  @Override
  public void updateToken(TimelineDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException {
    try {
      // 构造令牌存储键
      byte[] k = createTokenEntryKey(tokenId.getSequenceNumber());
      // 令牌不存在则抛出异常
      if (db.get(k) == null) {
        throw new IOException(tokenId + " doesn't exist");
      }
      // 序列化更新后的令牌数据
      byte[] v = buildTokenData(tokenId, renewDate);
      db.put(k, v);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void removeToken(TimelineDelegationTokenIdentifier tokenId)
      throws IOException {
    try {
      // 构造令牌存储键并删除
      byte[] key = createTokenEntryKey(tokenId.getSequenceNumber());
      db.delete(key);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void storeTokenMasterKey(DelegationKey key) throws IOException {
    try {
      // 构造主密钥存储键
      byte[] k = createTokenMasterKeyEntryKey(key.getKeyId());
      // 主密钥已存在则抛出异常
      if (db.get(k) != null) {
        throw new IOException(key + " already exists");
      }
      // 序列化主密钥数据并存储
      byte[] v = buildTokenMasterKeyData(key);
      db.put(k, v);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void removeTokenMasterKey(DelegationKey key) throws IOException {
    try {
      // 构造主密钥存储键并删除
      byte[] k = createTokenMasterKeyEntryKey(key.getKeyId());
      db.delete(k);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  private static byte[] buildTokenData(
      TimelineDelegationTokenIdentifier tokenId, Long renewDate)
      throws IOException {
    TimelineDelegationTokenIdentifierData data =
        new TimelineDelegationTokenIdentifierData(tokenId, renewDate);
    return data.toByteArray();
  }

  private static byte[] buildTokenMasterKeyData(DelegationKey key)
      throws IOException {
    ByteArrayOutputStream memStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(memStream);
    try {
      // 序列化主密钥对象到字节数组
      key.write(dataStream);
      dataStream.close();
    } finally {
      IOUtils.cleanupWithLogger(LOG, dataStream);
    }
    return memStream.toByteArray();
  }

  private static void loadTokenMasterKeyData(TimelineServiceState state,
      byte[] keyData)
      throws IOException {
    DelegationKey key = new DelegationKey();
    DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(keyData));
    try {
      // 反序列化主密钥对象
      key.readFields(in);
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    // 添加到状态对象中
    state.tokenMasterKeyState.add(key);
  }

  private static void loadTokenData(TimelineServiceState state, byte[] tokenData)
      throws IOException {
    TimelineDelegationTokenIdentifierData data =
        new TimelineDelegationTokenIdentifierData();
    DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(tokenData));
    try {
      // 反序列化令牌数据
      data.readFields(in);
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    // 添加到状态对象中
    state.tokenState.put(data.getTokenIdentifier(), data.getRenewDate());
  }

  private int loadTokenMasterKeys(TimelineServiceState state)
      throws IOException {
    // 构造主密钥前缀用于范围查询
    byte[] base = KeyBuilder.newInstance().add(TOKEN_MASTER_KEY_ENTRY_PREFIX)
        .getBytesForLookup();
    int numKeys = 0;
    LeveldbIterator iterator = null;
    try {
      // 遍历所有前缀匹配的主密钥条目
      for (iterator = new LeveldbIterator(db), iterator.seek(base);
          iterator.hasNext(); iterator.next()) {
        byte[] k = iterator.peekNext().getKey();
        // 前缀不匹配则结束遍历
        if (!prefixMatches(base, base.length, k)) {
          break;
        }
        byte[] v = iterator.peekNext().getValue();
        // 加载主密钥到状态
        loadTokenMasterKeyData(state, v);
        ++numKeys;
      }
    } finally {
      IOUtils.cleanupWithLogger(LOG, iterator);
    }
    return numKeys;
  }

  private int loadTokens(TimelineServiceState state) throws IOException {
    // 构造令牌前缀用于范围查询
    byte[] base = KeyBuilder.newInstance().add(TOKEN_ENTRY_PREFIX)
        .getBytesForLookup();
    int numTokens = 0;
    LeveldbIterator iterator = null;
    try {
      // 遍历所有前缀匹配的令牌条目
      for (iterator = new LeveldbIterator(db), iterator.seek(base);
          iterator.hasNext(); iterator.next()) {
        byte[] k = iterator.peekNext().getKey();
        // 前缀不匹配则结束遍历
        if (!prefixMatches(base, base.length, k)) {
          break;
        }
        byte[] v = iterator.peekNext().getValue();
        // 加载令牌到状态
        loadTokenData(state, v);
        ++numTokens;
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      IOUtils.cleanupWithLogger(LOG, iterator);
    }
    return numTokens;
  }

  private void loadLatestSequenceNumber(TimelineServiceState state)
      throws IOException {
    byte[] data = null;
    try {
      // 读取最新序列号存储值
      data = db.get(LATEST_SEQUENCE_NUMBER_KEY);
    } catch (DBException e) {
      throw new IOException(e);
    }
    if (data != null) {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
      try {
        // 反序列化并设置到状态对象
        state.latestSequenceNumber = in.readInt();
      } finally {
        IOUtils.cleanupWithLogger(LOG, in);
      }
    }
  }
  /**
   * Creates a domain entity key with column name suffix, of the form
   * TOKEN_ENTRY_PREFIX + sequence number.
   */
  private static byte[] createTokenEntryKey(int seqNum) throws IOException {
    return KeyBuilder.newInstance().add(TOKEN_ENTRY_PREFIX)
        .add(Integer.toString(seqNum)).getBytes();
  }

  /**
   * Creates a domain entity key with column name suffix, of the form
   * TOKEN_MASTER_KEY_ENTRY_PREFIX + sequence number.
   */
  private static byte[] createTokenMasterKeyEntryKey(int keyId)
      throws IOException {
    return KeyBuilder.newInstance().add(TOKEN_MASTER_KEY_ENTRY_PREFIX)
        .add(Integer.toString(keyId)).getBytes();
  }

  /**
   * 从LevelDB加载存储版本信息，供测试使用
   * @return 存储版本信息
   * @throws IOException IO异常
   */
  @VisibleForTesting
  Version loadVersion() throws IOException {
    try {
      byte[] data = db.get(TIMELINE_STATE_STORE_VERSION_KEY);
      // 如果之前没有存储版本，默认返回当前版本
      if (data == null || data.length == 0) {
        return getCurrentVersion();
      }
      Version version =
          new VersionPBImpl(
              YarnServerCommonProtos.VersionProto.parseFrom(data));
      return version;
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  /**
   * 将存储版本信息写入LevelDB，供测试使用
   * @param state 版本信息
   * @throws IOException IO异常
   */
  @VisibleForTesting
  void storeVersion(Version state) throws IOException {
    byte[] data =
        ((VersionPBImpl) state).getProto().toByteArray();
    try {
      db.put(TIMELINE_STATE_STORE_VERSION_KEY, data);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  /**
   * 获取当前存储版本，供测试使用
   * @return 当前版本信息
   */
  @VisibleForTesting
  Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  /**
   * 1) Versioning timeline state store:
   * major.minor. For e.g. 1.0, 1.1, 1.2...1.25, 2.0 etc.
   * 2) Any incompatible change of TS-store is a major upgrade, and any
   * compatible change of TS-store is a minor upgrade.
   * 3) Within a minor upgrade, say 1.1 to 1.2:
   * overwrite the version info and proceed as normal.
   * 4) Within a major upgrade, say 1.2 to 2.0:
   * throw exception and indicate user to use a separate upgrade tool to
   * upgrade timeline store or remove incompatible old state.
   */
  private void checkVersion() throws IOException {
    Version loadedVersion = loadVersion();
    LOG.info("Loaded timeline state store version info " + loadedVersion);
    // 版本完全一致直接返回
    if (loadedVersion.equals(getCurrentVersion())) {
      return;
    }
    // 版本兼容（主版本号相同，次版本号小于等于当前）则更新版本信息后继续
    if (loadedVersion.isCompatibleTo(getCurrentVersion())) {