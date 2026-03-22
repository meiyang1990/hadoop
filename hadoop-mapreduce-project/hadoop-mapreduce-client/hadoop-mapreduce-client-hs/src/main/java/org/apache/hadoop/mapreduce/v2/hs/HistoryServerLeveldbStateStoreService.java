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

package org.apache.hadoop.mapreduce.v2.hs;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于LevelDB实现的MapReduce历史服务器状态存储服务
 * 负责持久化存储MR代理令牌相关状态信息，支持历史服务器恢复时加载已有状态
 */
public class HistoryServerLeveldbStateStoreService extends
    HistoryServerStateStoreService {

  private static final String DB_NAME = "mr-jhs-state";
  private static final String DB_SCHEMA_VERSION_KEY = "jhs-schema-version";
  private static final String TOKEN_MASTER_KEY_KEY_PREFIX = "tokens/key_";
  private static final String TOKEN_STATE_KEY_PREFIX = "tokens/token_";

  private static final Version CURRENT_VERSION_INFO =
      Version.newInstance(1, 0);

  private DB db;

  public static final Logger LOG =
      LoggerFactory.getLogger(HistoryServerLeveldbStateStoreService.class);

  @Override
  protected void initStorage(Configuration conf) throws IOException {
  }

  @Override
  /**
   * 启动LevelDB状态存储，打开或创建数据库，并进行版本检查
   */
  protected void startStorage() throws IOException {
    // 创建存储目录
    Path storeRoot = createStorageDir(getConfig());
    Options options = new Options();
    options.createIfMissing(false);
    LOG.info("Using state database at " + storeRoot + " for recovery");
    File dbfile = new File(storeRoot.toString());
    try {
      // 尝试打开已存在的数据库
      db = JniDBFactory.factory.open(dbfile, options);
    } catch (NativeDB.DBException e) {
      // 数据库不存在则创建新数据库
      if (e.isNotFound() || e.getMessage().contains(" does not exist ")) {
        LOG.info("Creating state database at " + dbfile);
        options.createIfMissing(true);
        try {
          db = JniDBFactory.factory.open(dbfile, options);
          // 存储版本信息
          storeVersion();
        } catch (DBException dbErr) {
          throw new IOException(dbErr.getMessage(), dbErr);
        }
      } else {
          throw e;
      }
    }
    // 检查存储版本兼容性
    checkVersion();
  }

  @Override
  /**
   * 关闭LevelDB存储
   */
  protected void closeStorage() throws IOException {
    if (db != null) {
      db.close();
      db = null;
    }
  }

  @Override
  /**
   * 从LevelDB加载所有状态，恢复历史服务器状态
   * @return 恢复后的历史服务器状态对象
   * @throws IOException IO异常
   */
  public HistoryServerState loadState() throws IOException {
    HistoryServerState state = new HistoryServerState();
    // 加载令牌主密钥
    int numKeys = loadTokenMasterKeys(state);
    LOG.info("Recovered " + numKeys + " token master keys");
    // 加载代理令牌状态
    int numTokens = loadTokens(state);
    LOG.info("Recovered " + numTokens + " tokens");
    return state;
  }

  /**
   * 从LevelDB加载所有令牌主密钥到状态对象
   * @param state 目标状态对象
   * @return 加载的主密钥数量
   * @throws IOException IO异常
   */
  private int loadTokenMasterKeys(HistoryServerState state)
      throws IOException {
    int numKeys = 0;
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      // 定位到第一个令牌主密钥记录
      iter.seek(bytes(TOKEN_MASTER_KEY_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.next();
        String key = asString(entry.getKey());
        // 超出前缀范围停止遍历
        if (!key.startsWith(TOKEN_MASTER_KEY_KEY_PREFIX)) {
          break;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Loading master key from " + key);
        }
        try {
          // 解析并添加主密钥
          loadTokenMasterKey(state, entry.getValue());
        } catch (IOException e) {
          throw new IOException("Error loading token master key from " + key,
              e);
        }
        ++numKeys;
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }
    return numKeys;
  }

  /**
   * 反序列化并添加单个令牌主密钥到状态对象
   * @param state 目标状态对象
   * @param data 序列化后的二进制数据
   * @throws IOException IO异常
   */
  private void loadTokenMasterKey(HistoryServerState state, byte[] data)
      throws IOException {
    DelegationKey key = new DelegationKey();
    DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(data));
    try {
      key.readFields(in);
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    state.tokenMasterKeyState.add(key);
  }

  /**
   * 从LevelDB加载所有代理令牌到状态对象
   * @param state 目标状态对象
   * @return 加载的令牌数量
   * @throws IOException IO异常
   */
  private int loadTokens(HistoryServerState state) throws IOException {
    int numTokens = 0;
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      // 定位到第一个代理令牌记录
      iter.seek(bytes(TOKEN_STATE_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.next();
        String key = asString(entry.getKey());
        // 超出前缀范围停止遍历
        if (!key.startsWith(TOKEN_STATE_KEY_PREFIX)) {
          break;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Loading token from " + key);
        }
        try {
          // 解析并添加令牌
          loadToken(state, entry.getValue());
        } catch (IOException e) {
          throw new IOException("Error loading token state from " + key, e);
        }
        ++numTokens;
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }
    return numTokens;
  }

  /**
   * 反序列化并添加单个代理令牌到状态对象
   * @param state 目标状态对象
   * @param data 序列化后的二进制数据
   * @throws IOException IO异常
   */
  private void loadToken(HistoryServerState state, byte[] data)
      throws IOException {
    MRDelegationTokenIdentifier tokenId = new MRDelegationTokenIdentifier();
    long renewDate;
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
    try {
      tokenId.readFields(in);
      renewDate = in.readLong();
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    state.tokenState.put(tokenId, renewDate);
  }

  @Override
  /**
   * 存储新增的MR代理令牌到LevelDB
   * @param tokenId 令牌标识符
   * @param renewDate 令牌更新时间
   * @throws IOException IO异常
   */
  public void storeToken(MRDelegationTokenIdentifier tokenId, Long renewDate)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing token " + tokenId.getSequenceNumber());
    }

    ByteArrayOutputStream memStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(memStream);
    try {
      tokenId.write(dataStream);
      dataStream.writeLong(renewDate);
      dataStream.close();
      dataStream = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, dataStream);
    }

    String dbKey = getTokenDatabaseKey(tokenId);
    try {
      db.put(bytes(dbKey), memStream.toByteArray());
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  /**
   * 更新MR代理令牌更新时间，复用存储逻辑
   * @param tokenId 令牌标识符
   * @param renewDate 新的更新时间
   * @throws IOException IO异常
   */
  public void updateToken(MRDelegationTokenIdentifier tokenId, Long renewDate)
      throws IOException {
    storeToken(tokenId, renewDate);
  }

  @Override
  /**
   * 从LevelDB删除指定代理令牌
   * @param tokenId 要删除的令牌标识符
   * @throws IOException IO异常
   */
  public void removeToken(MRDelegationTokenIdentifier tokenId)
      throws IOException {
    String dbKey = getTokenDatabaseKey(tokenId);
    try {
      db.delete(bytes(dbKey));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  private String getTokenDatabaseKey(MRDelegationTokenIdentifier tokenId) {
    return TOKEN_STATE_KEY_PREFIX + tokenId.getSequenceNumber();
  }

  @Override
  /**
   * 存储新增的令牌主密钥到LevelDB
   * @param masterKey 要存储的主密钥对象
   * @throws IOException IO异常
   */
  public void storeTokenMasterKey(DelegationKey masterKey)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing master key " + masterKey.getKeyId());
    }

    ByteArrayOutputStream memStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(memStream);
    try {
      masterKey.write(dataStream);
      dataStream.close();
      dataStream = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, dataStream);
    }

    String dbKey = getTokenMasterKeyDatabaseKey(masterKey);
    try {
      db.put(bytes(dbKey), memStream.toByteArray());
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  /**
   * 从LevelDB删除指定令牌主密钥
   * @param masterKey 要删除的主密钥对象
   * @throws IOException IO异常
   */
  public void removeTokenMasterKey(DelegationKey masterKey)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing master key " + masterKey.getKeyId());
    }

    String dbKey = getTokenMasterKeyDatabaseKey(masterKey);
    try {
      db.delete(bytes(dbKey));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  private String getTokenMasterKeyDatabaseKey(DelegationKey masterKey) {
    return TOKEN_MASTER_KEY_KEY_PREFIX + masterKey.getKeyId();
  }

  /**
   * 根据配置创建LevelDB存储目录，设置700权限
   * @param conf 配置对象
   * @return 存储目录路径
   * @throws IOException 未配置路径或创建目录失败抛出异常
   */
  private Path createStorageDir(Configuration conf) throws IOException {
    String confPath = conf.get(JHAdminConfig.MR_HS_LEVELDB_STATE_STORE_PATH);
    if (confPath == null) {
      throw new IOException("No store location directory configured in " +
          JHAdminConfig.MR_HS_LEVELDB_STATE_STORE_PATH);
    }
    Path root = new Path(confPath, DB_NAME);
    FileSystem fs = FileSystem.getLocal(conf);
    fs.mkdirs(root, new FsPermission((short)0700));
    return root;
  }

  /**
   * 从LevelDB加载存储 schema 版本信息
   * @return 加载到的版本，不存在返回默认1.0版本
   * @throws IOException IO异常
   */
  Version loadVersion() throws IOException {
    byte[] data = db.get(bytes(DB_SCHEMA_VERSION_KEY));
    // if version is not stored previously, treat it as 1.0.
    if (data == null || data.length == 0) {
      return Version.newInstance(1, 0);
    }
    Version version =
        new VersionPBImpl(VersionProto.parseFrom(data));
    return version;
  }

  /**
   * 存储当前版本信息到LevelDB
   * @throws IOException IO异常
   */
  private void storeVersion() throws IOException {
    dbStoreVersion(CURRENT_VERSION_INFO);
  }

  /**
   * 将指定版本存储到LevelDB
   * @param version 要存储的版本对象
   * @throws IOException IO异常
   */
  void dbStoreVersion(Version state) throws IOException {
    String key = DB_SCHEMA_VERSION_KEY;
    byte[] data =
        ((VersionPBImpl) state).getProto().toByteArray();
    try {
      db.put(bytes(key), data);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  /**
   * 获取当前存储 schema 版本
   * @return 当前版本对象
   */
  Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  /**
   * 1) Versioning scheme: major.minor. For e.g. 1.0, 1.1, 1.2...1.25, 2.0 etc.
   * 2) Any incompatible change of state-store is a major upgrade, and any
   *    compatible change of state-store is a minor upgrade.
   * 3) Within a minor upgrade, say 1.1 to 1.2:
   *    overwrite the version info and proceed as normal.
   * 4) Within a major upgrade, say 1.2 to 2.0:
   *    throw exception and indicate user to use a separate upgrade tool to
   *    upgrade state or remove incompatible old state.
   */
  /**
   * 检查存储 schema 版本兼容性，不兼容则抛出异常
   * @throws IOException 版本不兼容抛出异常
   */
  private void checkVersion() throws IOException {
    Version loadedVersion = loadVersion();
    LOG.info("Loaded state version info " + loadedVersion);
    if (loadedVersion.equals(getCurrentVersion())) {
      return;
    }
    if (loadedVersion.isCompatibleTo(getCurrentVersion())) {
      LOG.info("Storing state version info " + getCurrentVersion());
      storeVersion();
    } else {
      throw new IOException(
        "Incompatible version for state: expecting state version "
            + getCurrentVersion() + ", but loading version " + loadedVersion);
    }
  }
}