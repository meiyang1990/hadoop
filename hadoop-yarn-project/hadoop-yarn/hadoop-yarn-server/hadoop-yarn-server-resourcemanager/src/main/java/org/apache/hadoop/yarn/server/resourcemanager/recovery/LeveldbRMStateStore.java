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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.yarn.server.resourcemanager.DBManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.Epoch;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMDelegationTokenIdentifierData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.AMRMTokenSecretManagerStatePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 基于LevelDB实现的ResourceManager状态存储，用于RM故障恢复。
 * 版本变更：1.0 -> 1.1 新增了ReservationSystem状态存储
 */
public class LeveldbRMStateStore extends RMStateStore {

  public static final Logger LOG =
      LoggerFactory.getLogger(LeveldbRMStateStore.class);

  private static final String SEPARATOR = "/";
  private static final String DB_NAME = "yarn-rm-state";
  private static final String RM_DT_MASTER_KEY_KEY_PREFIX =
      RM_DT_SECRET_MANAGER_ROOT + SEPARATOR + DELEGATION_KEY_PREFIX;
  private static final String RM_DT_TOKEN_KEY_PREFIX =
      RM_DT_SECRET_MANAGER_ROOT + SEPARATOR + DELEGATION_TOKEN_PREFIX;
  private static final String RM_DT_SEQUENCE_NUMBER_KEY =
      RM_DT_SECRET_MANAGER_ROOT + SEPARATOR + "RMDTSequentialNumber";
  private static final String RM_APP_KEY_PREFIX =
      RM_APP_ROOT + SEPARATOR + ApplicationId.appIdStrPrefix;
  private static final String RM_RESERVATION_KEY_PREFIX =
      RESERVATION_SYSTEM_ROOT + SEPARATOR;

  private static final Version CURRENT_VERSION_INFO = Version
      .newInstance(1, 1);

  private DB db;
  private DBManager dbManager = new DBManager();
  private long compactionIntervalMsec;

  private String getApplicationNodeKey(ApplicationId appId) {
    return RM_APP_ROOT + SEPARATOR + appId;
  }

  private String getApplicationAttemptNodeKey(ApplicationAttemptId attemptId) {
    return getApplicationAttemptNodeKey(
        getApplicationNodeKey(attemptId.getApplicationId()), attemptId);
  }

  private String getApplicationAttemptNodeKey(String appNodeKey,
      ApplicationAttemptId attemptId) {
    return appNodeKey + SEPARATOR + attemptId;
  }

  private String getRMDTMasterKeyNodeKey(DelegationKey masterKey) {
    return RM_DT_MASTER_KEY_KEY_PREFIX + masterKey.getKeyId();
  }

  private String getRMDTTokenNodeKey(RMDelegationTokenIdentifier tokenId) {
    return RM_DT_TOKEN_KEY_PREFIX + tokenId.getSequenceNumber();
  }

  private String getReservationNodeKey(String planName,
      String reservationId) {
    return RESERVATION_SYSTEM_ROOT + SEPARATOR + planName + SEPARATOR
        + reservationId;
  }

  private String getProxyCACertNodeKey() {
    return PROXY_CA_ROOT + SEPARATOR + PROXY_CA_CERT_NODE;
  }

  private String getProxyCAPrivateKeyNodeKey() {
    return PROXY_CA_ROOT + SEPARATOR + PROXY_CA_PRIVATE_KEY_NODE;
  }

  @Override
  protected void initInternal(Configuration conf) {
    // 从配置读取LevelDB压缩间隔，转换为毫秒
    compactionIntervalMsec = conf.getLong(
        YarnConfiguration.RM_LEVELDB_COMPACTION_INTERVAL_SECS,
        YarnConfiguration.DEFAULT_RM_LEVELDB_COMPACTION_INTERVAL_SECS) * 1000;
  }

  private Path getStorageDir() throws IOException {
    Configuration conf = getConfig();
    String storePath = conf.get(YarnConfiguration.RM_LEVELDB_STORE_PATH);
    if (storePath == null) {
      throw new IOException("No store location directory configured in " +
          YarnConfiguration.RM_LEVELDB_STORE_PATH);
    }
    // 拼接LevelDB文件路径
    return new Path(storePath, DB_NAME);
  }

  private Path createStorageDir() throws IOException {
    Path root = getStorageDir();
    FileSystem fs = FileSystem.getLocal(getConfig());
    // 创建存储目录，权限设置为700
    fs.mkdirs(root, new FsPermission((short)0700));
    return root;
  }

  @Override
  protected void startInternal() throws Exception {
    Path storeRoot = createStorageDir();
    Options options = new Options();
    // 若数据库不存在则不创建，后续初始化版本会处理
    options.createIfMissing(false);
    LOG.info("Using state database at " + storeRoot + " for recovery");
    File dbfile = new File(storeRoot.toString());
    // 初始化LevelDB，若为新数据库则存储版本信息
    db = dbManager.initDatabase(dbfile, options, (database) ->
        storeVersion(CURRENT_VERSION_INFO));
    // 启动定期压缩任务，清理删除数据产生的垃圾
    dbManager.startCompactionTimer(compactionIntervalMsec,
        this.getClass().getSimpleName());
  }

  @Override
  protected void closeInternal() throws Exception {
    dbManager.close();
  }

  @VisibleForTesting
  boolean isClosed() {
    return db == null;
  }

  @VisibleForTesting
  DB getDatabase() {
    return db;
  }

  @Override
  protected Version loadVersion() throws Exception {
    // 从数据库加载存储版本信息
    return dbManager.loadVersion(VERSION_NODE);
  }

  @Override
  protected void storeVersion() throws Exception {
    try {
      storeVersion(CURRENT_VERSION_INFO);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  protected void storeVersion(Version version) {
    // 将版本信息存储到数据库
    dbManager.storeVersion(VERSION_NODE, version);
  }

  @Override
  protected Version getCurrentVersion() {
    // 返回当前存储版本
    return CURRENT_VERSION_INFO;
  }

  @Override
  public synchronized long getAndIncrementEpoch() throws Exception {
    long currentEpoch = baseEpoch;
    byte[] dbKeyBytes = bytes(EPOCH_NODE);
    try {
      byte[] data = db.get(dbKeyBytes);
      // 从数据库读取已有epoch值
      if (data != null) {
        currentEpoch = EpochProto.parseFrom(data).getEpoch();
      }
      // 存储自增后的新epoch
      EpochProto proto = Epoch.newInstance(nextEpoch(currentEpoch)).getProto();
      db.put(dbKeyBytes, proto.toByteArray());
    } catch (DBException e) {
      throw new IOException(e);
    }
    return currentEpoch;
  }

  @Override
  public RMState loadState() throws Exception {
    RMState rmState = new RMState();
     // 加载RM委派令牌密钥管理器状态
     loadRMDTSecretManagerState(rmState);
     // 加载应用程序状态
     loadRMApps(rmState);
     // 加载AM-RM令牌密钥管理器状态
     loadAMRMTokenSecretManagerState(rmState);
    // 加载 Reservation 系统状态
    loadReservationState(rmState);
    // 加载代理CA证书状态
    loadProxyCAManagerState(rmState);
    return rmState;
   }

  private void loadReservationState(RMState rmState) throws IOException {
    int numReservations = 0;
    // 遍历所有Reservation前缀的键值对
    try (LeveldbIterator iter = new LeveldbIterator(db)) {
      iter.seek(bytes(RM_RESERVATION_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.next();
        String key = asString(entry.getKey());
        // 超出Reservation前缀范围，停止遍历
        if (!key.startsWith(RM_RESERVATION_KEY_PREFIX)) {
          break;
        }

        // 解析计划名和ReservationID
        String planReservationString =
            key.substring(RM_RESERVATION_KEY_PREFIX.length());
        String[] parts = planReservationString.split(SEPARATOR);
        if (parts.length != 2) {
          LOG.warn("Incorrect reservation state key " + key);
          continue;
        }
        String planName = parts[0];
        String reservationName = parts[1];
        // 反序列化Reservation状态
        ReservationAllocationStateProto allocationState =
            ReservationAllocationStateProto.parseFrom(entry.getValue());
        // 初始化计划对应map
        if (!rmState.getReservationState().containsKey(planName)) {
          rmState.getReservationState().put(planName,
              new HashMap<ReservationId, ReservationAllocationStateProto>());
        }
        ReservationId reservationId =
            ReservationId.parseReservationId(reservationName);
        rmState.getReservationState().get(planName).put(reservationId,
            allocationState);
        numReservations++;
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
    LOG.info("Recovered " + numReservations + " reservations");
  }

  private void loadRMDTSecretManagerState(RMState state) throws IOException {
    // 加载主密钥
    int numKeys = loadRMDTSecretManagerKeys(state);
    LOG.info("Recovered " + numKeys + " RM delegation token master keys");
    // 加载委派令牌
    int numTokens = loadRMDTSecretManagerTokens(state);
    LOG.info("Recovered " + numTokens + " RM delegation tokens");
    // 加载令牌序列号
    loadRMDTSecretManagerTokenSequenceNumber(state);
  }

  private int loadRMDTSecretManagerKeys(RMState state) throws IOException {
    int numKeys = 0;
    // 遍历所有主密钥前缀的键值对
    try (LeveldbIterator iter = new LeveldbIterator(db)) {
      iter.seek(bytes(RM_DT_MASTER_KEY_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.next();
        String key = asString(entry.getKey());
        // 超出主密钥前缀范围，停止遍历
        if (!key.startsWith(RM_DT_MASTER_KEY_KEY_PREFIX)) {
          break;
        }
        // 反序列化并加载主密钥
        DelegationKey masterKey = loadDelegationKey(entry.getValue());
        state.rmSecretManagerState.masterKeyState.add(masterKey);
        ++numKeys;
        LOG.debug("Loaded RM delegation key from {}: keyId={},"
            + " expirationDate={}", key, masterKey.getKeyId(),
            masterKey.getExpiryDate());
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
    return numKeys;
  }

  private DelegationKey loadDelegationKey(byte[] data) throws IOException {
    DelegationKey key = new DelegationKey();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
    try {
      key.readFields(in);
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    return key;
  }

  private int loadRMDTSecretManagerTokens(RMState state) throws IOException {
    int numTokens = 0;
    // 遍历所有委派令牌前缀的键值对
    try (LeveldbIterator iter = new LeveldbIterator(db)) {
      iter.seek(bytes(RM_DT_TOKEN_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.next();
        String key = asString(entry.getKey());
        // 超出委派令牌前缀范围，停止遍历
        if (!key.startsWith(RM_DT_TOKEN_KEY_PREFIX)) {
          break;
        }
        // 反序列化并加载委派令牌
        RMDelegationTokenIdentifierData tokenData = loadDelegationToken(
            entry.getValue());
        RMDelegationTokenIdentifier tokenId = tokenData.getTokenIdentifier();
        long renewDate = tokenData.getRenewDate();
        state.rmSecretManagerState.delegationTokenState.put(tokenId,
            renewDate);
        ++numTokens;
        LOG.debug("Loaded RM delegation token from {}: tokenId={},"
            + " renewDate={}", key, tokenId, renewDate);
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
    return numTokens;
  }

  private RMDelegationTokenIdentifierData loadDelegationToken(byte[] data)
      throws IOException {
    RMDelegationTokenIdentifierData tokenData;
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
    try {
      tokenData = RMStateStoreUtils.readRMDelegationTokenIdentifierData(in);
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    return tokenData;
  }

  private void loadRMDTSecretManagerTokenSequenceNumber(RMState state)
      throws IOException {
    byte[] data;
    try {
      // 读取令牌序列号
      data = db.get(bytes(RM_DT_SEQUENCE_NUMBER_KEY));
    } catch (DBException e) {
      throw new IOException(e);
    }
    if (data != null) {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
      try {
        // 设置序列号到恢复状态
        state.rmSecretManagerState.dtSequenceNumber = in.readInt();
      } finally {
        IOUtils.cleanupWithLogger(LOG, in);
      }
    }
  }

  private void loadRMApps(RMState state) throws IOException {
    int numApps = 0;
    int numAppAttempts = 0;
    // 遍历所有应用前缀的键值对
    try (LeveldbIterator iter = new LeveldbIterator(db)) {
      iter.seek(bytes(RM_APP_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.next();
        String key = asString(entry.getKey());
        // 超出应用前缀范围，停止遍历
        if (!key.startsWith(RM_APP_KEY_PREFIX)) {
          break;
        }

        // 提取应用ID字符串
        String appIdStr = key.substring(RM_APP_ROOT.length() + 1);
        // 包含分隔符说明不是应用根节点，跳过
        if (appIdStr.contains(SEPARATOR)) {
          LOG.warn("Skipping extraneous data " + key);
          continue;
        }

        // 加载应用及其所有尝试
        numAppAttempts += loadRMApp(state, iter, appIdStr, entry.getValue());