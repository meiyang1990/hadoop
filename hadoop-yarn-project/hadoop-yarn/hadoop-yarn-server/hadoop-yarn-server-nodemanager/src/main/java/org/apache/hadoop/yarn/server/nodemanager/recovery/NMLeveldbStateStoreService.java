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

package org.apache.hadoop.yarn.server.nodemanager.recovery;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.ContainerTokenIdentifierProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LogDeleterProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerRequestProto;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

/**
 * 基于LevelDB实现的NodeManager状态持久化存储服务，用于NM重启时恢复状态
 */
public class NMLeveldbStateStoreService extends NMStateStoreService {

  public static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(NMLeveldbStateStoreService.class);

  private static final String DB_NAME = "yarn-nm-state";
  private static final String DB_SCHEMA_VERSION_KEY = "nm-schema-version";

  /**
   * Changes from 1.0 to 1.1: Save AMRMProxy state in NMSS.
   * Changes from 1.1 to 1.2: Save queued container information.
   */
  private static final Version CURRENT_VERSION_INFO = Version.newInstance(1, 2);

  private static final String DELETION_TASK_KEY_PREFIX =
      "DeletionService/deltask_";

  private static final String APPLICATIONS_KEY_PREFIX =
      "ContainerManager/applications/";
  @Deprecated
  private static final String FINISHED_APPS_KEY_PREFIX =
      "ContainerManager/finishedApps/";

  private static final String LOCALIZATION_KEY_PREFIX = "Localization/";
  private static final String LOCALIZATION_PUBLIC_KEY_PREFIX =
      LOCALIZATION_KEY_PREFIX + "public/";
  private static final String LOCALIZATION_PRIVATE_KEY_PREFIX =
      LOCALIZATION_KEY_PREFIX + "private/";
  private static final String LOCALIZATION_STARTED_SUFFIX = "started/";
  private static final String LOCALIZATION_COMPLETED_SUFFIX = "completed/";
  private static final String LOCALIZATION_FILECACHE_SUFFIX = "filecache/";
  private static final String LOCALIZATION_APPCACHE_SUFFIX = "appcache/";

  private static final String CONTAINERS_KEY_PREFIX =
      "ContainerManager/containers/";
  private static final String CONTAINER_REQUEST_KEY_SUFFIX = "/request";
  private static final String CONTAINER_VERSION_KEY_SUFFIX = "/version";
  private static final String CONTAINER_START_TIME_KEY_SUFFIX = "/starttime";
  private static final String CONTAINER_DIAGS_KEY_SUFFIX = "/diagnostics";
  private static final String CONTAINER_LAUNCHED_KEY_SUFFIX = "/launched";
  private static final String CONTAINER_QUEUED_KEY_SUFFIX = "/queued";
  private static final String CONTAINER_PAUSED_KEY_SUFFIX = "/paused";
  private static final String CONTAINER_UPDATE_TOKEN_SUFFIX =
      "/updateToken";
  private static final String CONTAINER_KILLED_KEY_SUFFIX = "/killed";
  private static final String CONTAINER_EXIT_CODE_KEY_SUFFIX = "/exitcode";
  private static final String CONTAINER_REMAIN_RETRIES_KEY_SUFFIX =
      "/remainingRetryAttempts";
  private static final String CONTAINER_RESTART_TIMES_SUFFIX =
      "/restartTimes";
  private static final String CONTAINER_WORK_DIR_KEY_SUFFIX = "/workdir";
  private static final String CONTAINER_LOG_DIR_KEY_SUFFIX = "/logdir";

  private static final String CURRENT_MASTER_KEY_SUFFIX = "CurrentMasterKey";
  private static final String PREV_MASTER_KEY_SUFFIX = "PreviousMasterKey";
  private static final String NEXT_MASTER_KEY_SUFFIX = "NextMasterKey";
  private static final String NM_TOKENS_KEY_PREFIX = "NMTokens/";
  private static final String NM_TOKENS_CURRENT_MASTER_KEY =
      NM_TOKENS_KEY_PREFIX + CURRENT_MASTER_KEY_SUFFIX;
  private static final String NM_TOKENS_PREV_MASTER_KEY =
      NM_TOKENS_KEY_PREFIX + PREV_MASTER_KEY_SUFFIX;
  private static final String CONTAINER_TOKENS_KEY_PREFIX =
      "ContainerTokens/";
  private static final String CONTAINER_TOKEN_SECRETMANAGER_CURRENT_MASTER_KEY =
      CONTAINER_TOKENS_KEY_PREFIX + CURRENT_MASTER_KEY_SUFFIX;
  private static final String CONTAINER_TOKEN_SECRETMANAGER_PREV_MASTER_KEY =
      CONTAINER_TOKENS_KEY_PREFIX + PREV_MASTER_KEY_SUFFIX;

  private static final String LOG_DELETER_KEY_PREFIX = "LogDeleters/";

  private static final String AMRMPROXY_KEY_PREFIX = "AMRMProxy/";

  /**
   * The Local Tracker State DB key locations - "completed" and "started".
   * To seek through app tracker states in RecoveredUserResources
   * we need to move from one app tracker state to another using key "zzz".
   * zzz comes later in lexicographical order than started.
   * Similarly to move one user to another in RLS,we can use "zzz",
   * as RecoveredUserResources uses two keys appcache and filecache.
   */
  private static final String BEYOND_ENTRIES_SUFFIX = "zzz/";

  private static final String CONTAINER_ASSIGNED_RESOURCES_KEY_SUFFIX =
      "/assignedResources_";

  private static final byte[] EMPTY_VALUE = new byte[0];

  /** LevelDB实例 */
  private DB db;
  /** 标记是否是新建的数据库 */
  private boolean isNewlyCreated;
  /** 存储是否健康 */
  private boolean isHealthy;
  /** 定时压缩数据库定时器 */
  private Timer compactionTimer;

  /**
   * Map of containerID vs List of unknown key suffixes.
   */
  private ListMultimap<ContainerId, String> containerUnknownKeySuffixes =
      ArrayListMultimap.create();

  public NMLeveldbStateStoreService() {
    super(NMLeveldbStateStoreService.class.getName());
  }

  @Override
  protected void startStorage() throws IOException {
    // 启动时默认标记存储为健康
    isHealthy = true;
  }

  @Override
  protected void closeStorage() throws IOException {
    if (compactionTimer != null) {
      compactionTimer.cancel();
      compactionTimer = null;
    }
    if (db != null) {
      db.close();
    }
  }

  @Override
  public boolean isNewlyCreated() {
    return isNewlyCreated;
  }

  /**
   * If the state store throws an error after recovery has been performed
   * then we can not trust it any more to reflect the NM state. We need to
   * mark the store and node unhealthy.
   * Errors during the recovery will cause a service failure and thus a NM
   * start failure. Do not need to mark the store unhealthy for those.
   * @param dbErr Exception
   * 标记存储为不健康，并通知ResourceManager节点异常
   */
  private void markStoreUnHealthy(DBException dbErr) {
    // Always log the error here, we might not see the error in the caller
    LOG.error("Statestore exception: ", dbErr);
    // We have already been marked unhealthy so no need to do it again.
    if (!isHealthy) {
      return;
    }
    // Mark unhealthy, an out of band heartbeat will be sent and the state
    // will remain unhealthy (not recoverable).
    // No need to close the store: does not make any difference at this point.
    isHealthy = false;
    // We could get here before the nodeStatusUpdater is set
    NodeStatusUpdater nsu = getNodeStatusUpdater();
    if (nsu != null) {
      nsu.reportException(dbErr);
    }
  }

  @VisibleForTesting
  boolean isHealthy() {
    return isHealthy;
  }

  // 获取从指定key开始的LevelDB迭代器
  private LeveldbIterator getLevelDBIterator(String startKey)
      throws IOException {
    try {
      LeveldbIterator it = new LeveldbIterator(db);
      it.seek(bytes(startKey));
      return it;
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  // 基础恢复迭代器抽象类
  private abstract class BaseRecoveryIterator<T> implements
      RecoveryIterator<T> {
    LeveldbIterator it;
    T nextItem;

    BaseRecoveryIterator(String dbKey) throws IOException {
      this.it = getLevelDBIterator(dbKey);
      this.nextItem = null;
    }

    protected abstract T getNextItem(LeveldbIterator it) throws IOException;

    @Override
    public boolean hasNext() throws IOException {
      if (nextItem == null) {
        nextItem = getNextItem(it);
      }
      return (nextItem != null);
    }

    @Override
    public T next() throws IOException, NoSuchElementException {
      T tmp = nextItem;
      if (tmp != null) {
        nextItem = null;
        return tmp;
      } else {
        tmp = getNextItem(it);
        if (tmp == null) {
          throw new NoSuchElementException();
        }
        return tmp;
      }
    }

    @Override
    public void close() throws IOException {
      if (it != null) {
        it.close();
      }
    }
  }

  //  容器状态恢复迭代器
  private class ContainerStateIterator extends
      BaseRecoveryIterator<RecoveredContainerState> {
    ContainerStateIterator() throws IOException {
      super(CONTAINERS_KEY_PREFIX);
    }

    @Override
    protected RecoveredContainerState getNextItem(LeveldbIterator it)
        throws IOException {
      return getNextRecoveredContainer(it);
    }
  }

  /**
   * 从迭代器获取下一个待恢复的容器状态
   */
  private RecoveredContainerState getNextRecoveredContainer(LeveldbIterator it)
      throws IOException {
    RecoveredContainerState rcs = null;
    try {
      while (it.hasNext()) {
        Entry<byte[], byte[]> entry = it.peekNext();
        String key = asString(entry.getKey());
        if (!key.startsWith(CONTAINERS_KEY_PREFIX)) {
          return null;
        }

        int idEndPos = key.indexOf('/', CONTAINERS_KEY_PREFIX.length());
        if (idEndPos < 0) {
          throw new IOException("Unable to determine container in key: " + key);
        }
        String keyPrefix = key.substring(0, idEndPos + 1);
        rcs = loadContainerState(it, keyPrefix);
        if (rcs.startRequest != null) {
          break;
        } else {
          removeContainer(rcs.getContainerId());
          rcs = null;
        }
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
    return rcs;
  }


  @Override
  public RecoveryIterator<RecoveredContainerState> getContainerStateIterator()
      throws IOException {
    return new ContainerStateIterator();
  }

  /**
   * 从LevelDB加载指定容器的完整状态
   */
  private RecoveredContainerState loadContainerState(LeveldbIterator iter,
       String keyPrefix) throws IOException {
    ContainerId containerId = ContainerId.fromString(
        keyPrefix.substring(CONTAINERS_KEY_PREFIX.length(),
            keyPrefix.length()-1));
    RecoveredContainerState rcs = new RecoveredContainerState(containerId);
    rcs.status = RecoveredContainerStatus.REQUESTED;
    while (iter.hasNext()) {
      Entry<byte[],byte[]> entry = iter.peekNext();
      String key = asString(entry.getKey());
      if (!key.startsWith(keyPrefix)) {
        break;
      }
      iter.next();

      String suffix = key.substring(keyPrefix.length()-1);  // start with '/'
      if (suffix.equals(CONTAINER_REQUEST_KEY_SUFFIX)) {
        // 解析容器启动请求
        rcs.startRequest = new StartContainerRequestPBImpl(
            StartContainerRequestProto.parseFrom(entry.getValue()));
        ContainerTokenIdentifier containerTokenIdentifier = BuilderUtils
            .newContainerTokenIdentifier(rcs.startRequest.getContainerToken());
        rcs.capability = new ResourcePBImpl(
            containerTokenIdentifier.getProto().getResource());
      } else if (suffix.equals(CONTAINER_VERSION_KEY_SUFFIX)) {
        // 解析容器版本
        rcs.version = Integer.parseInt(asString(entry.getValue()));
      } else if (suffix.equals(CONTAINER_START_TIME_KEY_SUFFIX)) {
        // 解析容器启动时间
        rcs.setStartTime(Long.parseLong(asString(entry.getValue())));
      } else if (suffix.equals(CONTAINER_DIAGS_KEY_SUFFIX)) {
        // 解析容器诊断信息
        rcs.diagnostics = asString(entry.getValue());
      } else if (suffix.equals(CONTAINER_QUEUED_KEY_SUFFIX)) {
        // 更新容器状态为排队
        if (rcs.status == RecoveredContainerStatus.REQUESTED) {
          rcs.status = RecoveredContainerStatus.QUEUED;
        }
      } else if (suffix.equals(CONTAINER_PAUSED_KEY_SUFFIX)) {
        // 更新容器状态为暂停
        if ((rcs.status == RecoveredContainerStatus.LAUNCHED)
            ||(rcs.status == RecoveredContainerStatus.QUEUED)
            ||(rcs.status == RecoveredContainerStatus.REQUESTED)) {
          rcs.status = RecoveredContainerStatus.PAUSED;