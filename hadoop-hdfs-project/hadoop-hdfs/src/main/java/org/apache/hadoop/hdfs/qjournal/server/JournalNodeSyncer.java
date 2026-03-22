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
package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocolPB.InterQJournalProtocolPB;
import org.apache.hadoop.hdfs.qjournal.protocolPB.InterQJournalProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @fileoverview 日志节点同步器，运行在日志节点生命周期中，定期与其他日志节点同步编辑日志
 * 核心功能：周期性对比其他节点的编辑日志清单，下载本节点缺失的日志段，保证集群中所有日志节点数据一致
 */
@InterfaceAudience.Private
public class JournalNodeSyncer {
  public static final Logger LOG = LoggerFactory.getLogger(
      JournalNodeSyncer.class);
  private final JournalNode jn;
  private final Journal journal;
  private final String jid;
  private  String nameServiceId;
  private final JNStorage jnStorage;
  private final Configuration conf;
  private volatile Daemon syncJournalDaemon;
  private volatile boolean shouldSync = true;

  private List<JournalNodeProxy> otherJNProxies = Lists.newArrayList();
  private int numOtherJNs;
  private int journalNodeIndexForSync = 0;
  private final long journalSyncInterval;
  private final boolean tryFormatting;
  private final int logSegmentTransferTimeout;
  private final DataTransferThrottler throttler;
  private final JournalMetrics metrics;
  private boolean journalSyncerStarted;

  /**
   * 构造日志节点同步器，初始化配置和参数
   * @param jouranlNode 当前日志节点实例
   * @param journal 对应的日志实例
   * @param jid 日志ID
   * @param conf Hadoop配置
   * @param nameServiceId 命名服务ID
   */
  JournalNodeSyncer(JournalNode jouranlNode, Journal journal, String jid,
      Configuration conf, String nameServiceId) {
    this.jn = jouranlNode;
    this.journal = journal;
    this.jid = jid;
    this.nameServiceId = nameServiceId;
    this.jnStorage = journal.getStorage();
    this.conf = conf;
    // 从配置读取同步间隔时间
    journalSyncInterval = conf.getLong(
        DFSConfigKeys.DFS_JOURNALNODE_SYNC_INTERVAL_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_SYNC_INTERVAL_DEFAULT);
    // 从配置读取日志段传输超时时间
    logSegmentTransferTimeout = conf.getInt(
        DFSConfigKeys.DFS_EDIT_LOG_TRANSFER_TIMEOUT_KEY,
        DFSConfigKeys.DFS_EDIT_LOG_TRANSFER_TIMEOUT_DEFAULT);
    // 从配置读取是否允许同步过程中格式化日志
    tryFormatting = conf.getBoolean(
        DFSConfigKeys.DFS_JOURNALNODE_ENABLE_SYNC_FORMAT_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_ENABLE_SYNC_FORMAT_DEFAULT);
    // 初始化带宽限流控制器
    throttler = getThrottler(conf);
    metrics = journal.getMetrics();
    journalSyncerStarted = false;
  }

  /**
   * 停止同步线程，清理临时目录
   */
  void stopSync() {
    shouldSync = false;
    // 删除同步临时目录
    File editsSyncDir = journal.getStorage().getEditsSyncDir();
    if (editsSyncDir.exists()) {
      FileUtil.fullyDelete(editsSyncDir);
    }
    if (syncJournalDaemon != null) {
      syncJournalDaemon.interrupt();
    }
  }

  /**
   * 启动同步器
   * @param nsId 命名空间ID
   */
  public void start(String nsId) {
    if (nsId != null) {
      this.nameServiceId = nsId;
      journal.setTriedJournalSyncerStartedwithnsId(true);
    }
    if (!journalSyncerStarted && getOtherJournalNodeProxies()) {
      LOG.info("Starting SyncJournal daemon for journal " + jid);
      startSyncJournalsDaemon();
      journalSyncerStarted = true;
    }

  }

  /**
   * 获取同步器是否已启动
   * @return 同步器启动状态
   */
  public boolean isJournalSyncerStarted() {
    return journalSyncerStarted;
  }

  /**
   * 创建同步临时目录，用于存放下载中的日志段
   * @return 创建是否成功
   */
  private boolean createEditsSyncDir() {
    File editsSyncDir = journal.getStorage().getEditsSyncDir();
    if (editsSyncDir.exists()) {
      LOG.info(editsSyncDir + " directory already exists.");
      return true;
    }
    return editsSyncDir.mkdir();
  }

  /**
   * 获取其他所有日志节点的代理对象，用于RPC通信
   * @return 是否成功获取至少一个可用代理
   */
  private boolean getOtherJournalNodeProxies() {
    List<InetSocketAddress> otherJournalNodes = getOtherJournalNodeAddrs();
    if (otherJournalNodes == null || otherJournalNodes.isEmpty()) {
      LOG.warn("Other JournalNode addresses not available. Journal Syncing " +
          "cannot be done");
      return false;
    }
    for (InetSocketAddress addr : otherJournalNodes) {
      try {
        otherJNProxies.add(new JournalNodeProxy(addr));
      } catch (IOException e) {
        LOG.warn("Could not add proxy for Journal at addresss " + addr, e);
      }
    }
    // 检查是否至少有一个其他日志节点可用
    if (otherJNProxies.isEmpty()) {
      LOG.error("Cannot sync as there is no other JN available for sync.");
      return false;
    }
    numOtherJNs = otherJNProxies.size();
    return true;
  }

  /**
   * 启动同步守护线程，执行周期性同步逻辑
   */
  private void startSyncJournalsDaemon() {
    syncJournalDaemon = new Daemon(() -> {
      // 等待日志格式化完成再创建同步目录
      while(!journal.isFormatted()) {
        try {
          // 如果日志未格式化，尝试从其他节点获取信息自动格式化
          formatWithSyncer();
          Thread.sleep(journalSyncInterval);
        } catch (InterruptedException e) {
          LOG.error("JournalNodeSyncer daemon received Runtime exception.", e);
          Thread.currentThread().interrupt();
          return;
        }
      }
      if (!createEditsSyncDir()) {
        LOG.error("Failed to create directory for downloading log " +
                "segments: {}. Stopping Journal Node Sync.",
            journal.getStorage().getEditsSyncDir());
        return;
      }
      while(shouldSync) {
        try {
          if (!journal.isFormatted()) {
            LOG.warn("Journal cannot sync. Not formatted. Trying to format with the syncer");
            formatWithSyncer();
            if (journal.isFormatted() && !createEditsSyncDir()) {
              LOG.error("Failed to create directory for downloading log " +
                      "segments: {}. Stopping Journal Node Sync.",
                  journal.getStorage().getEditsSyncDir());
              return;
            }
            continue;
          } else {
            // 执行一次同步
            syncJournals();
          }
        } catch (Throwable t) {
          if (!shouldSync) {
            if (t instanceof InterruptedException) {
              LOG.info("Stopping JournalNode Sync.");
              Thread.currentThread().interrupt();
              return;
            } else {
              LOG.warn("JournalNodeSyncer received an exception while " +
                  "shutting down.", t);
            }
            break;
          } else {
            if (t instanceof InterruptedException) {
              LOG.warn("JournalNodeSyncer interrupted", t);
              Thread.currentThread().interrupt();
              return;
            }
          }
          LOG.error(
              "JournalNodeSyncer daemon received Runtime exception. ", t);
        }
        try {
          // 等待同步间隔后再执行下一次
          Thread.sleep(journalSyncInterval);
        } catch (InterruptedException e) {
          if (!shouldSync) {
            LOG.info("Stopping JournalNode Sync.");
          } else {
            LOG.warn("JournalNodeSyncer interrupted", e);
          }
          Thread.currentThread().interrupt();
          return;
        }
      }
    });
    syncJournalDaemon.start();
  }

  /**
   * 轮询与各个其他日志节点执行同步
   */
  private void syncJournals() {
    syncWithJournalAtIndex(journalNodeIndexForSync);
    // 轮询下一个节点
    journalNodeIndexForSync = (journalNodeIndexForSync + 1) % numOtherJNs;
  }

  /**
   * 从其他节点获取存储信息，自动格式化当前未格式化的日志
   */
  private void formatWithSyncer() {
    if (!tryFormatting) {
      return;
    }
    LOG.info("Trying to format the journal with the syncer");
    try {
      StorageInfo storage = null;
      // 遍历所有其他节点，尝试获取有效的存储信息
      for (JournalNodeProxy jnProxy : otherJNProxies) {
        // 跳过还没有编辑日志的节点，避免和NameNode格式化竞争
        if (!hasEditLogs(jnProxy)) {
          continue;
        }
        try {
          // 从目标节点获取存储信息
          HdfsServerProtos.StorageInfoProto storageInfoResponse =
              jnProxy.jnProxy.getStorageInfo(jid, nameServiceId);
          storage = PBHelper.convert(
              storageInfoResponse, HdfsServerConstants.NodeType.JOURNAL_NODE
          );
          if (storage.getNamespaceID() == 0) {
            LOG.error("Got invalid StorageInfo from " + jnProxy);
            storage = null;
            continue;
          }
          LOG.info("Got StorageInfo " + storage + " from " + jnProxy);
          break;
        } catch (IOException e) {
          LOG.error("Could not get StorageInfo from " + jnProxy, e);
        }
      }
      if (storage == null) {
        LOG.error("Could not get StorageInfo from any JournalNode. " +
            "JournalNodeSyncer cannot format the journal.");
        return;
      }
      // 使用获取到的命名空间信息格式化当前日志
      NamespaceInfo nsInfo = new NamespaceInfo(storage);
      journal.format(nsInfo, true);
    } catch (IOException e) {
      LOG.error("Exception in formatting the journal with the syncer", e);
    }
  }

  /**
   * 检查目标日志节点是否已经存在编辑日志
   * @param journalProxy 目标节点代理
   * @return 是否存在编辑日志
   */
  private boolean hasEditLogs(JournalNodeProxy journalProxy) {
    GetEditLogManifestResponseProto editLogManifest;
    try {
      editLogManifest = journalProxy.jnProxy.getEditLogManifestFromJournal(
          jid, nameServiceId, 0, false);
    } catch (IOException e) {
      LOG.error("Could not get edit log manifest from " + journalProxy, e);
      return false;
    }

    List<RemoteEditLog> otherJournalEditLogs = PBHelper.convert(
        editLogManifest.getManifest()).getLogs();
    if (otherJournalEditLogs == null || otherJournalEditLogs.isEmpty()) {
      LOG.warn("Journal at " + journalProxy.jnAddr + " has no edit logs");
      return false;
    }

    return true;
  }

  /**
   * 与指定索引对应的日志节点执行同步
   * @param index 目标节点在列表中的索引
   */
  private void syncWithJournalAtIndex(int index) {
    LOG.info("Syncing Journal " + jn.getBoundIpcAddress().getAddress() + ":"
        + jn.getBoundIpcAddress().getPort() + " with "
        + otherJNProxies.get(index) + ", journal id: " + jid);
    final InterQJournalProtocol jnProxy = otherJNProxies.get(index).jnProxy;
    if (jnProxy == null) {
      LOG.error("JournalNode Proxy not found.");
      return;
    }

    // 获取本节点当前的编辑日志清单
    List<RemoteEditLog> thisJournalEditLogs;
    try {
      thisJournalEditLogs = journal.getEditLogManifest(0, false).getLogs();
    } catch (IOException e) {
      LOG.error("Exception in getting local edit log manifest", e);
      return;
    }

    // 获取目标节点的编辑日志清单
    GetEditLogManifestResponseProto editLogManifest;
    try {
      editLogManifest = jnProxy.getEditLogManifestFromJournal(jid,
          nameServiceId, 0, false);
    } catch (IOException e) {
      LOG.debug("Could not sync with Journal at {}.",
          otherJNProxies.get(journalNodeIndexForSync), e);
      return;
    }

    // 对比并下载缺失的日志段
    getMissingLogSegments(thisJournalEditLogs, editLogManifest,
        otherJNProxies.get(index));
  }

  /**
   * 从配置中解析出所有其他日志节点的地址列表
   * @return 其他日志节点地址列表，解析失败返回null
   */
  private List<InetSocketAddress> getOtherJournalNodeAddrs() {
    String uriStr = "";
    try {
      // 读取共享编辑目录配置
      uriStr = conf.getTrimmed(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY);

      if (uriStr == null || uriStr.isEmpty()) {
        if (nameServiceId != null) {
          // 按命名服务ID读取配置
          uriStr = conf.getTrimmed(DFSConfigKeys
              .DFS_NAMENODE_SHARED_EDITS_DIR_KEY + "." + nameServiceId);
        }
      }

      if (uriStr == null || uriStr.isEmpty()) {
        HashSet<String> sharedEditsUri = new HashSet<>();
        if (nameServiceId != null) {
          // 兼容HA配置，遍历所有NameNode ID读取
          Collection<String> nnIds = DFSUtilClient.getNameNodeIds(
              conf, nameServiceId);
          for (String nnId : nnIds) {
            String suffix = nameServiceId + "." + nnId;
            uriStr = conf.getTrimmed(DFSConfigKeys
                .DFS_NAMENODE_SHARED_EDITS_DIR_KEY + "." + suffix);
            sharedEditsUri.add(uriStr);
          }
          if (sharedEditsUri.size() > 1) {
            uriStr = null;
            LOG.error("The conf property " + DFSConfigKeys
                .DFS_NAMENODE_SHARED_EDITS_DIR_KEY + " not set properly, " +
                "it has been configured with different journalnode values " +
                sharedEditsUri.toString() + " for a" +
                " single nameserviceId" + nameServiceId);
          }
        }
      }

      if (uriStr == null || uriStr.isEmpty()) {
        LOG.error("Could not construct Shared Edits Uri");