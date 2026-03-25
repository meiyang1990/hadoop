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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

/**
 * 文件级注释：JournalNode节点的存储管理实现，继承自Storage基础类，负责管理单个命名空间的元数据存储目录，
 * 提供编辑日志文件定位、格式清理、paxos状态存储、存储目录初始化检查等核心存储能力。
 * 
 * A {@link Storage} implementation for the {@link JournalNode}.
 * 
 * The JN has a storage directory for each namespace for which it stores
 * metadata. There is only a single directory per JN in the current design.
 */
class JNStorage extends Storage {

  private final FileJournalManager fjm;
  private final StorageDirectory sd;
  private StorageState state;

  // Paxos目录清理使用的正则表达式列表，用于提取文件中存储的事务ID
  private static final List<Pattern> PAXOS_DIR_PURGE_REGEXES =
      ImmutableList.of(Pattern.compile("(\\d+)"));

  // 同步编辑日志临时目录名称
  private static final String STORAGE_EDITS_SYNC = "edits.sync";

  /**
   * 构造JNStorage实例，初始化存储目录和日志管理器，并进行存储分析与恢复。
   * @param conf Hadoop配置对象
   * @param logDir 数据存储目录路径
   * @param startOpt 启动选项，标识启动场景
   * @param errorReporter 错误上报回调
   * @throws IOException 存储初始化失败时抛出IO异常
   */
  protected JNStorage(Configuration conf, File logDir, StartupOption startOpt,
      StorageErrorReporter errorReporter) throws IOException {
    super(NodeType.JOURNAL_NODE);
    
    sd = new StorageDirectory(logDir, null, false, new FsPermission(conf.get(
        DFSConfigKeys.DFS_JOURNAL_EDITS_DIR_PERMISSION_KEY,
        DFSConfigKeys.DFS_JOURNAL_EDITS_DIR_PERMISSION_DEFAULT)));
    this.addStorageDir(sd);
    this.fjm = new FileJournalManager(conf, sd, errorReporter);

    analyzeAndRecoverStorage(startOpt);
  }
  
  /**
   * 获取当前存储对应的文件日志管理器。
   * @return 文件日志管理器实例
   */
  FileJournalManager getJournalManager() {
    return fjm;
  }

  @Override
  public boolean isPreUpgradableLayout(StorageDirectory sd)
      throws IOException {
    return false;
  }

  /**
   * 查找覆盖指定事务ID范围的已完成编辑日志文件，找不到则抛出异常。
   * @param startTxId 起始事务ID
   * @param endTxId 结束事务ID
   * @return 匹配的编辑日志文件对象
   * @throws IOException 文件不存在时抛出IO异常
   */
  File findFinalizedEditsFile(long startTxId, long endTxId)
      throws IOException {
    File ret = new File(sd.getCurrentDir(),
        NNStorage.getFinalizedEditsFileName(startTxId, endTxId));
    if (!ret.exists()) {
      throw new IOException(
          "No edits file for range " + startTxId + "-" + endTxId);
    }
    return ret;
  }

  /**
   * 获取从指定事务ID开始的进行中编辑日志文件路径，不检查文件是否存在。
   * @param startTxId 起始事务ID
   * @return 进行中编辑日志文件对象
   */
  File getInProgressEditLog(long startTxId) {
    return new File(sd.getCurrentDir(),
        NNStorage.getInProgressEditsFileName(startTxId));
  }
  
  /**
   * 获取从远程JournalNode同步日志时使用的临时文件路径。
   * @param segmentTxId 日志段的起始事务ID
   * @param epoch 协调恢复的写入者的世代号
   * @return 临时文件路径对象
   */
  File getSyncLogTemporaryFile(long segmentTxId, long epoch) {
    String name = NNStorage.getInProgressEditsFileName(segmentTxId) +
        ".epoch=" + epoch; 
    return new File(sd.getCurrentDir(), name);
  }

  /**
   * Directory {@code edits.sync} temporarily holds the log segments
   * downloaded through {@link JournalNodeSyncer} before they are moved to
   * {@code current} directory.
   *
   * @return the directory path
   */
  File getEditsSyncDir() {
    return new File(sd.getRoot(), STORAGE_EDITS_SYNC);
  }

  /**
   * 获取同步过程中使用的临时编辑文件路径。
   * @param startTxId 起始事务ID
   * @param endTxId 结束事务ID
   * @return 临时文件对象
   */
  File getTemporaryEditsFile(long startTxId, long endTxId) {
    return new File(getEditsSyncDir(), String.format("%s_%019d-%019d",
            NNStorage.NameNodeFile.EDITS.getName(), startTxId, endTxId));
  }

  /**
   * 获取指定事务范围的已完成编辑日志文件路径。
   * @param startTxId 起始事务ID
   * @param endTxId 结束事务ID
   * @return 已完成编辑日志文件对象
   */
  File getFinalizedEditsFile(long startTxId, long endTxId) {
    return new File(sd.getCurrentDir(), String.format("%s_%019d-%019d",
            NNStorage.NameNodeFile.EDITS.getName(), startTxId, endTxId));
  }

  /**
   * 获取指定日志段对应paxos恢复过程持久化数据的文件路径。
   * @param segmentTxId 日志段的起始事务ID
   * @return paxos持久化文件对象
   */
  File getPaxosFile(long segmentTxId) {
    return new File(getOrCreatePaxosDir(), String.valueOf(segmentTxId));
  }
  
  /**
   * 获取paxos目录，如果目录不存在则创建。
   * @return paxos目录对象
   */
  File getOrCreatePaxosDir() {
    File paxosDir = new File(sd.getCurrentDir(), "paxos");
    if(!paxosDir.exists()) {
      LOG.info("Creating paxos dir: {}", paxosDir.toPath());
      if(!paxosDir.mkdir()) {
        LOG.error("Could not create paxos dir: {}", paxosDir.toPath());
      }
    }
    return paxosDir;
  }
  
  /**
   * 获取当前存储目录的根目录。
   * @return 根目录对象
   */
  File getRoot() {
    return sd.getRoot();
  }
  
  /**
   * 清理所有事务ID小于指定保留阈值的日志文件和关联paxos文件。
   * @param minTxIdToKeep 需要保留的最小事务ID，小于该值的文件会被清理
   * @throws IOException 清理过程中IO异常时抛出
   */
  void purgeDataOlderThan(long minTxIdToKeep) throws IOException {
    // 清理旧的日志文件
    fjm.purgeLogsOlderThan(minTxIdToKeep);

    // 清理旧的paxos文件
    purgeMatching(getOrCreatePaxosDir(),
        PAXOS_DIR_PURGE_REGEXES, minTxIdToKeep);
  }
  
  /**
   * 清理目录中匹配正则表达式且事务ID小于保留阈值的文件。
   * 正则需要包含一个数字捕获组用于提取文件对应的事务ID。
   * @param dir 需要清理的目标目录
   * @param patterns 匹配文件名的正则表达式列表
   * @param minTxIdToKeep 需要保留的最小事务ID
   * @throws IOException 目录列表读取失败时抛出IO异常
   */
  private static void purgeMatching(File dir, List<Pattern> patterns,
      long minTxIdToKeep) throws IOException {

    // 遍历目录中所有文件
    for (File f : FileUtil.listFiles(dir)) {
      // 跳过目录，只处理文件
      if (!f.isFile()) continue;
      
      // 尝试匹配每个正则表达式
      for (Pattern p : patterns) {
        Matcher matcher = p.matcher(f.getName());
        if (matcher.matches()) {
          // 正则保证解析一定成功
          long txid = Long.parseLong(matcher.group(1));
          if (txid < minTxIdToKeep) {
            // 事务ID小于保留阈值，删除文件
            LOG.info("Purging no-longer needed file {}", txid);
            if (!f.delete()) {
              LOG.warn("Unable to delete no-longer-needed data {}", f);
            }
            break;
          }
        }
      }
    }
  }

  /**
   * 格式化当前Journal存储目录，写入命名空间信息并初始化目录结构。
   * @param nsInfo 命名空间信息
   * @param force 是否强制格式化
   * @throws IOException 格式化过程IO异常时抛出
   */
  void format(NamespaceInfo nsInfo, boolean force) throws IOException {
    // 先解锁所有存储目录
    unlockAll();
    try {
      sd.analyzeStorage(StartupOption.FORMAT, this, !force);
    } finally {
      sd.unlock();
    }
    // 设置存储对应的命名空间信息
    setStorageInfo(nsInfo);

    LOG.info("Formatting journal {} with nsid: {}", sd, getNamespaceID());
    // 格式化前先解锁，后续分析会重新加锁
    unlockAll();
    // 清空目录原有内容
    sd.clearDirectory();
    // 写入存储属性文件
    writeProperties(sd);
    // 创建paxos目录
    getOrCreatePaxosDir();
    // 重新分析存储状态
    analyzeStorage();
  }
  
  /**
   * 分析当前存储状态，更新存储状态并刷新属性信息。
   * @throws IOException 存储分析失败时抛出IO异常
   */
  void analyzeStorage() throws IOException {
    this.state = sd.analyzeStorage(StartupOption.REGULAR, this);
    refreshStorage();
  }

  /**
   * 如果存储状态正常，读取存储属性信息。
   * @throws IOException 读取属性失败时抛出IO异常
   */
  void refreshStorage() throws IOException {
    if (state == StorageState.NORMAL) {
      readProperties(sd);
    }
  }

  @Override
  protected void setLayoutVersion(Properties props, StorageDirectory sd)
      throws IncorrectVersionException, InconsistentFSStateException {
    int lv = Integer.parseInt(getProperty(props, sd, "layoutVersion"));
    // For journal node, since it now does not decode but just scan through the
    // edits, it can handle edits with future version in most of the cases.
    // Thus currently we may skip the layoutVersion check here.
    layoutVersion = lv;
  }

  /**
   * 分析存储状态并根据需要进行存储恢复。
   * @param startOpt 启动选项，标识启动场景
   * @throws IOException 分析或恢复失败时抛出IO异常
   */
  void analyzeAndRecoverStorage(StartupOption startOpt) throws IOException {
    this.state = sd.analyzeStorage(startOpt, this);
    // 判断是否需要进行存储恢复
    final boolean needRecover = state != StorageState.NORMAL
        && state != StorageState.NON_EXISTENT
        && state != StorageState.NOT_FORMATTED;
    if (state == StorageState.NORMAL && startOpt != StartupOption.ROLLBACK) {
      // 状态正常且不是回滚启动，读取属性信息
      readProperties(sd);
    } else if (needRecover) {
      // 需要恢复，执行存储恢复
      sd.doRecover(state);
    }
  }

  /**
   * 检查当前存储的命名空间信息与传入NameNode的命名空间信息是否一致，不一致则抛出异常。
   * @param nsInfo NameNode的命名空间信息
   * @throws IOException 命名空间ID或集群ID不匹配时抛出IO异常
   */
  void checkConsistentNamespace(NamespaceInfo nsInfo)
      throws IOException {
    if (nsInfo.getNamespaceID() != getNamespaceID()) {
      throw new IOException("Incompatible namespaceID for journal " +
          this.sd + ": NameNode has nsId " + nsInfo.getNamespaceID() +
          " but storage has nsId " + getNamespaceID());
    }
    
    if (!nsInfo.getClusterID().equals(getClusterID())) {
      throw new IOException("Incompatible clusterID for journal " +
          this.sd + ": NameNode has clusterId '" + nsInfo.getClusterID() +
          "' but storage has clusterId '" + getClusterID() + "'");
      
    }
  }

  /**
   * 关闭当前存储，解锁所有存储目录。
   * @throws IOException 解锁失败时抛出IO异常
   */
  public void close() throws IOException {
    LOG.info("Closing journal storage for {}", sd);
    unlockAll();
  }

  /**
   * 判断当前存储是否已经完成格式化。
   * @return 格式化完成返回true，否则返回false
   */
  public boolean isFormatted() {
    return state == StorageState.NORMAL;
  }
}