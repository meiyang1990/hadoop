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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_REENCRYPTION_STATUS_NUM_RESPONSES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_REENCRYPTION_STATUS_NUM_RESPONSES_KEY;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants
    .CRYPTO_XATTR_ENCRYPTION_ZONE;

/**
 * 文件系统加密区管理器，负责管理所有HDFS加密区的生命周期、路径查询和重加密任务调度
 * <p>
 * 该管理器维护独立锁，但多数操作依赖FSDirectory全局锁，必须遵守加锁顺序：先获取FSDirectory锁，再获取管理器锁，禁止反向加锁
 * </p>
 */
public class EncryptionZoneManager {

  public static final Logger LOG = LoggerFactory.getLogger(EncryptionZoneManager
      .class);

  /**
   * 加密区内部表示，存储加密区核心元数据，外部公开API使用EncryptionZone类，该类不包含路径信息
   */
  private static class EncryptionZoneInt {
    private final long inodeId;
    private final CipherSuite suite;
    private final CryptoProtocolVersion version;
    private final String keyName;

    EncryptionZoneInt(long inodeId, CipherSuite suite,
        CryptoProtocolVersion version, String keyName) {
      Preconditions.checkArgument(suite != CipherSuite.UNKNOWN);
      Preconditions.checkArgument(version != CryptoProtocolVersion.UNKNOWN);
      this.inodeId = inodeId;
      this.suite = suite;
      this.version = version;
      this.keyName = keyName;
    }

    long getINodeId() {
      return inodeId;
    }

    CipherSuite getSuite() {
      return suite;
    }

    CryptoProtocolVersion getVersion() { return version; }

    String getKeyName() {
      return keyName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof EncryptionZoneInt)) {
        return false;
      }

      EncryptionZoneInt b = (EncryptionZoneInt)o;
      return new EqualsBuilder()
          .append(inodeId, b.getINodeId())
          .append(suite, b.getSuite())
          .append(version, b.getVersion())
          .append(keyName, b.getKeyName())
          .isEquals();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().
          append(inodeId).
          append(suite).
          append(version).
          append(keyName).
          toHashCode();
    }
  }

  private TreeMap<Long, EncryptionZoneInt> encryptionZones = null;
  private final FSDirectory dir;
  private final int maxListEncryptionZonesResponses;
  private final int maxListRecncryptionStatusResponses;

  private ThreadFactory reencryptionThreadFactory;
  private ExecutorService reencryptHandlerExecutor;
  private ReencryptionHandler reencryptionHandler;
  // 将重加密状态与处理器解耦：只要NameNode启动就能查询重加密状态，处理器仅在有KeyProvider时存在
  private final ReencryptionStatus reencryptionStatus;

  public static final BatchedListEntries<ZoneReencryptionStatus> EMPTY_LIST =
      new BatchedListEntries<>(new ArrayList<ZoneReencryptionStatus>(), false);

  @VisibleForTesting
  public void pauseReencryptForTesting() {
    reencryptionHandler.pauseForTesting();
  }

  @VisibleForTesting
  public void resumeReencryptForTesting() {
    reencryptionHandler.resumeForTesting();
  }

  @VisibleForTesting
  public void pauseForTestingAfterNthSubmission(final int count) {
    reencryptionHandler.pauseForTestingAfterNthSubmission(count);
  }

  @VisibleForTesting
  public void pauseReencryptUpdaterForTesting() {
    reencryptionHandler.pauseUpdaterForTesting();
  }

  @VisibleForTesting
  public void resumeReencryptUpdaterForTesting() {
    reencryptionHandler.resumeUpdaterForTesting();
  }

  @VisibleForTesting
  public void pauseForTestingAfterNthCheckpoint(final String zone,
      final int count) throws IOException {
    INodesInPath iip;
    final FSPermissionChecker pc = dir.getPermissionChecker();
    dir.getFSNamesystem().readLock(RwLockMode.FS);
    try {
      iip = dir.resolvePath(pc, zone, DirOp.READ);
    } finally {
      dir.getFSNamesystem().readUnlock(
          RwLockMode.FS, "pauseForTestingAfterNthCheckpoint");
    }
    reencryptionHandler
        .pauseForTestingAfterNthCheckpoint(iip.getLastINode().getId(), count);
  }

  @VisibleForTesting
  public void resetMetricsForTesting() {
    reencryptionStatus.resetMetrics();
  }

  @VisibleForTesting
  public ReencryptionStatus getReencryptionStatus() {
    return reencryptionStatus;
  }

  @VisibleForTesting
  public ZoneReencryptionStatus getZoneStatus(final String zone)
      throws IOException {
    final FSPermissionChecker pc = dir.getPermissionChecker();
    final INode inode;
    dir.getFSNamesystem().readLock(RwLockMode.FS);
    dir.readLock();
    try {
      final INodesInPath iip = dir.resolvePath(pc, zone, DirOp.READ);
      inode = iip.getLastINode();
      if (inode == null) {
        return null;
      }
      return getReencryptionStatus().getZoneStatus(inode.getId());
    } finally {
      dir.readUnlock();
      dir.getFSNamesystem().readUnlock(RwLockMode.FS, "getZoneStatus");
    }
  }

  FSDirectory getFSDirectory() {
    return dir;
  }

  /**
   * 构造加密区管理器
   *
   * @param dir 所属FSDirectory实例
   * @param conf Hadoop配置
   */
  public EncryptionZoneManager(FSDirectory dir, Configuration conf) {
    this.dir = dir;
    maxListEncryptionZonesResponses = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES,
        DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES_DEFAULT
    );
    Preconditions.checkArgument(maxListEncryptionZonesResponses >= 0,
        DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES + " " +
            "must be a positive integer."
    );
    if (getProvider() != null) {
      // 只有配置了密钥提供者才初始化重加密处理器
      reencryptionHandler = new ReencryptionHandler(this, conf);
      reencryptionThreadFactory = new ThreadFactoryBuilder().setDaemon(true)
          .setNameFormat("reencryptionHandlerThread #%d").build();
    }
    maxListRecncryptionStatusResponses =
        conf.getInt(DFS_NAMENODE_LIST_REENCRYPTION_STATUS_NUM_RESPONSES_KEY,
            DFS_NAMENODE_LIST_REENCRYPTION_STATUS_NUM_RESPONSES_DEFAULT);
    Preconditions.checkArgument(maxListRecncryptionStatusResponses >= 0,
        DFS_NAMENODE_LIST_REENCRYPTION_STATUS_NUM_RESPONSES_KEY +
            " must be a positive integer."
    );
    reencryptionStatus = new ReencryptionStatus();
  }

  KeyProviderCryptoExtension getProvider() {
    return dir.getProvider();
  }

  /**
   * 启动重加密处理后台线程
   */
  void startReencryptThreads() {
    if (getProvider() == null) {
      return;
    }
    Preconditions.checkNotNull(reencryptionHandler);
    reencryptHandlerExecutor =
        Executors.newSingleThreadExecutor(reencryptionThreadFactory);
    reencryptHandlerExecutor.execute(reencryptionHandler);
    reencryptionHandler.startUpdaterThread();
  }

  /**
   * 停止重加密处理后台线程
   */
  void stopReencryptThread() {
    if (getProvider() == null || reencryptionHandler == null) {
      return;
    }
    dir.getFSNamesystem().writeLock(RwLockMode.FS);
    try {
      reencryptionHandler.stopThreads();
    } finally {
      dir.getFSNamesystem().writeUnlock(RwLockMode.FS, "stopReencryptThread");
    }
    if (reencryptHandlerExecutor != null) {
      reencryptHandlerExecutor.shutdownNow();
      reencryptHandlerExecutor = null;
    }
  }

  /**
   * 添加加密区，调用时必须持有FSDirectory写锁
   *
   * @param inodeId 加密区根目录inode ID
   * @param suite 加密算法套件
   * @param version 加密协议版本
   * @param keyName 加密区密钥名称
   */
  void addEncryptionZone(Long inodeId, CipherSuite suite,
      CryptoProtocolVersion version, String keyName) {
    assert dir.hasWriteLock();
    unprotectedAddEncryptionZone(inodeId, suite, version, keyName);
  }

  /**
   * 添加加密区，不要求持有FSDirectory锁
   *
   * @param inodeId 加密区根目录inode ID
   * @param suite 加密算法套件
   * @param version 加密协议版本
   * @param keyName 加密区密钥名称
   */
  void unprotectedAddEncryptionZone(Long inodeId,
      CipherSuite suite, CryptoProtocolVersion version, String keyName) {
    final EncryptionZoneInt ez = new EncryptionZoneInt(
        inodeId, suite, version, keyName);
    if (encryptionZones == null) {
      encryptionZones = new TreeMap<>();
    }
    encryptionZones.put(inodeId, ez);
  }

  /**
   * 删除加密区，调用时必须持有FSDirectory写锁
   *
   * @param inodeId 加密区根目录inode ID
   */
  void removeEncryptionZone(Long inodeId) {
    assert dir.hasWriteLock();
    if (hasCreatedEncryptionZone()) {
      if (encryptionZones.remove(inodeId) == null
          || !getReencryptionStatus().hasRunningZone(inodeId)) {
        return;
      }
      if (reencryptionHandler != null) {
        reencryptionHandler.removeZone(inodeId);
      }
    }
  }

  /**
   * 判断给定路径是否位于某个加密区内，调用时必须持有FSDirectory读锁
   *
   * @param iip 待检查路径的INodes序列
   * @return true表示在加密区内，false表示不在
   */
  boolean isInAnEZ(INodesInPath iip) throws UnresolvedLinkException,
      SnapshotAccessControlException, IOException {
    assert dir.hasReadLock();
    return (getEncryptionZoneForPath(iip) != null);
  }

  /**
   * 根据inode ID获取完整路径，调用时必须持有FSDirectory读锁
   *
   * @param nodeId inode ID
   * @return 对应完整路径，inode不存在返回null
   */
  String getFullPathName(Long nodeId) {
    assert dir.hasReadLock();
    INode inode = dir.getInode(nodeId);
    if (inode == null) {
      return null;
    }
    return inode.getFullPathName();
  }

  /**
   * 获取路径所在加密区的密钥名称，调用时必须持有FSDirectory读锁
   *
   * @param iip 待检查路径的INodes序列
   * @return 密钥名称，路径不在加密区返回null
   */
  String getKeyName(final INodesInPath iip) throws IOException {
    assert dir.hasReadLock();
    EncryptionZoneInt ezi = getEncryptionZoneForPath(iip);
    if (ezi == null) {
      return null;
    }
    return ezi.getKeyName();
  }

  /**
   * 查找包含给定路径的最近加密区，调用时必须持有FSDirectory读锁
   *
   * @param iip 待检查路径的INodes序列
   * @return 匹配的加密区，路径不在任何加密区返回null
   */
  private EncryptionZoneInt getEncryptionZoneForPath(INodesInPath iip)
      throws  IOException{
    assert dir.hasReadLock();
    Preconditions.checkNotNull(iip);
    if (!hasCreatedEncryptionZone()) {
      return null;
    }

    int snapshotID = iip.getPathSnapshotId();
    // 从路径末端向上遍历，找到第一个加密区根目录
    for (int i = iip.length() - 1; i >= 0; i--) {
      final INode inode = iip.getINode(i);
      if (inode == null || !inode.isDirectory()) {
        // 加密区仅支持目录，非目录跳过
        continue;
      }
      if (snapshotID == Snapshot.CURRENT_STATE_ID) {
        // 当前状态，从内存加密区映射查询
        final EncryptionZoneInt ezi = encryptionZones.get(inode.getId());
        if (ezi != null) {
          return ezi;
        }
      } else {
        // 快照状态，从快照xattr中解析加密区信息
        XAttr xAttr = FSDirXAttrOp.unprotectedGetXAttrByPrefixedName(
            inode, snapshotID, CRYPTO_XATTR_ENCRYPTION_ZONE);
        if (xAttr != null) {
          try {
            final HdfsProtos.ZoneEncryptionInfoProto ezProto =
                HdfsProtos.ZoneEncryptionInfoProto.parseFrom(xAttr.getValue());
            return new EncryptionZoneInt(
                inode.getId(), PBHelperClient.convert(ezProto.getSuite()),
                PBHelperClient.convert(ezProto.getCryptoProtocolVersion()),
                ezProto.getKeyName());
          } catch (InvalidProtocolBufferException e) {
            throw new IOException("Could not parse encryption zone for inode "
                + iip.getPath(), e);
          }
        }
      }
    }
    return null;
  }

  /**
   * 查找包含给定路径的最近父加密区（排除路径自身），调用时必须持有FSDirectory读锁
   *
   * @param iip 待检查路径的INodes序列
   * @return 匹配的父加密区，路径不在任何加密区或路径是根目录返回null
   */
  private EncryptionZoneInt getParentEncryptionZoneForPath(INodesInPath iip)
      throws  IOException {
    assert dir