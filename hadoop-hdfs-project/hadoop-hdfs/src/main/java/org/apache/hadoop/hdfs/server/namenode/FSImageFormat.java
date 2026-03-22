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

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutFlags;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat.ReferenceMap;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

/**
 * 文件：FSImage格式处理工具类，提供FSImage磁盘格式的读写能力
 * 定义了HDFS NameNode元数据镜像文件（FSImage）的传统磁盘存储格式，包含加载和保存镜像的核心逻辑
 * 当前已被Protobuf格式的FSImage替代，本类仅用于保持向后兼容性，支持读取旧版本镜像
 * 
 * FSImage整体格式概述：
 * <pre>
 * FSImage {
 *   layoutVersion: int, namespaceID: int, numberItemsInFSDirectoryTree: long,
 *   namesystemGenerationStampV1: long, namesystemGenerationStampV2: long,
 *   generationStampAtBlockIdSwitch:long, lastAllocatedBlockId:
 *   long transactionID: long, snapshotCounter: int, numberOfSnapshots: int,
 *   numOfSnapshottableDirs: int,
 *   {FSDirectoryTree, FilesUnderConstruction, SecretManagerState} (可以压缩)
 * }
 *
 * FSDirectoryTree (支持 {@link Feature#FSIMAGE_NAME_OPTIMIZATION} 时) {
 *   root的INodeInfo, root的子节点数量: int
 *   [root子节点的INodeInfo列表],
 *   [root子目录的INodeDirectoryInfo列表]
 * }
 *
 * FSDirectoryTree (不支持 {@link Feature#FSIMAGE_NAME_OPTIMIZATION} 时){
 *   [拓扑序排列的所有INodeInfo列表]
 * }
 *
 * INodeInfo {
 *   {
 *     localName: short + byte[]
 *   } 当 {@link Feature#FSIMAGE_NAME_OPTIMIZATION} 被支持时
 *   或
 *   {
 *     fullPath: byte[]
 *   } 当 {@link Feature#FSIMAGE_NAME_OPTIMIZATION} 不被支持时
 *   replicationFactor: short, modificationTime: long,
 *   accessTime: long, preferredBlockSize: long,
 *   numberOfBlocks: int (-1表示INodeDirectory, -2表示INodeSymLink),
 *   {
 *     nsQuota: long, dsQuota: long,
 *     {
 *       isINodeSnapshottable: byte,
 *       isINodeWithSnapshot: byte (当isINodeSnapshottable为false时)
 *     } (当 {@link Feature#SNAPSHOT} 被支持时),
 *     fsPermission: short, PermissionStatus
 *   } 对应INodeDirectory
 *   或
 *   {
 *     symlinkString, fsPermission: short, PermissionStatus
 *   } 对应INodeSymlink
 *   或
 *   {
 *     [BlockInfo列表]
 *     [FileDiff列表]
 *     {
 *       isINodeFileUnderConstructionSnapshot: byte,
 *       {clientName: short + byte[], clientMachine: short + byte[]} (当
 *       isINodeFileUnderConstructionSnapshot为true时),
 *     } (当 {@link Feature#SNAPSHOT} 被支持且写入snapshotINode时),
 *     fsPermission: short, PermissionStatus
 *   } 对应INodeFile
 * }
 *
 * INodeDirectoryInfo {
 *   目录的完整路径: short + byte[],
 *   子节点数量: int, [子节点INode的INodeInfo列表],
 *   {
 *     快照数量: int,
 *     [Snapshot列表] (当NumberOfSnapshots为正时),
 *     目录Diff数量: int,
 *     [DirectoryDiff列表] (NumberOfDirectoryDiffs为正时),
 *     子目录的数量,
 *     [子目录的INodeDirectoryInfo列表] (包含已删除子目录的快照副本)
 *   } (当 {@link Feature#SNAPSHOT} 被支持时),
 * }
 *
 * Snapshot {
 *   snapshotID: int, Snapshot根目录: INodeDirectoryInfo (其local name为快照名称)
 * }
 *
 * DirectoryDiff {
 *   关联快照根目录的完整路径: short + byte[],
 *   子节点数量: int,
 *   是否为快照根: byte,
 *   snapshotINode不为空: byte (当isSnapshotRoot为false时),
 *   snapshotINode: INodeDirectory (当SnapshotINodeIsNotNull为true时), Diff
 * }
 *
 * Diff {
 *   创建列表大小: int, [创建列表中INode的本地名称],
 *   删除列表大小: int, [删除列表中INode: INodeInfo]
 * }
 *
 * FileDiff {
 *   关联快照根目录的完整路径: short + byte[],
 *   文件大小: long,
 *   snapshotINode不为空: byte,
 *   snapshotINode: INodeFile (当SnapshotINodeIsNotNull为true时), Diff
 * }
 * </pre>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSImageFormat {
  private static final Logger LOG = FSImage.LOG;

  // 纯静态工具类，禁止实例化
  private FSImageFormat() {}

  /**
   * FSImage加载器抽象接口，定义加载完成后获取镜像元数据的方法
   */
  interface AbstractLoader {
    /** 获取已加载镜像的MD5校验和 */
    MD5Hash getLoadedImageMd5();
    /** 获取已加载镜像对应事务ID */
    long getLoadedImageTxId();
  }

  /**
   * FSImage加载器委托类，根据镜像文件头自动选择传统格式或Protobuf格式加载器
   */
  static class LoaderDelegator implements AbstractLoader {
    private AbstractLoader impl;
    private final Configuration conf;
    private final FSNamesystem fsn;

    LoaderDelegator(Configuration conf, FSNamesystem fsn) {
      this.conf = conf;
      this.fsn = fsn;
    }

    @Override
    public MD5Hash getLoadedImageMd5() {
      return impl.getLoadedImageMd5();
    }

    @Override
    public long getLoadedImageTxId() {
      return impl.getLoadedImageTxId();
    }

    /**
     * 加载指定FSImage文件，根据魔数自动选择加载器实现
     * @param file 要加载的FSImage文件
     * @param requireSameLayoutVersion 是否要求镜像版本与当前版本一致
     * @throws IOException 加载过程中发生IO错误
     */
    public void load(File file, boolean requireSameLayoutVersion)
        throws IOException {
      Preconditions.checkState(impl == null, "Image already loaded!");

      InputStream is = null;
      try {
        is = Files.newInputStream(file.toPath());
        // 读取魔数判断文件格式
        byte[] magic = new byte[FSImageUtil.MAGIC_HEADER.length];
        IOUtils.readFully(is, magic, 0, magic.length);
        if (Arrays.equals(magic, FSImageUtil.MAGIC_HEADER)) {
          // 新Protobuf格式FSImage
          FSImageFormatProtobuf.Loader loader = new FSImageFormatProtobuf.Loader(
              conf, fsn, requireSameLayoutVersion);
          impl = loader;
          loader.load(file);
        } else {
          // 传统格式FSImage
          Loader loader = new Loader(conf, fsn);
          impl = loader;
          loader.load(file);
        }
      } finally {
        IOUtils.cleanupWithLogger(LOG, is);
      }
    }
  }

  /**
   * 创建新的加载器委托实例，自动根据镜像格式选择对应加载器
   * @param conf Hadoop配置对象
   * @param fsn 目标文件系统命名空间
   * @return 加载器委托实例
   */
  public static LoaderDelegator newLoader(Configuration conf, FSNamesystem fsn) {

    return new LoaderDelegator(conf, fsn);
  }

  /**
   * 传统格式FSImage加载器，单次使用类，负责从磁盘加载传统格式FSImage到FSNamesystem
   * 加载成功后可通过getter方法获取镜像的MD5校验和与事务ID
   */
  public static class Loader implements AbstractLoader {
    private final Configuration conf;
    /** 当前加载器目标FSNamesystem */
    private final FSNamesystem namesystem;

    /** 标记是否已完成加载 */
    private boolean loaded = false;

    /** 加载镜像对应的最后一个编辑日志事务ID */
    private long imgTxId;
    /** 加载镜像的MD5校验和 */
    private MD5Hash imgDigest;
    
    private Map<Integer, Snapshot> snapshotMap = null;
    private final ReferenceMap referenceMap = new ReferenceMap();

    Loader(Configuration conf, FSNamesystem namesystem) {
      this.conf = conf;
      this.namesystem = namesystem;
    }

    /**
     * 获取已加载镜像的MD5校验和
     * @throws IllegalStateException 如果尚未调用load()方法
     */
    @Override
    public MD5Hash getLoadedImageMd5() {
      checkLoaded();
      return imgDigest;
    }

    @Override
    public long getLoadedImageTxId() {
      checkLoaded();
      return imgTxId;
    }

    /**
     * 检查是否已完成加载，未加载则抛出异常
     */
    private void checkLoaded() {
      if (!loaded) {
        throw new IllegalStateException("Image not yet loaded!");
      }
    }

    /**
     * 检查是否未完成加载，已加载则抛出异常
     */
    private void checkNotLoaded() {
      if (loaded) {
        throw new IllegalStateException("Image already loaded!");
      }
    }

    /**
     * 加载指定的传统格式FSImage文件，将元数据加载到FSNamesystem
     * @param curFile 要加载的FSImage文件
     * @throws IOException 加载过程中发生IO错误或格式错误
     */
    public void load(File curFile) throws IOException {
      checkNotLoaded();
      assert curFile != null : "curFile is null";

      // 初始化启动进度统计
      StartupProgress prog = NameNode.getStartupProgress();
      Step step = new Step(StepType.INODES);
      prog.beginStep(Phase.LOADING_FSIMAGE, step);
      long startTime = monotonicNow();

      // 创建MD5 digester计算镜像校验和
      MessageDigest digester = MD5Hash.getDigester();
      DigestInputStream fin = new DigestInputStream(
          Files.newInputStream(curFile.toPath()), digester);

      DataInputStream in = new DataInputStream(fin);
      try {
        // 读取镜像版本号
        int imgVersion = in.readInt();
        if (getLayoutVersion() != imgVersion) {
          throw new InconsistentFSStateException(curFile, 
              "imgVersion " + imgVersion +
              " expected to be " + getLayoutVersion());
        }
        // 检查是否支持快照功能
        boolean supportSnapshot = NameNodeLayoutVersion.supports(
            LayoutVersion.Feature.SNAPSHOT, imgVersion);
        // 读取布局标志位
        if (NameNodeLayoutVersion.supports(
            LayoutVersion.Feature.ADD_LAYOUT_FLAGS, imgVersion)) {
          LayoutFlags.read(in);
        }

        // 跳过命名空间ID（已在其他地方初始化）
        in.readInt();

        // 读取INode总数
        long numFiles = in.readLong();

        // 读取旧版本块的最后一代时间戳
        long genstamp = in.readLong();
        final BlockIdManager blockIdManager = namesystem.getBlockManager()
            .getBlockIdManager();
        blockIdManager.setLegacyGenerationStamp(genstamp);

        // 如果支持顺序块ID，读取块ID相关元数据
        if (NameNodeLayoutVersion.supports(
            LayoutVersion.Feature.SEQUENTIAL_BLOCK_ID, imgVersion)) {
          // 顺序块ID的起始生成时间戳
          genstamp = in.readLong();
          blockIdManager.setGenerationStamp(genstamp);

          // 切换顺序块ID时的最后一代时间戳
          long stampAtIdSwitch = in.readLong();
          blockIdManager.setLegacyGenerationStampLimit(stampAtIdSwitch);

          // 最大已分配顺序块ID
          long maxSequentialBlockId = in.readLong();
          blockIdManager.setLastAllocatedContiguousBlockId(maxSequentialBlockId);
        } else {
          // 从旧版本升级：升级生成时间戳
          long startingGenStamp = blockIdManager.upgradeLegacyGenerationStamp();
          LOG.info("Upgrading to sequential block IDs. Generation stamp " +
                   "for new blocks set to " + startingGenStamp);
        }

        // 读取镜像对应的最后事务ID
        if (NameNodeLayoutVersion.supports(
            LayoutVersion.Feature.STORED_TXIDS, imgVersion)) {
          imgTxId = in.readLong();
        } else {
          imgTxId = 0;
        }

        // 读取最后分配的Inode ID
        if (NameNodeLayoutVersion.supports(
            LayoutVersion.Feature.ADD_INODE_ID, imgVersion)) {
          long lastInodeId = in.readLong();
          namesystem.dir.resetLastInodeId(lastInodeId);
          if (LOG.isDebugEnabled()) {
            LOG.debug("load last allocated InodeId from fsimage:" + lastInodeId);
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Old layout version doesn't have inode id."
                + " Will assign new id for each inode.");
          }
        }
        
        // 如果支持快照，读取快照管理器状态
        if (supportSnapshot) {
          snapshotMap = namesystem.getSnapshotManager().read(in, this);
        }

        // 处理压缩，解压输入流
        FSImageCompression compression;
        if (NameNodeLayoutVersion.supports(
            LayoutVersion.Feature.FSIMAGE_COMPRESSION, imgVersion)) {
          compression = FSImageCompression.readCompressionHeader(conf, in);
        } else {
          compression = FSImageCompression.createNoopCompression