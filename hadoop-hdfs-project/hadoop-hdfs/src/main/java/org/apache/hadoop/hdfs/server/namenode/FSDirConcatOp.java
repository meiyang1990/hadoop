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

import org.apache.hadoop.util.Preconditions;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.List;

import static org.apache.hadoop.util.Time.now;

/**
 * HDFS文件拼接操作处理类，负责实现多个文件拼接为一个目标文件的NameNode元数据操作
 * 拼接操作满足以下限制条件:
 * <pre>
 * 1. 所有源文件与目标文件必须位于同一目录
 * 2. 所有源文件不能存在于快照中
 * 3. 任意源文件不能与目标文件相同
 * 4. 源文件不能处于构建中也不能为空
 * 5. 源文件的首选块大小不能大于目标文件
 * </pre>
 */
class FSDirConcatOp {

  /**
   * 执行文件拼接操作，将多个源文件拼接追加到目标文件末尾，然后删除源文件
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param target 目标文件路径
   * @param srcs 源文件路径数组
   * @param logRetryCache 是否记录到重试缓存
   * @return 拼接后目标文件的FileStatus信息
   * @throws IOException 操作失败抛出异常
   */
  static FileStatus concat(FSDirectory fsd, FSPermissionChecker pc,
      String target, String[] srcs, boolean logRetryCache) throws IOException {
    validatePath(target, srcs);
    assert srcs != null;
    NameNode.stateChangeLog.debug("DIR* NameSystem.concat: {} to {}",
        Arrays.toString(srcs), target);

    final INodesInPath targetIIP = fsd.resolvePath(pc, target, DirOp.WRITE);
    // 检查目标文件写权限
    if (fsd.isPermissionEnabled()) {
      fsd.checkPathAccess(pc, targetIIP, FsAction.WRITE);
    }

    // 验证目标文件合法性
    verifyTargetFile(fsd, target, targetIIP);
    // 验证所有源文件合法性
    INodeFile[] srcFiles = verifySrcFiles(fsd, srcs, targetIIP, pc);

    long timestamp = now();
    fsd.writeLock();
    try {
      unprotectedConcat(fsd, targetIIP, srcFiles, timestamp);
    } finally {
      fsd.writeUnlock();
    }
    // 记录拼接操作到编辑日志
    fsd.getEditLog().logConcat(target, srcs, timestamp, logRetryCache);
    return fsd.getAuditFileInfo(targetIIP);
  }

  /**
   * 验证拼接操作输入路径的合法性，检查路径格式是否合法
   * @param target 目标文件路径
   * @param srcs 源文件路径数组
   * @throws IOException 路径不合法抛出异常
   */
  private static void validatePath(String target, String[] srcs)
      throws IOException {
    Preconditions.checkArgument(!target.isEmpty(), "Target file name is empty");
    Preconditions.checkArgument(srcs != null && srcs.length > 0,
        "No sources given");
    if (FSDirectory.isReservedRawName(target)
        || FSDirectory.isReservedInodesName(target)) {
      throw new IOException("Concat operation doesn't support "
          + FSDirectory.DOT_RESERVED_STRING + " relative path : " + target);
    }
    for (String srcPath : srcs) {
      if (FSDirectory.isReservedRawName(srcPath)
          || FSDirectory.isReservedInodesName(srcPath)) {
        throw new IOException("Concat operation doesn't support "
            + FSDirectory.DOT_RESERVED_STRING + " relative path : " + srcPath);
      }
    }
  }

  /**
   * 验证目标文件合法性，检查加密区、构建状态
   * @param fsd 文件目录管理器
   * @param target 目标文件路径
   * @param targetIIP 目标文件的INode路径对象
   * @throws IOException 目标文件不合法抛出异常
   */
  private static void verifyTargetFile(FSDirectory fsd, final String target,
      final INodesInPath targetIIP) throws IOException {
    // 加密区文件不允许拼接
    if (FSDirEncryptionZoneOp.getEZForPath(fsd, targetIIP) != null) {
      throw new HadoopIllegalArgumentException(
          "concat can not be called for files in an encryption zone.");
    }
    final INodeFile targetINode = INodeFile.valueOf(targetIIP.getLastINode(),
        target);
    if(targetINode.isUnderConstruction()) {
      throw new HadoopIllegalArgumentException("concat: target file "
          + target + " is under construction");
    }
  }

  /**
   * 批量验证所有源文件合法性，检查权限、路径、快照状态、编码策略等所有约束条件
   * @param fsd 文件目录管理器
   * @param srcs 源文件路径数组
   * @param targetIIP 目标文件的INode路径对象
   * @param pc 权限检查器
   * @return 验证通过的源文件INode数组
   * @throws IOException 任意源文件不合法抛出异常
   */
  private static INodeFile[] verifySrcFiles(FSDirectory fsd, String[] srcs,
      INodesInPath targetIIP, FSPermissionChecker pc) throws IOException {
    // 用于去重，保证没有重复源文件
    Set<INodeFile> si = new LinkedHashSet<>();
    final INodeFile targetINode = targetIIP.getLastINode().asFile();
    final INodeDirectory targetParent = targetINode.getParent();
    // 遍历检查每个源文件
    for(String src : srcs) {
      final INodesInPath iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      // 源文件权限检查：读文件和删除（父目录写权限）
      if (pc != null && fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.READ);
        fsd.checkParentAccess(pc, iip, FsAction.WRITE);
      }
      final INode srcINode = iip.getLastINode();
      final INodeFile srcINodeFile = INodeFile.valueOf(srcINode, src);
      // 检查源文件与目标文件是否同目录
      if (srcINodeFile.getParent() != targetParent) {
        throw new HadoopIllegalArgumentException("Source file " + src
            + " is not in the same directory with the target "
            + targetIIP.getPath());
      }
      // 检查源文件是否在快照中
      if (srcINode.isInLatestSnapshot(iip.getLatestSnapshotId())) {
        throw new SnapshotException("Concat: the source file " + src
            + " is in snapshot");
      }
      // 检查源文件是否被快照引用，存在多个引用则不允许拼接
      if (srcINode.isReference() && ((INodeReference.WithCount)
          srcINode.asReference().getReferredINode()).getReferenceCount() > 1) {
        throw new SnapshotException("Concat: the source file " + src
            + " is referred by some other reference in some snapshot.");
      }
      // 检查源文件不能等于目标文件
      if (srcINode.equals(targetINode)) {
        throw new HadoopIllegalArgumentException("concat: the src file " + src
            + " is the same with the target file " + targetIIP.getPath());
      }
      // 检查源文件不能处于构建也不能为空
      if(srcINodeFile.isUnderConstruction() || srcINodeFile.numBlocks() == 0) {
        throw new HadoopIllegalArgumentException("concat: source file " + src
            + " is invalid or empty or underConstruction");
      }

      // 检查源文件首选块大小不大于目标文件
      if (srcINodeFile.getPreferredBlockSize() >
          targetINode.getPreferredBlockSize()) {
        throw new HadoopIllegalArgumentException("concat: source file " + src
            + " has preferred block size " + srcINodeFile.getPreferredBlockSize()
            + " which is greater than the target file's preferred block size "
            + targetINode.getPreferredBlockSize());
      }
      // 检查源文件与目标文件纠错编码策略一致
      if(srcINodeFile.getErasureCodingPolicyID() !=
          targetINode.getErasureCodingPolicyID()) {
        throw new HadoopIllegalArgumentException("Source file " + src
            + " and target file " + targetIIP.getPath()
            + " have different erasure coding policy");
      }
      si.add(srcINodeFile);
    }

    // 检查存在重复源文件
    if(si.size() < srcs.length) {
      throw new HadoopIllegalArgumentException(
          "concat: at least two of the source files are the same");
    }
    return si.toArray(new INodeFile[si.size()]);
  }

  /**
   * 计算拼接操作后配额的变化量，处理副本数不同导致的空间配额变化
   * @param fsd 文件目录管理器
   * @param target 目标文件INode
   * @param srcList 源文件INode数组
   * @return 配额变化量对象
   */
  private static QuotaCounts computeQuotaDeltas(FSDirectory fsd,
      INodeFile target, INodeFile[] srcList) {
    QuotaCounts deltas = new QuotaCounts.Builder().build();
    final short targetRepl = target.getPreferredBlockReplication();
    for (INodeFile src : srcList) {
      short srcRepl = src.getFileReplication();
      long fileSize = src.computeFileSize();
      // 源文件与目标文件副本数不同，需要调整空间配额
      if (targetRepl != srcRepl) {
        deltas.addStorageSpace(fileSize * (targetRepl - srcRepl));
        BlockStoragePolicy bsp =
            fsd.getBlockStoragePolicySuite().getPolicy(src.getStoragePolicyID());
        if (bsp != null) {
          // 扣除源文件原有存储类型配额
          List<StorageType> srcTypeChosen = bsp.chooseStorageTypes(srcRepl);
          for (StorageType t : srcTypeChosen) {
            if (t.supportTypeQuota()) {
              deltas.addTypeSpace(t, -fileSize);
            }
          }
          // 增加目标副本数对应的存储类型配额
          List<StorageType> targetTypeChosen = bsp.chooseStorageTypes(targetRepl);
          for (StorageType t : targetTypeChosen) {
            if (t.supportTypeQuota()) {
              deltas.addTypeSpace(t, fileSize);
            }
          }
        }
      }
    }
    // 删除srcList个文件，名称空间配额减少对应数量
    deltas.addNameSpace(-srcList.length);
    return deltas;
  }

  /**
   * 验证拼接后的配额是否超出限制
   * @param fsd 文件目录管理器
   * @param targetIIP 目标文件INode路径对象
   * @param deltas 配额变化量
   * @throws QuotaExceededException 配额超出抛出异常
   */
  private static void verifyQuota(FSDirectory fsd, INodesInPath targetIIP,
      QuotaCounts deltas) throws QuotaExceededException {
    if (!fsd.getFSNamesystem().isImageLoaded() || fsd.shouldSkipQuotaChecks()) {
      // 镜像未加载完成时跳过配额检查
      return;
    }
    FSDirectory.verifyQuota(targetIIP, targetIIP.length() - 1, deltas, null);
  }

  /**
   * 不额外加锁执行拼接操作，已获取写锁后调用，将源文件所有块追加到目标文件，删除源文件
   * @param fsd 文件目录管理器
   * @param targetIIP 目标文件INode路径对象
   * @param srcList 源文件INode数组
   * @param timestamp 操作时间戳
   * @throws IOException 操作失败抛出异常
   */
  static void unprotectedConcat(FSDirectory fsd, INodesInPath targetIIP,
      INodeFile[] srcList, long timestamp) throws IOException {
    assert fsd.hasWriteLock();
    NameNode.stateChangeLog.debug("DIR* NameSystem.concat to {}",
        targetIIP.getPath());

    final INodeFile trgInode = targetIIP.getLastINode().asFile();
    QuotaCounts deltas = computeQuotaDeltas(fsd, trgInode, srcList);
    verifyQuota(fsd, targetIIP, deltas);

    // 目标文件可能在快照中，记录修改操作
    trgInode.recordModification(targetIIP.getLatestSnapshotId());
    INodeDirectory trgParent = targetIIP.getINode(-2).asDirectory();
    // 将源文件块追加到目标文件末尾
    trgInode.concatBlocks(srcList, fsd.getBlockManager());

    // 删除所有源文件
    int count = 0;
    for (INodeFile nodeToRemove : srcList) {
      if(nodeToRemove != null) {
        // 清空源文件块信息
        nodeToRemove.clearBlocks();
        // 从父目录删除源文件，处理快照差异
        nodeToRemove.getParent().removeChild(nodeToRemove,
            targetIIP.getLatestSnapshotId());
        // 从INode映射中删除
        fsd.getINodeMap().remove(nodeToRemove);
        count++;
      }
    }

    // 更新目标文件和父目录修改时间
    trgInode.setModificationTime(timestamp, targetIIP.getLatestSnapshotId());
    trgParent.updateModificationTime(timestamp, targetIIP.getLatestSnapshotId());
    // 更新目录配额
    FSDirectory.unprotectedUpdateCount(targetIIP, targetIIP.length() - 1, deltas);
  }
}