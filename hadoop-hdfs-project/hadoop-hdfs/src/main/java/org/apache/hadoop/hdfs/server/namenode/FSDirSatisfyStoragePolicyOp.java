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

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_SATISFY_STORAGE_POLICY;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfyManager;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.util.Lists;

/**
 * 文件级存储策略满足操作工具类，为NameNode处理存储策略满足请求提供辅助能力，
 * 负责权限检查、扩展属性标记、路径加入异步处理队列等操作，支撑存储策略自动满足功能。
 */
final class FSDirSatisfyStoragePolicyOp {

  /**
   * 工具类私有构造方法，禁止实例化，仅提供静态方法。
   */
  private FSDirSatisfyStoragePolicyOp() {
  }

  /**
   * 触发指定路径的存储策略满足操作，添加路径到存储策略满足管理器异步队列，
   * 由后台线程异步处理数据块迁移，使文件/目录符合指定存储策略。
   *
   * @param fsd          FSDirectory对象，管理文件系统目录树
   * @param bm           BlockManager对象，管理数据块
   * @param src          源路径，需要满足存储策略的路径
   * @param logRetryCache 是否需要在编辑日志中记录RPC ID用于重试缓存重建
   * @return 目标路径的文件状态信息
   * @throws IOException 权限检查、元数据操作异常
   */
  static FileStatus satisfyStoragePolicy(FSDirectory fsd, BlockManager bm,
      String src, boolean logRetryCache) throws IOException {

    assert fsd.getFSNamesystem().hasWriteLock(RwLockMode.FS);
    FSPermissionChecker pc = fsd.getPermissionChecker();
    INodesInPath iip;
    fsd.writeLock();
    try {

      // 检查操作权限并解析路径
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }
      INode inode = FSDirectory.resolveLastINode(iip);
      // 空文件跳过处理，无块需要迁移
      if (inode.isFile() && inode.asFile().numBlocks() == 0) {
        if (NameNode.LOG.isInfoEnabled()) {
          NameNode.LOG.info(
              "Skipping satisfy storage policy on path:{} as "
                  + "this file doesn't have any blocks!",
              inode.getFullPathName());
        }
      } else if (inodeHasSatisfyXAttr(inode)) {
        // 已经存在满足标记，拒绝重复请求
        NameNode.LOG
            .warn("Cannot request to call satisfy storage policy on path: "
                + inode.getFullPathName()
                + ", as this file/dir was already called for satisfying "
                + "storage policy.");
      } else {
        // 构建存储策略满足扩展属性
        XAttr satisfyXAttr = XAttrHelper
            .buildXAttr(XATTR_SATISFY_STORAGE_POLICY);
        List<XAttr> xAttrs = Arrays.asList(satisfyXAttr);
        List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(inode);
        // 设置扩展属性到inode
        List<XAttr> newXAttrs = FSDirXAttrOp.setINodeXAttrs(fsd, existingXAttrs,
            xAttrs, EnumSet.of(XAttrSetFlag.CREATE));
        XAttrStorage.updateINodeXAttrs(inode, newXAttrs,
            iip.getLatestSnapshotId());
        // 记录操作到编辑日志
        fsd.getEditLog().logSetXAttrs(src, xAttrs, logRetryCache);

        // 将路径ID加入存储策略满足管理器的待处理队列，后台异步处理
        StoragePolicySatisfyManager spsManager =
            fsd.getBlockManager().getSPSManager();
        if (spsManager != null) {
          spsManager.addPathId(inode.getId());
        }
      }
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * 无权限保护版本的存储策略满足触发，直接将inode加入处理队列，
   * 用于内部恢复场景，已经提前完成权限和路径检查。
   *
   * @param inode  目标inode
   * @param fsd    FSDirectory对象
   * @return 成功加入队列返回true，空文件跳过返回false
   */
  static boolean unprotectedSatisfyStoragePolicy(INode inode, FSDirectory fsd) {
    if (inode.isFile() && inode.asFile().numBlocks() == 0) {
      return false;
    } else {
      // 将路径ID加入存储策略满足管理器的待处理队列，后台异步处理
      StoragePolicySatisfyManager spsManager =
          fsd.getBlockManager().getSPSManager();
      if (spsManager != null) {
        spsManager.addPathId(inode.getId());
      }
      return true;
    }
  }

  /**
   * 检查inode是否已经存在存储策略满足扩展属性，避免重复提交请求。
   *
   * @param inode 目标inode
   * @return 存在满足标记返回true，否则返回false
   */
  private static boolean inodeHasSatisfyXAttr(INode inode) {
    final XAttrFeature f = inode.getXAttrFeature();
    if (inode.isFile() && f != null
        && f.getXAttr(XATTR_SATISFY_STORAGE_POLICY) != null) {
      return true;
    }
    return false;
  }

  /**
   * 存储策略满足处理完成后，移除inode上的存储策略满足扩展属性，
   * 更新元数据并记录操作到编辑日志。
   *
   * @param fsd      FSDirectory对象
   * @param inode    目标inode
   * @param spsXAttr 存储策略满足扩展属性对象
   * @throws IOException 元数据更新异常
   */
  static void removeSPSXattr(FSDirectory fsd, INode inode, XAttr spsXAttr)
      throws IOException {
    try {
      fsd.writeLock();
      List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(inode);
      existingXAttrs.remove(spsXAttr);
      XAttrStorage.updateINodeXAttrs(inode, existingXAttrs, INodesInPath
          .fromINode(inode).getLatestSnapshotId());
      List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
      xAttrs.add(spsXAttr);
      fsd.getEditLog().logRemoveXAttrs(inode.getFullPathName(), xAttrs, false);
    } finally {
      fsd.writeUnlock();
    }
  }
}