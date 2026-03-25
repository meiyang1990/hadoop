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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.AccessControlException;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_ERASURECODING_POLICY;

/**
 * 目录内容汇总计算上下文，保存目录内容统计过程中的状态信息，
 * 支持增量分批次统计以避免长时间占用NameNode全局锁，同时提供权限检查、纠删码策略获取等辅助能力。
 * 核心职责是维护统计计数、处理锁让步机制，支撑大目录的内容汇总计算。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ContentSummaryComputationContext {
  private FSDirectory dir = null;
  private FSNamesystem fsn = null;
  private BlockStoragePolicySuite bsps = null;
  private ContentCounts counts = null;
  private ContentCounts snapshotCounts = null;
  private long nextCountLimit = 0;
  private long limitPerRun = 0;
  private long yieldCount = 0;
  private long sleepMilliSec = 0;
  private int sleepNanoSec = 0;

  public static final String REPLICATED = "Replicated";
  public static final Logger LOG = LoggerFactory
      .getLogger(ContentSummaryComputationContext.class);

  private FSPermissionChecker pc;

  /**
   * 构造内容汇总计算上下文，用于非阻塞式分批次统计。
   *
   * @param dir FSDirectory实例，对应要统计的目录命名空间
   * @param fsn FSNamesystem实例，对应文件系统整体命名空间
   * @param limitPerRun 一次锁持有周期内允许的操作数，小于等于0表示不限制（不做让步）
   * @param sleepMicroSec 锁让步后休眠的微秒数，让其他线程获取锁执行
   */
  public ContentSummaryComputationContext(FSDirectory dir,
      FSNamesystem fsn, long limitPerRun, long sleepMicroSec) {
    this(dir, fsn, limitPerRun, sleepMicroSec, null);
  }

  /**
   * 构造内容汇总计算上下文，支持自定义权限检查器。
   *
   * @param dir FSDirectory实例，对应要统计的目录命名空间
   * @param fsn FSNamesystem实例，对应文件系统整体命名空间
   * @param limitPerRun 一次锁持有周期内允许的操作数，小于等于0表示不限制（不做让步）
   * @param sleepMicroSec 锁让步后休眠的微秒数，让其他线程获取锁执行
   * @param pc 权限检查器，用于统计过程中的权限校验
   */
  public ContentSummaryComputationContext(FSDirectory dir,
      FSNamesystem fsn, long limitPerRun, long sleepMicroSec,
      FSPermissionChecker pc) {
    this.dir = dir;
    this.fsn = fsn;
    this.limitPerRun = limitPerRun;
    this.nextCountLimit = limitPerRun;
    this.counts = new ContentCounts.Builder().build();
    this.snapshotCounts = new ContentCounts.Builder().build();
    this.sleepMilliSec = sleepMicroSec/1000;
    this.sleepNanoSec = (int)((sleepMicroSec%1000)*1000);
    this.pc = pc;
  }

  /**
   * 构造阻塞式计算上下文，用于不需要分批次让步的统计场景。
   *
   * @param bsps 块存储策略集合
   */
  public ContentSummaryComputationContext(BlockStoragePolicySuite bsps) {
    this(null, null, 0, 1000);
    this.bsps = bsps;
  }

  /**
   * 获取锁让步执行的总次数。
   *
   * @return 锁让步的总次数
   */
  public long getYieldCount() {
    return yieldCount;
  }

  /**
   * 释放当前持有的锁，短暂休眠后重新获取锁，让其他竞争锁的线程有机会执行。
   * 用于大目录统计时避免长时间占用NameNode全局锁，阻塞其他关键操作。
   *
   * @return 如果成功释放并重获取锁返回true，否则返回false
   */
  public boolean yield() {
    // 检查是否启用让步机制
    if (limitPerRun <= 0 || dir == null || fsn == null) {
      return false;
    }

    // 检查当前计数是否达到本轮限制
    long currentCount = counts.getFileCount() +
        counts.getSymlinkCount() +
        counts.getDirectoryCount() +
        counts.getSnapshotableDirectoryCount();
    if (currentCount <= nextCountLimit) {
      return false;
    }

    // 更新下一轮的计数限制
    nextCountLimit = currentCount + limitPerRun;

    // 记录当前锁的持有状态
    boolean hadDirReadLock = dir.hasReadLock();
    boolean hadDirWriteLock = dir.hasWriteLock();
    boolean hadFsnReadLock = fsn.hasReadLock(RwLockMode.GLOBAL);
    boolean hadFsnWriteLock = fsn.hasWriteLock(RwLockMode.GLOBAL);

    // 合法性检查：必须只持有FSDirectory和FSNamesystem的读锁，且读锁计数为1才能让步
    if (!hadDirReadLock || !hadFsnReadLock || hadDirWriteLock ||
        hadFsnWriteLock || fsn.getReadHoldCount() != 1) {
      // 无法释放锁，返回失败
      return false;
    }

    // 释放锁
    dir.readUnlock();
    fsn.readUnlock(RwLockMode.GLOBAL, "contentSummary");

    try {
      // 休眠指定时间，让其他线程获取锁
      Thread.sleep(sleepMilliSec, sleepNanoSec);
    } catch (InterruptedException ie) {
    } finally {
      // 重新获取锁，继续后续统计
      fsn.readLock(RwLockMode.GLOBAL);
      dir.readLock();
    }
    yieldCount++;
    return true;
  }

  /**
   * 获取当前统计的内容计数结果。
   *
   * @return 非快照目录的内容计数对象
   */
  public ContentCounts getCounts() {
    return counts;
  }

  /**
   * 获取快照相关的内容计数结果。
   *
   * @return 快照目录的内容计数对象
   */
  public ContentCounts getSnapshotCounts() {
    return snapshotCounts;
  }

  /**
   * 获取块存储策略集合，优先使用上下文自带的实例，不存在则从FSNamesystem获取。
   *
   * @return 块存储策略集合实例
   */
  public BlockStoragePolicySuite getBlockStoragePolicySuite() {
    Preconditions.checkState((bsps != null || fsn != null),
        "BlockStoragePolicySuite must be either initialized or available via" +
            " FSNameSystem");
    return (bsps != null) ? bsps:
        fsn.getBlockManager().getStoragePolicySuite();
  }

  /**
   * 获取指定inode对应的纠删码策略名称，从inode本身或继承自父目录。
   *
   * @param inode 要查询纠删码策略的inode节点
   * @return 纠删码策略名称，默认副本策略返回REPLICATED，软链接或未找到返回空字符串
   */
  public String getErasureCodingPolicyName(INode inode) {
    if (inode.isFile()) {
      INodeFile iNodeFile = inode.asFile();
      if (iNodeFile.isStriped()) {
        byte ecPolicyId = iNodeFile.getErasureCodingPolicyID();
        return fsn.getErasureCodingPolicyManager()
            .getByID(ecPolicyId).getName();
      } else {
        return REPLICATED;
      }
    }
    if (inode.isSymlink()) {
      return "";
    }
    try {
      final XAttrFeature xaf = inode.getXAttrFeature();
      if (xaf != null) {
        XAttr xattr = xaf.getXAttr(XATTR_ERASURECODING_POLICY);
        if (xattr != null) {
          ByteArrayInputStream bins =
              new ByteArrayInputStream(xattr.getValue());
          DataInputStream din = new DataInputStream(bins);
          String ecPolicyName = WritableUtils.readString(din);
          return dir.getFSNamesystem()
              .getErasureCodingPolicyManager()
              .getErasureCodingPolicyByName(ecPolicyName)
              .getName();
        }
      } else if (inode.getParent() != null) {
          // 当前目录未设置策略，继承父目录的策略
          return getErasureCodingPolicyName(inode.getParent());
      }
    } catch (IOException ioe) {
      LOG.warn("Encountered error getting ec policy for "
          + inode.getFullPathName(), ioe);
      return "";
    }
    return "";
  }

  /**
   * 检查当前用户对指定目录节点是否有对应访问权限，开启权限检查时才会执行校验。
   *
   * @param inode 要检查权限的目录节点
   * @param snapshotId 快照ID，用于快照场景下的权限检查
   * @param access 需要检查的访问操作类型
   * @throws AccessControlException 权限不足时抛出异常
   */
  void checkPermission(INodeDirectory inode, int snapshotId, FsAction access)
      throws AccessControlException {
    if (dir != null && dir.isPermissionEnabled()
        && pc != null) {
      if (pc.isSuperUser()) {
        // 超级用户也需要调用外部检查器，生成审计日志
        pc.checkSuperuserPrivilege(inode.getFullPathName());
      } else {
        // 普通用户执行权限检查
        pc.checkPermission(inode, snapshotId, access);
      }
    }
  }
}