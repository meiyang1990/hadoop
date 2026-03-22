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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.util.Preconditions;

/**
 * HDFS文件系统树遍历器，递归遍历目录并批量处理文件，通过深度优先遍历降低内存占用。
 * 支持遍历过程中释放读写锁避免长时间阻塞，适用于大规模目录树的遍历任务。
 */
@InterfaceAudience.Private
public abstract class FSTreeTraverser {


  public static final Logger LOG = LoggerFactory
      .getLogger(FSTreeTraverser.class);

  private final FSDirectory dir;

  private long readLockReportingThresholdMs;

  private Timer timer;

  /**
   * 构造文件系统树遍历器，初始化配置和依赖组件。
   * @param dir HDFS目录管理对象
   * @param conf Hadoop配置对象
   */
  public FSTreeTraverser(FSDirectory dir, Configuration conf) {
    this.dir = dir;
    this.readLockReportingThresholdMs = conf.getLong(
        DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY,
        DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_DEFAULT);
    timer = new Timer();
  }

  public FSDirectory getFSDirectory() {
    return dir;
  }

  /**
   * 从指定起点开始深度优先递归遍历目录树，仅在内存中维护当前遍历路径而非全部目录节点，降低内存消耗。
   * 支持断点续遍历，从指定位置之后开始处理，支持分批处理过程中释放锁后重新恢复遍历。
   * 
   * @param parent 父目录INode对象
   * @param startId 起始根节点的INode ID
   * @param startAfter 遍历开始位置对应的全路径字节数组，遍历将从该节点之后开始
   * @param traverseInfo 遍历过程中需要传递的附加信息对象
   * @throws IOException 遍历过程中发生IO异常
   * @throws InterruptedException 遍历被中断异常
   */
  protected void traverseDir(final INodeDirectory parent, final long startId,
      byte[] startAfter, final TraverseInfo traverseInfo)
      throws IOException, InterruptedException {
    List<byte[]> startAfters = new ArrayList<>();
    if (parent == null) {
      return;
    }
    INode curr = parent;
    // 从当前节点向上构造到起始根节点的路径数组
    startAfters.add(startAfter);
    while (curr.getId() != startId) {
      startAfters.add(0, curr.getLocalNameBytes());
      curr = curr.getParent();
    }
    curr = traverseDirInt(startId, parent, startAfters, traverseInfo);
    // 持续遍历直到路径为空，完成整个目录树遍历
    while (!startAfters.isEmpty()) {
      if (curr == null) {
        // 锁被释放后重新获取，需要重新解析路径恢复遍历位置
        curr = resolvePaths(startId, startAfters);
      }
      curr = traverseDirInt(startId, curr, startAfters, traverseInfo);
    }
  }

  /**
   * 遍历当前目录，处理直接子节点：文件加入当前批量、目录递归进入，批量达到阈值后提交处理并可能释放锁。
   * 长时间持有读锁超过阈值会主动释放锁再重新获取，避免阻塞NameNode其他操作。
   *
   * @param startId 起始根节点的INode ID
   * @param curr 当前处理的INode节点
   * @param startAfters 当前遍历位置路径数组
   * @param traverseInfo 遍历附加信息
   * @return 如果整个过程持有锁返回当前处理完的INode，如果锁被释放返回null
   * @throws IOException 遍历过程IO异常
   * @throws InterruptedException 遍历中断异常
   */
  protected INode traverseDirInt(final long startId, INode curr,
      List<byte[]> startAfters, TraverseInfo traverseInfo)
      throws IOException, InterruptedException {
    assert dir.hasReadLock();
    assert dir.getFSNamesystem().hasReadLock(RwLockMode.FS);
    // 记录读锁开始时间，用于判断是否超时
    long lockStartTime = timer.monotonicNow();
    Preconditions.checkNotNull(curr, "Current inode can't be null");
    checkINodeReady(startId);
    // 获取当前节点对应的父目录
    final INodeDirectory parent = curr.isDirectory() ? curr.asDirectory()
        : curr.getParent();
    // 获取当前目录的所有子节点列表（当前快照状态）
    ReadOnlyList<INode> children = parent
        .getChildrenList(Snapshot.CURRENT_STATE_ID);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Traversing directory {}", parent.getFullPathName());
    }

    // 获取当前层级的起始遍历位置
    final byte[] startAfter = startAfters.get(startAfters.size() - 1);
    boolean lockReleased = false;
    // 从起始位置之后开始遍历所有子节点
    for (int i = INodeDirectory.nextChild(children, startAfter); i < children
        .size(); ++i) {
      final INode inode = children.get(i);
      // 处理当前inode，如果不是文件返回false
      if (!processFileInode(inode, traverseInfo)) {
        // 不是文件也不是目录，跳过
        if (!inode.isDirectory()) {
          continue;
        }
        // 目录不可遍历，跳过
        if (!canTraverseDir(inode)) {
          continue;
        }
        // 深度优先遍历进入下一级目录，更新路径数组
        curr = inode;
        if (!startAfters.isEmpty()) {
          startAfters.remove(startAfters.size() - 1);
          startAfters.add(curr.getLocalNameBytes());
        }
        startAfters.add(HdfsFileStatus.EMPTY_NAME);
        return lockReleased ? null : curr;
      }
      // 检查当前批次是否达到提交阈值
      if (shouldSubmitCurrentBatch()) {
        // 记录当前处理位置，准备释放锁
        final byte[] currentStartAfter = inode.getLocalNameBytes();
        final String parentPath = parent.getFullPathName();
        lockReleased = true;
        // 释放读锁
        readUnlock();
        // 提交当前批次处理
        submitCurrentBatch(startId);
        try {
          // 限速处理
          throttle();
          // 测试用暂停检查
          checkPauseForTesting();
        } finally {
          // 重新获取读锁
          readLock();
          // 重置锁计时
          lockStartTime = timer.monotonicNow();
        }
        checkINodeReady(startId);

        // 锁释放后目录可能被修改，需要重新解析父目录inode
        FSPermissionChecker pc = dir.getPermissionChecker();
        INode newParent = dir
            .resolvePath(pc, parentPath, FSDirectory.DirOp.READ)
            .getLastINode();
        // 父目录被删除或重建，结束当前遍历
        if (newParent == null || !newParent.equals(parent)) {
          return null;
        }
        // 重新获取子节点列表
        children = parent.getChildrenList(Snapshot.CURRENT_STATE_ID);
        // 计算下一个需要处理的位置，抵消循环自增
        i = INodeDirectory.nextChild(children, currentStartAfter) - 1;
      }
      // 检查持有读锁是否超过阈值，超时则释放锁重入避免长时间阻塞其他操作
      if ((timer.monotonicNow()
          - lockStartTime) > readLockReportingThresholdMs) {
        readUnlock();
        try {
          throttle();
        } finally {
          readLock();
          lockStartTime = timer.monotonicNow();
        }
      }
    }
    // 当前目录遍历完成，回退到上一级目录，更新起始位置
    startAfters.remove(startAfters.size() - 1);
    if (!startAfters.isEmpty()) {
      startAfters.remove(startAfters.size() - 1);
      startAfters.add(curr.getLocalNameBytes());
    }
    curr = curr.getParent();
    return lockReleased ? null : curr;
  }

  /**
   * 锁释放重获取后，重新根据路径数组解析当前遍历位置，找到最近的有效父节点。
   * 如果路径中某一级inode被删除，从最后一个有效父节点恢复遍历。
   *
   * @param startId 起始根节点INode ID
   * @param startAfters 路径数组，存储每一级节点名称字节
   * @return 解析得到的有效父节点，如果起始根节点被删除返回null
   * @throws FileNotFoundException 起始根节点被删除抛出异常
   */
  private INode resolvePaths(final long startId, List<byte[]> startAfters)
      throws IOException {
    // 获取起始根节点
    INode zoneNode = dir.getInode(startId);
    if (zoneNode == null) {
      throw new FileNotFoundException("Zone " + startId + " is deleted.");
    }
    INodeDirectory parent = zoneNode.asDirectory();
    // 逐级解析路径
    for (int i = 0; i < startAfters.size(); ++i) {
      // 最后一级不需要解析，nextChild会自动处理起始位置
      if (i == startAfters.size() - 1) {
        break;
      }
      // 获取当前层级的子节点
      INode curr = parent.getChild(startAfters.get(i),
          Snapshot.CURRENT_STATE_ID);
      if (curr == null) {
        // 当前层级节点不存在，删除所有更低层级路径，从当前父节点继续遍历
        for (; i < startAfters.size(); ++i) {
          startAfters.remove(startAfters.size() - 1);
        }
        break;
      }
      // 进入下一级继续解析
      parent = curr.asDirectory();
    }
    return parent;
  }

  /**
   * 获取FS和目录层级的读锁。
   */
  protected void readLock() {
    dir.getFSNamesystem().readLock(RwLockMode.FS);
    dir.readLock();
  }

  /**
   * 释放FS和目录层级的读锁。
   */
  protected void readUnlock() {
    dir.readUnlock();
    dir.getFSNamesystem().readUnlock(RwLockMode.FS, "FSTreeTraverser");
  }


  /**
   * 测试用抽象方法，检查是否需要暂停遍历。
   * @throws InterruptedException 暂停被中断异常
   */
  protected abstract void checkPauseForTesting() throws InterruptedException;

  /**
   * 处理inode，如果是文件则添加到当前批次。
   * @param inode 需要处理的inode节点
   * @param traverseInfo 遍历附加信息
   * @return true成功添加到批次，false不是文件或不需要处理
   * @throws IOException 处理IO异常
   * @throws InterruptedException 处理中断异常
   */
  protected abstract boolean processFileInode(INode inode,
      TraverseInfo traverseInfo) throws IOException, InterruptedException;

  /**
   * 检查当前批次是否满足提交条件。
   * @return true可以提交批次，false不需要提交
   */
  protected abstract boolean shouldSubmitCurrentBatch();

  /**
   * 检查起始根节点是否可遍历，如果不可遍历抛出异常。
   * @param startId 起始根节点INode ID
   * @throws IOException 节点不可用抛出异常
   */
  protected abstract void checkINodeReady(long startId) throws IOException;

  /**
   * 提交当前批次进行处理。
   * @param startId 起始根节点INode ID
   * @throws IOException 提交IO异常
   * @throws InterruptedException 提交中断异常
   */
  protected abstract void submitCurrentBatch(Long startId)
      throws IOException, InterruptedException;

  /**
   * 遍历限速，控制遍历速度避免影响NameNode其他业务。
   * @throws InterruptedException 限速休眠被中断异常
   */
  protected abstract void throttle() throws InterruptedException;

  /**
   * 检查目录是否可以被遍历。
   * @param inode 目录inode节点
   * @return true可以遍历，false不可遍历
   * @throws IOException 检查过程IO异常
   */
  protected abstract boolean canTraverseDir(INode inode) throws IOException;

  /**
   * 遍历附加信息的封装类，用于传递遍历过程中需要的额外上下文信息。
   */
  public static class TraverseInfo {

  }
}