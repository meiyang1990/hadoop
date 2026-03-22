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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileAttributes;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat.ReferenceMap;

/**
 * 文件目录INodeFile在两个快照之间的差异信息记录，用于快照的回滚与差异对比
 * 保存了快照创建时文件的大小和块列表信息，支持快照删除后的数据空间回收
 */
public class FileDiff extends
    AbstractINodeDiff<INodeFile, INodeFileAttributes, FileDiff> {

  /** 快照创建时刻的文件大小 */
  private final long fileSize;
  /** 快照中文件块列表的副本，仅在文件被截断时使用，保存截断前的块信息 */
  private BlockInfo[] blocks;

  /**
   * 构造文件差异对象，基于当前文件状态创建快照差异
   * @param snapshotId 快照ID
   * @param file 当前文件INode
   */
  FileDiff(int snapshotId, INodeFile file) {
    super(snapshotId, null, null);
    fileSize = file.computeFileSize();
    blocks = null;
  }

  /**
   * 构造文件差异对象，用于FSImage加载时恢复快照差异
   * @param snapshotId 快照ID
   * @param snapshotINode 快照保存的文件属性
   * @param posteriorDiff 后一个快照的差异信息
   * @param fileSize 快照保存的文件大小
   */
  FileDiff(int snapshotId, INodeFileAttributes snapshotINode,
      FileDiff posteriorDiff, long fileSize) {
    super(snapshotId, snapshotINode, posteriorDiff);
    this.fileSize = fileSize;
    blocks = null;
  }

  /**
   * 获取快照保存的文件大小
   * @return 快照中的文件大小
   */
  public long getFileSize() {
    return fileSize;
  }

  /**
   * 将当前文件的块引用拷贝到快照中，仅保留到快照文件大小对应的块，仅可调用一次
   * 用于文件截断后保存截断前的块信息，支持快照回滚
   * @param blocks 当前文件的块数组
   */
  public void setBlocks(BlockInfo[] blocks) {
    if(this.blocks != null)
      return;
    // 计算快照文件大小对应的实际块数量
    int numBlocks = 0;
    for(long s = 0; numBlocks < blocks.length && s < fileSize; numBlocks++)
      s += blocks[numBlocks].getNumBytes();
    // 拷贝所需块，截断多余块
    this.blocks = Arrays.copyOf(blocks, numBlocks);
  }

  /**
   * 获取快照保存的块列表
   * @return 快照中的块数组
   */
  public BlockInfo[] getBlocks() {
    return blocks;
  }

  /**
   * 合并后续快照差异，回收不再被任何快照引用的块空间
   * @param reclaimContext 空间回收上下文，记录回收信息
   * @param currentINode 当前文件INode
   * @param posterior 后续的文件差异
   */
  @Override
  void combinePosteriorAndCollectBlocks(
      INode.ReclaimContext reclaimContext, INodeFile currentINode,
      FileDiff posterior) {
    FileWithSnapshotFeature sf = currentINode.getFileWithSnapshotFeature();
    assert sf != null : "FileWithSnapshotFeature is null";
    sf.updateQuotaAndCollectBlocks(reclaimContext, currentINode, posterior);
  }
  
  @Override
  public String toString() {
    return super.toString() + " fileSize=" + fileSize + ", rep="
        + (snapshotINode == null? "?": snapshotINode.getFileReplication());
  }

  /**
   * 将文件差异序列化写入FSImage
   * @param out 数据输出流
   * @param referenceMap 引用映射表，用于去重
   * @throws IOException 序列化异常
   */
  @Override
  void write(DataOutput out, ReferenceMap referenceMap) throws IOException {
    writeSnapshot(out);
    out.writeLong(fileSize);

    // 写入快照文件属性
    if (snapshotINode != null) {
      out.writeBoolean(true);
      FSImageSerialization.writeINodeFileAttributes(snapshotINode, out);
    } else {
      out.writeBoolean(false);
    }
  }

  /**
   * 销毁当前差异并回收不再被引用的存储空间
   * @param reclaimContext 空间回收上下文
   * @param currentINode 当前文件INode
   */
  @Override
  void destroyDiffAndCollectBlocks(INode.ReclaimContext reclaimContext,
      INodeFile currentINode) {
    currentINode.getFileWithSnapshotFeature().updateQuotaAndCollectBlocks(
        reclaimContext, currentINode, this);
  }

  /**
   * 销毁快照块列表，将所有块添加到待删除集合，完成空间回收
   * @param collectedBlocks 收集待删除块的信息对象
   */
  public void destroyAndCollectSnapshotBlocks(
      BlocksMapUpdateInfo collectedBlocks) {
    if (blocks == null || collectedBlocks == null) {
      return;
    }
    for (BlockInfo blk : blocks) {
      collectedBlocks.addDeleteBlock(blk);
    }
    blocks = null;
  }
}