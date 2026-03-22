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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;

/**
 * 文件级特性类，表示文件处于构建中（正在被写入）的状态。
 * 保存了当前写入该文件的客户端信息，为租约管理和块大小更新提供支持。
 * 是HDFS NameNode中INode特性模式的实现之一，用于标记和处理未关闭的写入中文件。
 */
@InterfaceAudience.Private
public class FileUnderConstructionFeature implements INode.Feature {
  /** 持有该文件租约的客户端名称 */
  private String clientName;
  /** 客户端所在机器标识 */
  private final String clientMachine;

  /**
   * 构造处于构建中文件的特性对象，保存客户端信息。
   * @param clientName 租约持有者客户端名称
   * @param clientMachine 客户端所在机器标识
   */
  public FileUnderConstructionFeature(final String clientName, final String clientMachine) {
    this.clientName = clientName;
    this.clientMachine = clientMachine;
  }

  /**
   * 获取持有租约的客户端名称。
   * @return 客户端名称字符串
   */
  public String getClientName() {
    return clientName;
  }

  /**
   * 更新持有租约的客户端名称。
   * @param clientName 新的客户端名称
   */
  void setClientName(String clientName) {
    this.clientName = clientName;
  }

  /**
   * 获取客户端所在机器标识。
   * @return 客户端机器标识字符串
   */
  public String getClientMachine() {
    return clientMachine;
  }

  /**
   * 根据客户端上报更新文件最后一个块的长度。
   * 用于写入过程中动态更新块大小，只对未完成的最后一块生效。
   *
   * @param f 目标构建中文件对应的INodeFile对象
   * @param lastBlockLength 客户端上报的最后一块长度
   * @throws IOException 当参数校验不通过时抛出异常
   */
  void updateLengthOfLastBlock(INodeFile f, long lastBlockLength)
      throws IOException {
    BlockInfo lastBlock = f.getLastBlock();
    assert (lastBlock != null) : "The last block for path "
        + f.getFullPathName() + " is null when updating its length";
    assert !lastBlock.isComplete()
        : "The last block for path " + f.getFullPathName()
            + " is not under-construction when updating its length";
    lastBlock.setNumBytes(lastBlockLength);
  }

  /**
   * 清理快照删除场景下大小为0的未完成最后块。
   * 当在当前目录删除已存在快照中的文件时，如果该文件最后一个未完成块大小为0，
   * 则将该块加入待删除列表并从文件中移除，避免冗余块占用空间。
   *
   * @param f 目标构建中文件对应的INodeFile对象
   * @param collectedBlocks 收集待删除块的信息对象，用于后续块映射表更新
   */
  void cleanZeroSizeBlock(final INodeFile f,
      final BlocksMapUpdateInfo collectedBlocks) {
    final BlockInfo[] blocks = f.getBlocks();
    if (blocks != null && blocks.length > 0
        && !blocks[blocks.length - 1].isComplete()) {
      BlockInfo lastUC = blocks[blocks.length - 1];
      if (lastUC.getNumBytes() == 0) {
        // 这是一个0长度的未完成块，直接删除即可
        collectedBlocks.addDeleteBlock(lastUC);
        f.removeLastBlock(lastUC);
      }
    }
  }
}