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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.security.AccessControlException;

/** 
 * 块集合接口，供块管理器使用，暴露文件/目录对应的块集合的核心属性与操作。
 * 是连续块文件和纠删码 striped 文件的抽象统一接口。
 */
@InterfaceAudience.Private
public interface BlockCollection {
  /**
   * 获取当前块集合的最后一个块信息。
   * @return 最后一个块的BlockInfo对象
   */
  BlockInfo getLastBlock();

  /** 
   * 计算当前块集合对应的内容摘要。
   * @param bsps 块存储策略套件，用于获取存储策略信息
   * @return 计算得到的内容摘要，包含大小、文件数、目录数等信息
   * @throws AccessControlException 权限校验失败时抛出
   */
  ContentSummary computeContentSummary(BlockStoragePolicySuite bsps)
      throws AccessControlException;

  /**
   * 获取当前块集合包含的块/块组数量。
   * @return 块或块组的数量
   */ 
  int numBlocks();

  /**
   * 获取当前块集合包含的所有块信息，支持连续块和纠删码条带块。
   * @return 块信息数组
   */
  BlockInfo[] getBlocks();

  /**
   * 获取当前块集合首选块大小。
   * @return 首选块大小，单位为字节
   */
  long getPreferredBlockSize();

  /**
   * 获取当前块集合块副本数。
   * @return 块副本数，纠删码文件返回0
   */
  short getPreferredBlockReplication();

  /**
   * 获取当前块集合存储策略ID。
   * @return 存储策略ID
   */
  byte getStoragePolicyID();

  /**
   * 获取当前块集合的名称，通常为文件路径。
   * @return 块集合名称
   */
  String getName();

  /**
   * 设置指定索引位置的块，支持连续块和条带块。
   * @param index 块在集合中的索引
   * @param blk 要设置的块信息
   */
  void setBlock(int index, BlockInfo blk);

  /**
   * 将当前集合的最后一个块转换为构建中状态，并设置块的目标数据节点位置。
   * @param lastBlock 最后一个块信息
   * @param targets 目标数据节点存储信息数组
   * @throws IOException 转换失败时抛出IO异常
   */
  void convertLastBlockToUC(BlockInfo lastBlock,
      DatanodeStorageInfo[] targets) throws IOException;

  /**
   * 判断当前块集合是否处于构建中状态（文件正在写入）。
   * @return 是构建中返回true，否则返回false
   */
  boolean isUnderConstruction();

  /**
   * 判断当前块集合是否为纠删码条带格式。
   * @return 是条带格式返回true，否则返回false
   */
  boolean isStriped();

  /**
   * 获取当前块集合的唯一ID。
   * @return 块集合ID，通常对应文件的INode ID
   */
  long getId();
}