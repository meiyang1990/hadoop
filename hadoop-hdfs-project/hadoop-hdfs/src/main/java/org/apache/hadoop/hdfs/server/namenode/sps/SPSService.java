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
package org.apache.hadoop.hdfs.server.namenode.sps;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;

/**
 * 存储策略满足器（Storage Policy Satisfier, SPS）服务接口，定义了SPS服务的生命周期和核心处理API。
 * SPS负责将HDFS中不符合存储策略的块移动到对应存储类型，实现存储策略的自动满足。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface SPSService {

  /**
   * 初始化SPS服务的辅助依赖，建立NameNode与SPS之间的通信通道。
   *
   * @param ctxt 上下文对象，提供NameNode与SPS之间的通信辅助能力
   */
  void init(Context ctxt);

  /**
   * 启动SPS服务，调用此方法前必须先完成初始化。
   *
   * @param spsMode SPS服务运行模式（内部/外部等）
   */
  void start(StoragePolicySatisfierMode spsMode);

  /**
   * 优雅停止SPS服务，等待工作线程完成当前处理后退出，带超时等待。
   */
  void stopGracefully();

  /**
   * 停止SPS服务，可选择是否强制清除所有待处理路径的标记信息。
   *
   * @param forceStop true表示强制清除所有待处理路径的提示信息，false表示保留
   */
  void stop(boolean forceStop);

  /**
   * 检查SPS服务是否正在运行。
   *
   * @return 正在运行返回true，否则返回false
   */
  boolean isRunning();

  /**
   * 将待处理文件/块信息添加到处理队列，等待满足存储策略。
   *
   * @param itemInfo 需要满足存储策略的文件信息对象
   * @param scanCompleted 目录扫描是否已完成
   */
  void addFileToProcess(ItemInfo itemInfo, boolean scanCompleted);

  /**
   * 批量将多个待处理文件/块信息添加到处理队列。
   *
   * @param startPathId 触发SPS处理的根目录/文件ID
   * @param itemInfoList 待处理的文件信息列表
   * @param scanCompleted 目录扫描是否已完成，该批次为最后一批
   */
  void addAllFilesToProcess(long startPathId, List<ItemInfo> itemInfoList,
      boolean scanCompleted);

  /**
   * 获取当前待处理队列的大小。
   *
   * @return 当前处理队列中的项目数量
   */
  int processingQueueSize();

  /**
   * 获取SPS服务使用的配置对象。
   *
   * @return Hadoop配置对象
   */
  Configuration getConf();

  /**
   * 标记指定路径的扫描流程已完成。
   *
   * @param spsPath 需要标记的路径ID
   */
  void markScanCompletedForPath(long spsPath);

  /**
   * 接收DataNode上报的块移动尝试完成通知，更新SPS内部处理状态。
   *
   * @param dnInfo 完成移动上报的DataNode信息
   * @param storageType 目标存储类型
   * @param block 完成移动尝试的块
   */
  void notifyStorageMovementAttemptFinishedBlk(DatanodeInfo dnInfo,
      StorageType storageType, Block block);
}