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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier.DatanodeMap;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.net.NetworkTopology;

/**
 * 文件级注释：存储策略满足器(SPS)与NameNode模块之间的通信接口，定义SPS访问NameNode核心能力的统一契约
 * An interface for the communication between SPS and Namenode module.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface Context {

  /**
   * 检查SPS服务当前是否正在运行
   * @return true表示SPS正在运行，false表示已停止
   */
  boolean isRunning();

  /**
   * 检查NameNode当前是否处于安全模式
   * @return true表示NameNode在安全模式，false表示不在
   */
  boolean isInSafeMode();

  /**
   * 根据目标数据节点信息获取集群网络拓扑结构
   * @param datanodeMap 目标数据节点信息映射
   * @return 集群网络拓扑对象
   */
  NetworkTopology getNetworkTopology(DatanodeMap datanodeMap);

  /**
   * 检查指定文件是否存在于NameNode命名空间中
   * @param filePath 文件路径标识（inode ID）
   * @return true表示文件存在，false表示不存在
   */
  boolean isFileExist(long filePath);

  /**
   * 根据策略ID获取对应的存储策略详情
   * @param policyId 存储策略ID
   * @return 存储策略详细对象
   */
  BlockStoragePolicy getStoragePolicy(byte policyId);

  /**
   * 移除跟踪SPS调用的路径提示，处理完成后清理标记
   * @param spsPath 需要清理的SPS路径标识（inode ID）
   * @throws IOException IO操作异常时抛出
   */
  void removeSPSHint(long spsPath) throws IOException;

  /**
   * 获取集群中存活数据节点的总数量
   * @return 存活数据节点数量
   */
  int getNumLiveDataNodes();

  /**
   * 获取指定文件的元数据信息
   * @param file 文件路径标识（inode ID）
   * @return 文件状态元数据对象
   * @throws IOException IO操作异常时抛出
   */
  HdfsFileStatus getFileInfo(long file) throws IOException;

  /**
   * 获取所有存活数据节点的存储信息报告
   * @return 存活数据节点存储报告数组
   * @throws IOException IO操作异常时抛出
   */
  DatanodeStorageReport[] getLiveDatanodeStorageReport()
      throws IOException;

  /**
   * 获取下一个需要SPS处理的路径
   * @return 待处理路径标识（inode ID），无待处理路径时返回null
   */
  Long getNextSPSPath();

  /**
   * 扫描指定目录下的所有文件，收集不符合存储策略的块并添加到处理队列
   * @param filePath 根目录路径标识（inode ID）
   * @throws IOException IO操作异常时抛出
   * @throws InterruptedException 线程中断时抛出
   */
  void scanAndCollectFiles(long filePath)
      throws IOException, InterruptedException;

  /**
   * 提交块移动任务，由NameNode调度数据节点执行块迁移
   * @param blkMovingInfo 块移动信息，包含源位置、目标位置和存储类型等必要信息
   * @throws IOException IO操作异常时抛出
   */
  void submitMoveTask(BlockMovingInfo blkMovingInfo) throws IOException;

  /**
   * 通知SPS模块块移动尝试已完成，由SPS判断是否需要重试
   * @param moveAttemptFinishedBlks 移动尝试已完成的块数组
   */
  void notifyMovementTriedBlocks(Block[] moveAttemptFinishedBlks);
}