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

import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import java.util.Queue;

/**
 * 数据节点管理员监控接口，定义退役/维护操作监控的统一规范，由DatanodeAdminManager实例化具体实现
 * 负责跟踪处于退役、维护等管理状态的数据节点，监控数据块迁移进度
 */
public interface DatanodeAdminMonitorInterface extends Runnable {
  /**
   * 停止跟踪指定数据节点的管理进度
   * @param dn 目标数据节点描述符
   */
  void stopTrackingNode(DatanodeDescriptor dn);

  /**
   * 开始跟踪指定数据节点的管理进度
   * @param dn 目标数据节点描述符
   */
  void startTrackingNode(DatanodeDescriptor dn);

  /**
   * 获取等待处理的节点数量
   * @return 待处理节点数
   */
  int getPendingNodeCount();

  /**
   * 获取当前正在跟踪的节点总数
   * @return 跟踪中节点数
   */
  int getTrackedNodeCount();

  /**
   * 获取本次周期内已检查块的节点数量
   * @return 已检查节点数
   */
  int getNumNodesChecked();

  /**
   * 获取等待处理的数据节点队列
   * @return 待处理节点队列
   */
  Queue<DatanodeDescriptor> getPendingNodes();

  /**
   * 获取已取消处理的数据节点队列
   * @return 已取消节点队列
   */
  Queue<DatanodeDescriptor> getCancelledNodes();

  /**
   * 设置关联的块管理器
   * @param bm 块管理器实例
   */
  void setBlockManager(BlockManager bm);

  /**
   * 设置关联的数据节点管理员
   * @param dnm 数据节点管理员实例
   */
  void setDatanodeAdminManager(DatanodeAdminManager dnm);

  /**
   * 设置关联的名称系统
   * @param ns 名称系统实例
   */
  void setNameSystem(Namesystem ns);

  /**
   * 获取待处理复制请求的数量限制
   * @return 待处理复制请求上限
   */
  int getPendingRepLimit();

  /**
   * 设置待处理复制请求的数量限制
   * @param pendingRepLimit 待处理复制请求上限
   */
  void setPendingRepLimit(int pendingRepLimit);

  /**
   * 获取每次加锁处理的块数量
   * @return 每次加锁处理的块数
   */
  int getBlocksPerLock();

  /**
   * 设置每次加锁处理的块数量
   * @param blocksPerLock 每次加锁处理的块数
   */
  void setBlocksPerLock(int blocksPerLock);
}