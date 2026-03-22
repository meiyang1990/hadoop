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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;

/**
 * 文件概览：HDFS DataNode 节点上等待恢复的副本实现类，属于数据块恢复流程中的一个状态载体
 * 核心职责：表示等待被恢复的未完成写入的数据块副本，承载该状态下副本的元数据与行为约束
 * 
 * 业务背景：DataNode重启后，所有原位于rbw（正在写入）目录的副本都会被加载为此类型；
 * 等待恢复的副本既不提供读服务，也不参与管线恢复，会在租约恢复流程中被处理或过期淘汰。
 */
public class ReplicaWaitingToBeRecovered extends LocalReplica {

  /**
   * 构造等待恢复副本对象，通过参数直接指定各元数据属性
   * @param blockId 数据块ID
   * @param len 副本长度
   * @param genStamp 副本生成时间戳
   * @param vol 副本所在的存储卷
   * @param dir 数据块和元数据文件所在的目录
   */
  public ReplicaWaitingToBeRecovered(long blockId, long len, long genStamp,
      FsVolumeSpi vol, File dir) {
    super(blockId, len, genStamp, vol, dir);
  }
  
  /**
   * 构造等待恢复副本对象，通过Block对象传入块元数据
   * @param block 数据块对象，包含块ID、长度、生成时间戳
   * @param vol 副本所在的存储卷
   * @param dir 数据块和元数据文件所在的目录
   */
  public ReplicaWaitingToBeRecovered(Block block, FsVolumeSpi vol, File dir) {
    super(block, vol, dir);
  }
  
  /**
   * 拷贝构造函数，基于已有等待恢复副本创建新对象
   * @param from 源等待恢复副本对象
   */
  public ReplicaWaitingToBeRecovered(ReplicaWaitingToBeRecovered from) {
    super(from);
  }

  /**
   * 获取当前副本的状态枚举
   * @return 返回RWR（等待恢复）状态
   */
  @Override //ReplicaInfo
  public ReplicaState getState() {
    return ReplicaState.RWR;
  }
  
  /**
   * 获取当前副本对外可见的长度，等待恢复的副本不对外提供读服务
   * @return 固定返回-1，表示无可见数据
   */
  @Override //ReplicaInfo
  public long getVisibleLength() {
    return -1;  //no bytes are visible
  }
  
  @Override
  public long getBytesOnDisk() {
    return getNumBytes();
  }

  @Override  // Object
  public boolean equals(Object o) {
    return super.equals(o);
  }
  
  @Override  // Object
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    return super.toString();
  }

  /**
   * 获取原始副本，当前类型不支持该操作
   * @return 永远抛出不支持操作异常
   */
  @Override
  public ReplicaInfo getOriginalReplica() {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support getOriginalReplica");
  }

  /**
   * 获取恢复ID，当前类型不支持该操作
   * @return 永远抛出不支持操作异常
   */
  @Override
  public long getRecoveryID() {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support getRecoveryID");
  }

  /**
   * 设置恢复ID，当前类型不支持该操作
   * @param recoveryId 恢复ID
   * @return 永远抛出不支持操作异常
   */
  @Override
  public void setRecoveryID(long recoveryId) {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support getRecoveryID");
  }

  /**
   * 创建副本恢复信息，当前类型不支持该操作
   * @return 永远抛出不支持操作异常
   */
  @Override
  public ReplicaRecoveryInfo createInfo() {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support createInfo");
  }
}