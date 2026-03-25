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

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;

/**
 * 文件表示：DataNode节点上处于块恢复过程中的数据块副本
 * 核心功能：封装待恢复副本的原信息和恢复ID，支持并发恢复的抢占机制：高恢复ID可抢占低恢复ID的恢复流程
 * 恢复ID等于恢复完成后副本将更新到的生成 stamps，用于处理多节点并发块恢复的冲突
 */
public class ReplicaUnderRecovery extends LocalReplica {
  // 待恢复的原始副本
  private LocalReplica original; // original replica to be recovered
  // 恢复ID，同时也是恢复完成后副本将更新到的生成 stamp
  private long recoveryId; // recovery id; it is also the generation stamp 
                           // that the replica will be bumped to after recovery

  /**
   * 构造处于恢复状态的副本对象
   * @param replica 待恢复的原始副本
   * @param recoveryId 本次恢复的恢复ID
   */
  public ReplicaUnderRecovery(ReplicaInfo replica, long recoveryId) {
    super(replica, replica.getVolume(), ((LocalReplica)replica).getDir());
    // 检查原始副本状态是否支持恢复，仅允许已完成、RBW、RWR状态的副本进入恢复
    if ( replica.getState() != ReplicaState.FINALIZED &&
         replica.getState() != ReplicaState.RBW &&
         replica.getState() != ReplicaState.RWR ) {
      throw new IllegalArgumentException("Cannot recover replica: " + replica);
    }
    this.original = (LocalReplica) replica;
    this.recoveryId = recoveryId;
  }

  /**
   * 拷贝构造函数，基于已有恢复副本创建新对象
   * @param from 源恢复副本对象
   */
  public ReplicaUnderRecovery(ReplicaUnderRecovery from) {
    super(from);
    this.original = (LocalReplica) from.getOriginalReplica();
    this.recoveryId = from.getRecoveryID();
  }

  @Override
  public long getRecoveryID() {
    return recoveryId;
  }

  @Override
  public void setRecoveryID(long recoveryId) {
    // 仅允许更新为更大的恢复ID，保证高ID抢占低ID的规则
    if (recoveryId > this.recoveryId) {
      this.recoveryId = recoveryId;
    } else {
      throw new IllegalArgumentException("The new recovery id: " + recoveryId
          + " must be greater than the current one: " + this.recoveryId);
    }
  }

  /**
   * 获取本次恢复的原始待恢复副本
   * @return 原始副本对象
   */
  @Override
  public ReplicaInfo getOriginalReplica() {
    return original;
  }
  
  @Override //ReplicaInfo
  public ReplicaState getState() {
    // 返回恢复中状态标识
    return ReplicaState.RUR;
  }
  
  @Override
  public long getVisibleLength() {
    // 委托原始副本获取可见长度
    return original.getVisibleLength();
  }

  @Override
  public long getBytesOnDisk() {
    // 委托原始副本获取磁盘占用大小
    return original.getBytesOnDisk();
  }

  @Override  //org.apache.hadoop.hdfs.protocol.Block
  public void setBlockId(long blockId) {
    super.setBlockId(blockId);
    original.setBlockId(blockId);
  }

  @Override //org.apache.hadoop.hdfs.protocol.Block
  public void setGenerationStamp(long gs) {
    super.setGenerationStamp(gs);
    original.setGenerationStamp(gs);
  }
  
  @Override //org.apache.hadoop.hdfs.protocol.Block
  public void setNumBytes(long numBytes) {
    super.setNumBytes(numBytes);
    original.setNumBytes(numBytes);
  }
  
  @Override //ReplicaInfo
  public void updateWithReplica(StorageLocation replicaLocation) {
    super.updateWithReplica(replicaLocation);
    original.updateWithReplica(replicaLocation);
  }
  
  @Override //ReplicaInfo
  void setVolume(FsVolumeSpi vol) {
    super.setVolume(vol);
    original.setVolume(vol);
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
    return super.toString()
        + "\n  recoveryId=" + recoveryId
        + "\n  original=" + original;
  }

  /**
   * 创建该恢复副本的恢复信息对象，用于上报给NameNode
   * @return 封装好的副本恢复信息
   */
  @Override
  public ReplicaRecoveryInfo createInfo() {
    return new ReplicaRecoveryInfo(original.getBlockId(), 
        original.getBytesOnDisk(), original.getGenerationStamp(),
        original.getState()); 
  }
}