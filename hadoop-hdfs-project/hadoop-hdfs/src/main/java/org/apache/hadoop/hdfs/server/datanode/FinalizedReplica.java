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
import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;

/**
 * 已完成写入的 finalized 数据块副本，描述已经完整写入、可以对外提供读取服务的数据块副本。
 * 此类表示写入完成的数据块，不支持恢复修改等操作，DataNode上绝大多数正常对外服务的块都属于此类。
 */
public class FinalizedReplica extends LocalReplica {
  // 最后一个不完整数据块的校验和
  private byte[] lastPartialChunkChecksum;
  // 元数据文件长度，缓存计算结果，-1表示尚未计算
  private int metaLength = -1;
  /**
   * 构造 finalized 数据块副本。
   * @param blockId 块ID
   * @param len 副本长度
   * @param genStamp 副本世代时间戳
   * @param vol 副本所在存储卷
   * @param dir 块文件和元数据文件所在目录
   */
  public FinalizedReplica(long blockId, long len, long genStamp,
      FsVolumeSpi vol, File dir) {
    this(blockId, len, genStamp, vol, dir, null);
  }

  /**
   * 构造带最后一个不完整块校验和的 finalized 数据块副本。
   * @param blockId 块ID
   * @param len 副本长度
   * @param genStamp 副本世代时间戳
   * @param vol 副本所在存储卷
   * @param dir 块文件和元数据文件所在目录
   * @param checksum 最后一个不完整块的校验和
   */
  public FinalizedReplica(long blockId, long len, long genStamp,
      FsVolumeSpi vol, File dir, byte[] checksum) {
    super(blockId, len, genStamp, vol, dir);
    this.setLastPartialChunkChecksum(checksum);
  }

  /**
   * 基于已有Block对象构造 finalized 数据块副本。
   * @param block 块对象
   * @param vol 副本所在存储卷
   * @param dir 块文件和元数据文件所在目录
   */
  public FinalizedReplica(Block block, FsVolumeSpi vol, File dir) {
    this(block, vol, dir, null);
  }

  /**
   * 基于已有Block对象构造，指定最后一个不完整块校验和。
   * @param block 块对象
   * @param vol 副本所在存储卷
   * @param dir 块文件和元数据文件所在目录
   * @param checksum 最后一个不完整块的校验和
   */
  public FinalizedReplica(Block block, FsVolumeSpi vol, File dir,
      byte[] checksum) {
    super(block, vol, dir);
    this.setLastPartialChunkChecksum(checksum);
  }

  /**
   * 拷贝构造方法，基于已有 FinalizedReplica创建副本。
   * @param from 源对象
   */
  public FinalizedReplica(FinalizedReplica from) {
    super(from);
    this.setLastPartialChunkChecksum(from.getLastPartialChunkChecksum());
  }

  @Override  // ReplicaInfo
  /**
   * 获取当前副本状态，返回 finalized 状态。
   * @return FINALIZED 已完成状态
   */
  public ReplicaState getState() {
    return ReplicaState.FINALIZED;
  }
  
  @Override
  /**
   * 获取副本对外可见长度，已完成块所有字节都可见。
   * @return 副本总长度
   */
  public long getVisibleLength() {
    return getNumBytes();       // all bytes are visible
  }

  @Override
  /**
   * 获取磁盘上占用的字节数。
   * @return 副本数据长度
   */
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

  @Override
  /**
   * 获取原始副本，已完成块不支持该操作，抛出异常。
   * @return 永远不会正常返回，总是抛出异常
   */
  public ReplicaInfo getOriginalReplica() {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support getOriginalReplica");
  }

  @Override
  /**
   * 获取恢复ID，已完成块不支持该操作，抛出异常。
   * @return 永远不会正常返回，总是抛出异常
   */
  public long getRecoveryID() {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support getRecoveryID");
  }

  @Override
  /**
   * 设置恢复ID，已完成块不支持该操作，抛出异常。
   * @param recoveryId 恢复ID
   * @return 永远不会正常返回，总是抛出异常
   */
  public void setRecoveryID(long recoveryId) {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support setRecoveryID");
  }

  @Override
  /**
   * 创建恢复信息，已完成块不支持该操作，抛出异常。
   * @return 永远不会正常返回，总是抛出异常
   */
  public ReplicaRecoveryInfo createInfo() {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support createInfo");
  }

  @Override
  /**
   * 获取元数据文件长度，缓存计算结果避免重复计算。
   * @return 元数据文件长度
   */
  public long getMetadataLength() {
    // 首次调用时计算并缓存结果
    if (metaLength < 0) {
      metaLength = (int)super.getMetadataLength();
    }
    return metaLength;
  }

  /**
   * 获取最后一个不完整块的校验和。
   * @return 最后一个不完整块的校验和字节数组
   */
  public byte[] getLastPartialChunkChecksum() {
    return lastPartialChunkChecksum;
  }

  /**
   * 设置最后一个不完整块的校验和。
   * @param checksum 校验和字节数组
   */
  public void setLastPartialChunkChecksum(byte[] checksum) {
    lastPartialChunkChecksum = checksum;
  }

  /**
   * 从磁盘加载最后一个不完整块的校验和。
   * @throws IOException 加载过程IO异常
   */
  public void loadLastPartialChunkChecksum()
      throws IOException {
    // 委托存储卷加载块文件和元文件中的最后一个不完整块校验和
    byte[] lastChecksum = getVolume().loadLastPartialChunkChecksum(
        getBlockFile(), getMetaFile());
    setLastPartialChunkChecksum(lastChecksum);
  }
}