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

import java.net.URI;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.RawPathHandle;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;

/**
 * 文件路径：hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/FinalizedProvidedReplica.java
 * 
 * 已完成的外部提供数据块副本实现类
 * 用于表示存储在第三方外部存储系统（如云存储）中、已经完成写入的数据块副本，
 * 是HDFS异构存储（provided storage）特性中数据块副本的核心实现类。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FinalizedProvidedReplica extends ProvidedReplica {

  /**
   * 构造已完成的外部提供数据块副本
   * @param blockId 数据块ID
   * @param fileURI 外部存储文件URI
   * @param fileOffset 文件内数据块偏移量
   * @param blockLen 数据块长度
   * @param genStamp 数据块生成时间戳
   * @param pathHandle 外部存储路径句柄
   * @param volume DataNode存储卷
   * @param conf Hadoop配置
   * @param remoteFS 外部文件系统实例
   */
  public FinalizedProvidedReplica(long blockId, URI fileURI, long fileOffset,
      long blockLen, long genStamp, PathHandle pathHandle, FsVolumeSpi volume,
      Configuration conf, FileSystem remoteFS) {
    super(blockId, fileURI, fileOffset, blockLen, genStamp, pathHandle, volume,
        conf, remoteFS);
  }

  /**
   * 从文件区域构造已完成的外部提供数据块副本
   * @param fileRegion 文件区域信息，包含数据块和存储位置信息
   * @param volume DataNode存储卷
   * @param conf Hadoop配置
   * @param remoteFS 外部文件系统实例
   */
  public FinalizedProvidedReplica(FileRegion fileRegion, FsVolumeSpi volume,
      Configuration conf, FileSystem remoteFS) {
    super(fileRegion.getBlock().getBlockId(),
        fileRegion.getProvidedStorageLocation().getPath().toUri(),
        fileRegion.getProvidedStorageLocation().getOffset(),
        fileRegion.getBlock().getNumBytes(),
        fileRegion.getBlock().getGenerationStamp(),
        new RawPathHandle(ByteBuffer
            .wrap(fileRegion.getProvidedStorageLocation().getNonce())),
        volume, conf, remoteFS);
  }

  /**
   * 通过路径前缀后缀构造已完成的外部提供数据块副本
   * @param blockId 数据块ID
   * @param pathPrefix 外部存储路径前缀
   * @param pathSuffix 外部存储路径后缀
   * @param fileOffset 文件内数据块偏移量
   * @param blockLen 数据块长度
   * @param genStamp 数据块生成时间戳
   * @param pathHandle 外部存储路径句柄
   * @param volume DataNode存储卷
   * @param conf Hadoop配置
   * @param remoteFS 外部文件系统实例
   */
  public FinalizedProvidedReplica(long blockId, Path pathPrefix,
      String pathSuffix, long fileOffset, long blockLen, long genStamp,
      PathHandle pathHandle, FsVolumeSpi volume, Configuration conf,
      FileSystem remoteFS) {
    super(blockId, pathPrefix, pathSuffix, fileOffset, blockLen,
        genStamp, pathHandle, volume, conf, remoteFS);
  }

  /**
   * 获取副本状态
   * @return 固定返回FINALIZED（已完成状态）
   */
  @Override
  public ReplicaState getState() {
    return ReplicaState.FINALIZED;
  }

  /**
   * 获取磁盘上占用的字节数
   * @return 数据块长度，外部副本不占用本地磁盘空间，此处返回逻辑长度
   */
  @Override
  public long getBytesOnDisk() {
    return getNumBytes();
  }

  /**
   * 获取副本可见长度
   * @return 数据块总长度，已完成副本所有字节都可见
   */
  @Override
  public long getVisibleLength() {
    return getNumBytes(); //all bytes are visible
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
   * 获取原始副本，已完成外部副本不支持此操作
   * @return 永远抛出异常
   */
  @Override
  public ReplicaInfo getOriginalReplica() {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support getOriginalReplica");
  }

  /**
   * 获取恢复ID，已完成外部副本不支持恢复操作
   * @return 永远抛出异常
   */
  @Override
  public long getRecoveryID() {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support getRecoveryID");
  }

  /**
   * 设置恢复ID，已完成外部副本不支持恢复操作
   * @param recoveryId 恢复ID
   */
  @Override
  public void setRecoveryID(long recoveryId) {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support setRecoveryID");
  }

  /**
   * 创建副本恢复信息，已完成外部副本不支持恢复操作
   * @return 永远抛出异常
   */
  @Override
  public ReplicaRecoveryInfo createInfo() {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support createInfo");
  }
}