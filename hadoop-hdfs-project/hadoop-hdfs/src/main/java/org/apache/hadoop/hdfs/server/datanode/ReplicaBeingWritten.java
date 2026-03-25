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

/**
 * 表示正在写入的数据块副本，这类副本由DFS客户端发起的写入流水线创建。
 * 处于RBW(Replica Being Written)状态，是写入过程中存储在DataNode的数据块副本。
 */
public class ReplicaBeingWritten extends LocalReplicaInPipeline {
  /**
   * 构造零长度的待写入副本。
   * @param blockId 块ID
   * @param genStamp 副本生成时间戳
   * @param vol 副本所在的存储卷
   * @param dir 存储块文件和元数据文件的目录
   * @param bytesToReserve 根据预估最大块长度预留给本副本的磁盘空间
   */
  public ReplicaBeingWritten(long blockId, long genStamp,
        FsVolumeSpi vol, File dir, long bytesToReserve) {
    super(blockId, genStamp, vol, dir, bytesToReserve);
  }

  /**
   * 构造待写入副本。
   * @param block 数据块对象
   * @param vol 副本所在的存储卷
   * @param dir 存储块文件和元数据文件的目录
   * @param writer 正在写入该副本的线程
   */
  public ReplicaBeingWritten(Block block,
      FsVolumeSpi vol, File dir, Thread writer) {
    super(block, vol, dir, writer);
  }

  /**
   * 构造指定长度的待写入副本。
   * @param blockId 块ID
   * @param len 副本当前长度
   * @param genStamp 副本生成时间戳
   * @param vol 副本所在的存储卷
   * @param dir 存储块文件和元数据文件的目录
   * @param writer 正在写入该副本的线程
   * @param bytesToReserve 根据预估最大块长度预留给本副本的磁盘空间
   */
  public ReplicaBeingWritten(long blockId, long len, long genStamp,
      FsVolumeSpi vol, File dir, Thread writer, long bytesToReserve) {
    super(blockId, len, genStamp, vol, dir, writer, bytesToReserve);
  }

  /**
   * 拷贝构造函数，基于已有副本创建新副本。
   * @param from 拷贝来源副本对象
   */
  public ReplicaBeingWritten(ReplicaBeingWritten from) {
    super(from);
  }

  /**
   * 获取副本对NameNode可见的长度，所有已确认写入的字节都对NameNode可见。
   * @return 已确认写入的字节数
   */
  @Override
  public long getVisibleLength() {
    return getBytesAcked();       // all acked bytes are visible
  }

  /**
   * 获取当前副本的状态。
   * @return 返回RBW状态，表示该副本正在被写入
   */
  @Override   //ReplicaInfo
  public ReplicaState getState() {
    return ReplicaState.RBW;
  }
  
  @Override  // Object
  public boolean equals(Object o) {
    return super.equals(o);
  }
  
  @Override  // Object
  public int hashCode() {
    return super.hashCode();
  }
}