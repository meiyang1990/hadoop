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

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.util.DataChecksum;

/** 
 * 管道写流程中正在写入的副本接口，定义了数据块写入过程中副本需要支持的核心操作
 * 用于DataNode处理客户端写入数据块时，管理副本的写入进度、校验和与资源分配
 */
public interface ReplicaInPipeline extends Replica {
  /**
   * 设置已接收的数据字节数
   * @param bytesReceived 已接收的字节数
   */
  void setNumBytes(long bytesReceived);

  /**
   * 获取已确认完成的字节数
   * @return 已确认完成的字节数
   */
  long getBytesAcked();

  /**
   * 设置已确认完成的字节数
   * @param bytesAcked 已确认完成的字节数
   */
  void setBytesAcked(long bytesAcked);

  /**
   * 释放该副本预留的所有磁盘空间
   */
  public void releaseAllBytesReserved();

  /**
   * 从ReplicaInfo中释放预留的磁盘空间
   */
  void releaseReplicaInfoBytesReserved();

  /**
   * 存储最后一个数据块的校验和以及当前数据长度
   * @param dataLength 磁盘上已存储的字节数
   * @param lastChecksum 最后一个数据块的校验和字节数组
   */
  public void setLastChecksumAndDataLen(long dataLength, byte[] lastChecksum);
  
  /**
   * 获取最后一个数据块的校验和以及对应数据块长度
   * @return 包含最后一个块校验和和数据长度的ChunkChecksum对象
   */
  public ChunkChecksum getLastChecksumAndDataLen();
  
  /**
   * 创建当前副本的输出流，分别用于写入数据块文件和校验文件
   *
   * @param isCreate 是否为新建文件
   * @param requestedChecksum 写入器期望使用的校验和配置
   * @return 用于写入的输出流对象，包含数据流和校验流
   * @throws IOException 创建输出流过程中发生IO错误时抛出
   */
  public ReplicaOutputStreams createStreams(boolean isCreate,
      DataChecksum requestedChecksum) throws IOException;

  /**
   * 创建用于写入重启元数据的输出流，用于DataNode快速重启场景
   *
   * @return 用于写入重启元数据的输出流
   * @throws IOException 创建输出流过程中发生IO错误时抛出
   */
  public OutputStream createRestartMetaStream() throws IOException;
  
  /**
   * 获取当前副本的基础信息对象
   * @return 副本信息对象
   */
  ReplicaInfo getReplicaInfo();
  
  /**
   * 设置当前写入该副本的线程
   * @param writer 写入该副本的线程对象
   */
  void setWriter(Thread writer);
  
  /**
   * 中断当前写入线程
   */
  void interruptThread();
  
  /**
   * 尝试将写入线程从旧线程更新为新线程
   * @param prevWriter 期望的旧写入线程
   * @param newWriter 需要设置的新写入线程
   * @return 更新成功返回true，否则返回false
   */
  boolean attemptToSetWriter(Thread prevWriter, Thread newWriter);

  /**
   * 中断写入线程并等待其终止
   * @param xceiverStopTimeout 等待线程终止的超时时间
   * @throws IOException 等待过程被中断时抛出
   */
  void stopWriter(long xceiverStopTimeout) throws IOException;

  /**
   * 让当前线程等待，直到副本写入长度达到指定最小值、线程被中断或超时
   *
   * @param minLength 需要达到的最小写入长度
   * @param time 最大等待时间
   * @param unit 时间单位
   * @throws IOException 当前线程被中断，或超时后仍未达到最小长度时抛出
   */
  void waitForMinLength(long minLength, long time, TimeUnit unit)
      throws IOException;
}