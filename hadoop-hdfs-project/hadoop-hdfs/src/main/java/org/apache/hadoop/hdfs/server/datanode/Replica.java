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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

/** 
 * 数据节点存储的块副本抽象接口，定义了所有块副本类型必须实现的基础方法
 * 该接口为DataNode提供统一的副本操作契约，支持不同状态和类型副本的统一管理
 */
@InterfaceAudience.Private
public interface Replica {
  /**
   * 获取该副本对应的块ID
   * @return 块ID
   */
  public long getBlockId();

  /**
   * 获取该副本的生成时间戳，用于版本识别
   * @return 生成时间戳
   */
  public long getGenerationStamp();

  /**
   * 获取该副本当前所处的状态
   * @return 副本状态枚举值
   */
  public ReplicaState getState();

  /**
   * 获取已经接收到的字节数，用于写入过程中统计进度
   * @return 已经接收到的字节数
   */
  public long getNumBytes();
  
  /**
   * 获取已经写入磁盘的字节数
   * @return 已经落盘的字节数
   */
  public long getBytesOnDisk();

  /**
   * 获取对读者可见的字节长度，部分写入完成的副本仅可读取已完成部分
   * @return 对读者可见的字节长度
   */
  public long getVisibleLength();

  /**
   * 获取存储该副本的卷存储UUID
   * @return 卷存储UUID
   */
  public String getStorageUuid();

  /**
   * 判断该副本所在存储是否为内存临时存储
   * @return 如果是RAM-backed存储返回true，否则返回false
   */
  public boolean isOnTransientStorage();

  /**
   * 获取存储该副本的文件卷对象
   * @return 存储该副本的FsVolumeSpi实例
   */
  public FsVolumeSpi getVolume();
}