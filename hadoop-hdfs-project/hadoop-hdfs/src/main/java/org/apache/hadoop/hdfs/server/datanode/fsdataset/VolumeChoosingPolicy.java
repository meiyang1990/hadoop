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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 文件存储卷选择策略接口，定义DataNode为数据块副本选择存储卷的策略规范
 * 为HDFS DataNode存储层提供可扩展的存储卷选择策略支持
 */
@InterfaceAudience.Private
public interface VolumeChoosingPolicy<V extends FsVolumeSpi> {

  /**
   * 根据给定的可用存储卷列表和待存储副本大小，选择一个合适的存储卷存放数据块副本
   * 
   * @param volumes 可用的存储卷列表，调用方需要保证对该列表的线程安全访问
   * @param replicaSize 待存储副本的大小，单位为字节
   * @param storageId NameNode指定的期望存储卷ID，大多数策略可忽略该参数
   * @return 选中的存储卷对象
   * @throws IOException 当所有磁盘都不可用或者没有足够空间存储副本时抛出
   */
  V chooseVolume(List<V> volumes, long replicaSize, String storageId)
      throws IOException;
}