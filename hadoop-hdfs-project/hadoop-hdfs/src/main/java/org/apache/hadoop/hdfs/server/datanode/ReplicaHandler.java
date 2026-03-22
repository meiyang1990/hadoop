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

import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;

import java.io.Closeable;
import java.io.IOException;

/**
 * 文件: org.apache.hadoop.hdfs.server.datanode.ReplicaHandler
 * 
 * 数据节点正在写入的副本处理器，封装了正在写入的副本对象及其所在卷的引用
 * 用于管理副本写入过程中的资源持有，保证卷引用计数正确，防止资源被提前释放
 */
public class ReplicaHandler implements Closeable {
  private final ReplicaInPipeline replica;
  private final FsVolumeReference volumeReference;

  /**
   * 构造副本处理器，封装正在写入的副本和对应卷引用
   * @param replica 正在写入的管道副本对象
   * @param reference 副本所在数据卷的引用对象
   */
  public ReplicaHandler(
      ReplicaInPipeline replica, FsVolumeReference reference) {
    this.replica = replica;
    this.volumeReference = reference;
  }

  /**
   * 关闭处理器，释放卷引用，递减卷的引用计数
   * @throws IOException 关闭卷引用时可能抛出IO异常
   */
  @Override
  public void close() throws IOException {
    if (this.volumeReference != null) {
      volumeReference.close();
    }
  }

  /**
   * 获取当前处理器封装的正在写入的副本对象
   * @return 正在写入的管道副本对象
   */
  public ReplicaInPipeline getReplica() {
    return replica;
  }
}