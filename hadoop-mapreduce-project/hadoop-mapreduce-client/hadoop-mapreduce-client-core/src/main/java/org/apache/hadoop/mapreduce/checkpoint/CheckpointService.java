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
package org.apache.hadoop.mapreduce.checkpoint;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * 检查点服务接口，为MapReduce任务提供任务状态的存储与恢复能力
 * 
 * 检查点具备以下特性：原子性、单写者、一次写入、多读者、可多次读取。
 * 实现该特性的方式是：仅在检查点提交后才向用户返回CheckpointID，并且禁止检查点重新打开写入。
 * 
 * 持久性、可用性、压缩、垃圾回收、配额等非功能属性由具体实现决定，本接口不做强制要求。
 * 
 * 该接口被设计为检查点服务的基础构建块，可以在其上构建更丰富的上层接口（例如对象序列化、检查点元数据与溯源管理等框架功能）。
 */
public interface CheckpointService {

  /**
   * 检查点写入通道，继承自WritableByteChannel，用于向检查点写入数据
   */
  public interface CheckpointWriteChannel extends WritableByteChannel { }

  /**
   * 检查点读取通道，继承自ReadableByteChannel，用于从检查点读取数据
   */
  public interface CheckpointReadChannel extends ReadableByteChannel { }

  /**
   * 创建一个新的检查点，并返回可写入该检查点的通道
   * 调用此方法时用户不需要知道检查点的名称/位置，并且CheckpointID直到提交完成后才会返回给用户
   * 这种设计保证了写入操作的原子性
   * @return 可用于写入检查点的通道
   * @throws IOException 如果创建过程发生IO异常
   * @throws InterruptedException 如果创建过程被中断
   */
  public CheckpointWriteChannel create()
    throws IOException, InterruptedException;

  /**
   * 提交已完成写入的检查点，返回可用于后续读取该检查点的CheckpointID
   * 提交操作保证了检查点的原子性，只有提交完成后检查点才对外可见
   * @param ch 已完成写入的检查点写入通道
   * @return 可用于后续读取该检查点的唯一标识CheckpointID
   * @throws IOException 如果提交过程发生IO异常
   * @throws InterruptedException 如果提交过程被中断
   */
  public CheckpointID commit(CheckpointWriteChannel ch)
    throws IOException, InterruptedException;

  /**
   * 中止当前正在写入的检查点，丢弃已写入的内容
   * 垃圾回收策略由具体实现决定，中止后的检查点不会对外暴露CheckpointID，因此无法被访问
   * @param ch 需要中止的检查点写入通道
   * @throws IOException 如果中止过程发生IO异常
   * @throws InterruptedException 如果中止过程被中断
   */
  public void abort(CheckpointWriteChannel ch)
      throws IOException, InterruptedException;

  /**
   * 根据CheckpointID打开已提交的检查点，返回用于读取数据的通道
   * @param id 需要打开的检查点唯一标识
   * @return 可用于读取检查点数据的通道
   * @throws IOException 如果打开过程发生IO异常
   * @throws InterruptedException 如果打开过程被中断
   */
  public CheckpointReadChannel open(CheckpointID id)
    throws IOException, InterruptedException;

  /**
   * 删除指定CheckpointID对应的检查点，释放存储资源
   * @param id 需要删除的检查点唯一标识
   * @return 删除成功返回true，失败返回false
   * @throws IOException 如果删除过程发生IO异常
   * @throws InterruptedException 如果删除过程被中断
   */
  public boolean delete(CheckpointID id)
    throws IOException, InterruptedException;

}