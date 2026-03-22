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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * 文件: org.apache.hadoop.mapreduce.task.reduce.ShuffleHeader
 * 所属模块: hadoop-mapreduce-client-core
 * 核心职责: 定义MapReduce Shuffle阶段的数据头结构，封装Map任务输出数据的元数据信息，用于Map端和Reduce端之间的数据传输
 * 
 * Shuffle Header信息，由Map任务所在节点发送，由Reduce任务的Fetcher线程解析，用于标识传输的Map输出数据
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class ShuffleHeader implements Writable {

  /** Shuffle HTTP请求/响应的头名称 */
  public static final String HTTP_HEADER_NAME = "name";
  public static final String DEFAULT_HTTP_HEADER_NAME = "mapreduce";
  public static final String HTTP_HEADER_VERSION = "version";
  public static final String DEFAULT_HTTP_HEADER_VERSION = "1.0.0";

  /**
   * 允许接受的任务尝试ID的最大长度，防止非法数据
   */
  private static final int MAX_ID_LENGTH = 1000;

  // Map任务ID
  String mapId;
  // 输出数据解压后的长度
  long uncompressedLength;
  // 输出数据压缩后的长度
  int forReduce;
  // 目标Reduce分区编号
  int compressedLength;
  
  /**
   * 空构造方法，用于反序列化
   */
  public ShuffleHeader() { }
  
  /**
   * 构造Shuffle数据头
   * @param mapId Map任务ID
   * @param compressedLength 压缩后数据长度
   * @param uncompressedLength 解压后数据长度
   * @param forReduce 目标Reduce分区编号
   */
  public ShuffleHeader(String mapId, long compressedLength,
      long uncompressedLength, int forReduce) {
    this.mapId = mapId;
    this.compressedLength = compressedLength;
    this.uncompressedLength = uncompressedLength;
    this.forReduce = forReduce;
  }
  
  /**
   * 从输入流反序列化读取Shuffle头信息
   * @param in 输入流
   * @throws IOException 读取失败时抛出异常
   */
  public void readFields(DataInput in) throws IOException {
    mapId = WritableUtils.readStringSafely(in, MAX_ID_LENGTH);
    compressedLength = WritableUtils.readVLong(in);
    uncompressedLength = WritableUtils.readVLong(in);
    forReduce = WritableUtils.readVInt(in);
  }

  /**
   * 将Shuffle头信息序列化写入输出流
   * @param out 输出流
   * @throws IOException 写入失败时抛出异常
   */
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, mapId);
    WritableUtils.writeVLong(out, compressedLength);
    WritableUtils.writeVLong(out, uncompressedLength);
    WritableUtils.writeVInt(out, forReduce);
  }
}