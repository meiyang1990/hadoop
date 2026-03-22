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

import java.io.InputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;

import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapreduce.TaskAttemptID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Map输出结果内存存储实现类，用于Reduce阶段shuffle过程中将Map输出缓存到内存中，
 * 避免磁盘IO，提升shuffle性能，适合小尺寸的Map输出结果。
 * 继承自IFileWrappedMapOutput，实现基于内存的Map输出读取与管理。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class InMemoryMapOutput<K, V> extends IFileWrappedMapOutput<K, V> {
  private static final Logger LOG =
      LoggerFactory.getLogger(InMemoryMapOutput.class);
  // 存储Map输出数据的内存缓冲区
  private final byte[] memory;
  // 绑定到内存缓冲区的字节输出流
  private BoundedByteArrayOutputStream byteStream;
  // 压缩编解码器，用于对压缩的Map输出进行解压
  private final CompressionCodec codec;
  // 解压处理器，从编解码器池获取
  private final Decompressor decompressor;

  /**
   * 构造内存版Map输出对象，初始化内存缓冲区和解压相关资源
   * @param conf Hadoop配置对象
   * @param mapId 所属Map任务尝试ID
   * @param merger Merge管理器
   * @param size 缓冲区大小
   * @param codec 压缩编解码器，若Map输出未压缩则为null
   * @param primaryMapOutput 是否为主要Map输出
   */
  public InMemoryMapOutput(Configuration conf, TaskAttemptID mapId,
                           MergeManagerImpl<K, V> merger,
                           int size, CompressionCodec codec,
                           boolean primaryMapOutput) {
    super(conf, merger, mapId, (long)size, primaryMapOutput);
    this.codec = codec;
    byteStream = new BoundedByteArrayOutputStream(size);
    memory = byteStream.getBuffer();
    if (codec != null) {
      decompressor = CodecPool.getDecompressor(codec);
    } else {
      decompressor = null;
    }
  }

  /**
   * 获取存储数据的底层内存缓冲区
   * @return 内存字节数组缓冲区
   */
  public byte[] getMemory() {
    return memory;
  }

  /**
   * 获取绑定到缓冲区的字节输出流
   * @return 限长字节输出流
   */
  public BoundedByteArrayOutputStream getArrayStream() {
    return byteStream;
  }

  /**
   * 执行shuffle数据读取，从Map端拉取输出数据并写入内存缓冲区
   * 若数据压缩则先解压再写入内存
   * @param host Map输出所在主机信息
   * @param iFin IFile格式输入流
   * @param compressedLength 压缩后数据长度
   * @param decompressedLength 解压后数据长度
   * @param metrics shuffle客户端指标收集器
   * @param reporter 任务进度上报器
   * @throws IOException 读取或解压异常
   */
  @Override
  protected void doShuffle(MapHost host, IFileInputStream iFin,
                      long compressedLength, long decompressedLength,
                      ShuffleClientMetrics metrics,
                      Reporter reporter) throws IOException {
    InputStream input = iFin;

    // 若存在压缩编解码器，创建解压输入流
    if (codec != null) {
      decompressor.reset();
      input = codec.createInputStream(input, decompressor);
    }
  
    try {
      // 将输入流数据完整读入内存缓冲区
      IOUtils.readFully(input, memory, 0, memory.length);
      // 更新输入字节数指标
      metrics.inputBytes(memory.length);
      // 上报任务进度，避免超时
      reporter.progress();
      LOG.info("Read " + memory.length + " bytes from map-output for " +
                getMapId());

      /**
       * 验证输入流已读取完毕，确保解压后数据长度符合预期，
       * 同时强制解压器读取所有 trailing bytes，保持流同步
       */
      if (input.read() >= 0 ) {
        throw new IOException("Unexpected extra bytes from input stream for " +
                               getMapId());
      }
    } finally {
      // 归还解压处理器到编解码器池
      CodecPool.returnDecompressor(decompressor);
    }
  }

  /**
   * 提交当前内存输出，通知合并管理器关闭该内存文件，触发合并流程
   * @throws IOException 提交异常
   */
  @Override
  public void commit() throws IOException {
    getMerger().closeInMemoryFile(this);
  }
  
  /**
   * 中止当前内存输出，释放缓冲区占用的内存配额
   */
  @Override
  public void abort() {
    getMerger().unreserve(memory.length);
  }

  /**
   * 获取输出存储类型描述
   * @return 存储类型描述字符串
   */
  @Override
  public String getDescription() {
    return "MEMORY";
  }
}