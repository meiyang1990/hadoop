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

package org.apache.hadoop.mapred.nativetask.handlers;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.nativetask.NativeDataTarget;
import org.apache.hadoop.mapred.nativetask.buffer.ByteBufferDataWriter;
import org.apache.hadoop.mapred.nativetask.serde.IKVSerializer;
import org.apache.hadoop.mapred.nativetask.serde.KVSerializer;
import org.apache.hadoop.mapred.nativetask.util.SizedWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件：BufferPusher.java
 * 所属模块：Hadoop MapReduce 本地任务处理
 * 核心职责：将Java Map阶段输出的键值对主动推送到本地任务缓冲区，供Native处理端消费
 */
/**
 * 主动将数据推送到缓冲区，并通知BufferPushee拉取处理，实现Java到Native任务的数据传递
 */
@InterfaceAudience.Private
public class BufferPusher<K, V> implements OutputCollector<K, V> {
  
  private static final Logger LOG = LoggerFactory.getLogger(BufferPusher.class);

  private final SizedWritable<K> tmpInputKey;
  private final SizedWritable<V> tmpInputValue;
  private ByteBufferDataWriter out;
  IKVSerializer serializer;
  private boolean closed = false;

  /**
   * 构造BufferPusher，初始化序列化器和数据写入器
   * @param iKClass 键类型Class对象
   * @param iVClass 值类型Class对象
   * @param target 本地数据目标，用于数据输出
   * @throws IOException 初始化失败时抛出IO异常
   */
  public BufferPusher(Class<K> iKClass, Class<V> iVClass,
                      NativeDataTarget target) throws IOException {
    tmpInputKey = new SizedWritable<K>(iKClass);
    tmpInputValue = new SizedWritable<V>(iVClass);

    if (null != iKClass && null != iVClass) {
      this.serializer = new KVSerializer<K, V>(iKClass, iVClass);
    }
    this.out = new ByteBufferDataWriter(target);
  }

  /**
   * 收集带分区信息的键值对，序列化后推送到缓冲区
   * @param key 输出键
   * @param value 输出值
   * @param partition 分区编号
   * @throws IOException 序列化或写入失败时抛出IO异常
   */
  public void collect(K key, V value, int partition) throws IOException {
    tmpInputKey.reset(key);
    tmpInputValue.reset(value);
    serializer.serializePartitionKV(out, partition, tmpInputKey, tmpInputValue);
  };

  @Override
  /**
   * 收集键值对，序列化后推送到缓冲区，实现OutputCollector接口
   * @param key 输出键
   * @param value 输出值
   * @throws IOException 序列化或写入失败时抛出IO异常
   */
  public void collect(K key, V value) throws IOException {
    // 已关闭则直接返回
    if (closed) {
      return;
    }
    tmpInputKey.reset(key);
    tmpInputValue.reset(value);
    serializer.serializeKV(out, tmpInputKey, tmpInputValue);
  };

  /**
   * 刷新缓冲区，将未写出的数据写入目标
   * @throws IOException 刷新失败时抛出IO异常
   */
  public void flush() throws IOException {
    if (null != out) {
      // 仅在存在未刷新数据时执行刷新
      if (out.hasUnFlushedData()) {
        out.flush();
      }
    }
  }
  
  /**
   * 关闭BufferPusher，释放资源并标记为已关闭
   * @throws IOException 关闭底层写入器失败时抛出IO异常
   */
  public void close() throws IOException {
    if (closed) {
      return;
    }
    if (null != out) {
      out.close();
    }
    closed = true;
  }
}