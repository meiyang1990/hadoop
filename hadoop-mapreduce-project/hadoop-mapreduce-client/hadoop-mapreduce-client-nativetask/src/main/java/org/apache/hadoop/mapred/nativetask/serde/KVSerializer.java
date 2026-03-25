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

package org.apache.hadoop.mapred.nativetask.serde;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.nativetask.Constants;
import org.apache.hadoop.mapred.nativetask.buffer.DataInputStream;
import org.apache.hadoop.mapred.nativetask.buffer.DataOutputStream;
import org.apache.hadoop.mapred.nativetask.util.SizedWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 原生任务键值对序列化器，实现MapReduce键值对数据的序列化与反序列化
 * 为原生任务处理框架提供Java层与Native层之间的键值对数据交换能力
 */
@InterfaceAudience.Private
public class KVSerializer<K, V> implements IKVSerializer {

  private static final Logger LOG = LoggerFactory.getLogger(KVSerializer.class);
  
  /** 键值对头部长度，存储键长度和值长度两个整数 */
  public static final int KV_HEAD_LENGTH = Constants.SIZEOF_KV_LENGTH;

  private final INativeSerializer<Writable> keySerializer;
  private final INativeSerializer<Writable> valueSerializer;

  /**
   * 构造方法，根据键和值的类型获取对应序列化器
   * @param kclass 键类型
   * @param vclass 值类型
   * @throws IOException 获取序列化器失败时抛出异常
   */
  public KVSerializer(Class<K> kclass, Class<V> vclass) throws IOException {
    
    this.keySerializer = NativeSerialization.getInstance().getSerializer(kclass);
    this.valueSerializer = NativeSerialization.getInstance().getSerializer(vclass);
  }

  /**
   * 更新键和值的序列化长度
   * @param key 键包装对象
   * @param value 值包装对象
   * @throws IOException 计算长度失败时抛出异常
   */
  @Override
  public void updateLength(SizedWritable<?> key, SizedWritable<?> value) throws IOException {
    key.length = keySerializer.getLength(key.v);
    value.length = valueSerializer.getLength(value.v);
    return;
  }

  /**
   * 序列化不带分区信息的键值对
   * @param out 输出流
   * @param key 键包装对象
   * @param value 值包装对象
   * @return 序列化后总字节数
   * @throws IOException 序列化失败时抛出异常
   */
  @Override
  public int serializeKV(DataOutputStream out, SizedWritable<?> key, SizedWritable<?> value)
    throws IOException {
    return serializePartitionKV(out, -1, key, value);
  }

  /**
   * 序列化带分区信息的键值对
   * @param out 输出流
   * @param partitionId 分区ID，-1表示无分区
   * @param key 键包装对象
   * @param value 值包装对象
   * @return 序列化后总字节数
   * @throws IOException 序列化失败时抛出异常
   */
  @Override
  public int serializePartitionKV(DataOutputStream out, int partitionId,
      SizedWritable<?> key, SizedWritable<?> value)
      throws IOException {

    // 如果长度未计算，先计算长度
    if (key.length == SizedWritable.INVALID_LENGTH ||
        value.length == SizedWritable.INVALID_LENGTH) {
      updateLength(key, value);
    }

    final int keyLength = key.length;
    final int valueLength = value.length;

    // 计算总字节数
    int bytesWritten = KV_HEAD_LENGTH + keyLength + valueLength;
    if (partitionId != -1) {
      bytesWritten += Constants.SIZEOF_PARTITION_LENGTH;
    }

    // 如果缓冲区空间不足，先刷新输出流
    if (out.hasUnFlushedData() && out.shortOfSpace(bytesWritten)) {
      out.flush();
    }

    // 写入分区ID（如果存在）
    if (partitionId != -1) {
      out.writeInt(partitionId);
    }
        
    // 写入键长度和值长度头信息
    out.writeInt(keyLength);
    out.writeInt(valueLength);
    
    // 序列化键和值
    keySerializer.serialize(key.v, out);
    valueSerializer.serialize(value.v, out);

    return bytesWritten;
  }

  /**
   * 反序列化键值对
   * @param in 输入流
   * @param key 键包装对象，存储反序列化结果
   * @param value 值包装对象，存储反序列化结果
   * @return 反序列化总字节数，无数据时返回0
   * @throws IOException 反序列化失败时抛出异常
   */
  @Override
  public int deserializeKV(DataInputStream in, SizedWritable<?> key,
      SizedWritable<?> value) throws IOException {

    // 无未读数据直接返回
    if (!in.hasUnReadData()) {
      return 0;
    }

    // 读取键和值长度
    key.length = in.readInt();
    value.length = in.readInt();

    // 反序列化键和值
    keySerializer.deserialize(in, key.length, key.v);
    valueSerializer.deserialize(in, value.length, value.v);

    return key.length + value.length + KV_HEAD_LENGTH;
  }

}