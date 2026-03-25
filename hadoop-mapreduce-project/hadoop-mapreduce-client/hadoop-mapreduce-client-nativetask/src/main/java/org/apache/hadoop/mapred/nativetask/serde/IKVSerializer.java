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
import org.apache.hadoop.mapred.nativetask.buffer.DataInputStream;
import org.apache.hadoop.mapred.nativetask.buffer.DataOutputStream;
import org.apache.hadoop.mapred.nativetask.util.SizedWritable;

/**
 * 原生任务键值对序列化接口，定义键值对数据的序列化和反序列化规范
 * 用于Hadoop MapReduce原生任务中，在Java层和Native层之间传递键值对数据
 */
@InterfaceAudience.Private
public interface IKVSerializer {

  /**
   * 更新SizedWritable对象的长度字段，根据实际数据计算序列化后的大小
   * @param key 带长度信息的键对象
   * @param value 带长度信息的值对象
   * @throws IOException 长度计算或IO异常
   */
  public void updateLength(SizedWritable<?> key, SizedWritable<?> value) throws IOException;

  /**
   * 将键值对序列化输出到数据输出流
   * @param out 数据输出流
   * @param key 待序列化的键
   * @param value 待序列化的值
   * @return 序列化后的总字节数
   * @throws IOException 序列化IO异常
   */
  public int serializeKV(DataOutputStream out, SizedWritable<?> key,
      SizedWritable<?> value) throws IOException;

  /**
   * 将带分区编号的键值对序列化输出到数据输出流
   * @param out 数据输出流
   * @param partitionId 分区编号，用于Shuffle阶段分区排序
   * @param key 待序列化的键
   * @param value 待序列化的值
   * @return 序列化后的总字节数
   * @throws IOException 序列化IO异常
   */
  public int serializePartitionKV(DataOutputStream out, int partitionId,
      SizedWritable<?> key, SizedWritable<?> value)
      throws IOException;

  /**
   * 从数据输入流反序列化键值对
   * @param in 数据输入流
   * @param key 存储反序列化结果的键对象
   * @param value 存储反序列化结果的值对象
   * @return 反序列化读取的总字节数
   * @throws IOException 反序列化IO异常
   */
  public int deserializeKV(DataInputStream in, SizedWritable<?> key, SizedWritable<?> value)
    throws IOException;
}