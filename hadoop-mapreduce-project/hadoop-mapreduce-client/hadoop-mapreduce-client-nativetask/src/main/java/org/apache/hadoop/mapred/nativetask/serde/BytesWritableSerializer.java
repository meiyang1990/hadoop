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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.nativetask.INativeComparable;

/**
 * BytesWritable 类型的原生任务序列化器，实现原生任务字节序列化和比较接口
 * 用于在原生 MapReduce 任务中处理 BytesWritable 类型数据的序列化与反序列化
 */
@InterfaceAudience.Private
public class BytesWritableSerializer
  implements INativeComparable, INativeSerializer<BytesWritable> {

  /**
   * 获取 BytesWritable 对象的序列化后字节长度
   * @param w 待获取长度的 BytesWritable 对象
   * @return 序列化后字节长度
   * @throws IOException IO异常
   */
  @Override
  public int getLength(BytesWritable w) throws IOException {
    return w.getLength();
  }

  /**
   * 将 BytesWritable 对象序列化输出到 DataOutput
   * @param w 待序列化的 BytesWritable 对象
   * @param out 输出流
   * @throws IOException IO异常
   */
  @Override
  public void serialize(BytesWritable w, DataOutput out) throws IOException {
    out.write(w.getBytes(), 0, w.getLength());
  }

  /**
   * 从 DataInput 反序列化出 BytesWritable 对象
   * @param in 输入流
   * @param length 待读取数据长度
   * @param w 目标 BytesWritable 对象
   * @throws IOException IO异常
   */
  @Override
  public void deserialize(DataInput in, int length, BytesWritable w) throws IOException {
    w.setSize(length);
    in.readFully(w.getBytes(), 0, length);
  }
}