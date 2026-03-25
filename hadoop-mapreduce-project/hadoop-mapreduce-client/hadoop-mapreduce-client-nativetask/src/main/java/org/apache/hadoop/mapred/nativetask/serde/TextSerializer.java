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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.nativetask.INativeComparable;

/**
 * Text类型序列化器，为原生任务提供Text类型的序列化、反序列化能力，同时支持原生比较
 * 用于Hadoop本地任务框架中处理Text类型键值对的序列化
 */
@InterfaceAudience.Private
public class TextSerializer implements INativeSerializer<Text>, INativeComparable {

  /**
   * 构造Text序列化器实例
   * @throws SecurityException 安全检查异常
   * @throws NoSuchMethodException 找不到对应方法异常
   */
  public TextSerializer() throws SecurityException, NoSuchMethodException {
  }

  /**
   * 获取Text对象的序列化后字节长度
   * @param w 待计算长度的Text对象
   * @return 序列化后的字节长度
   * @throws IOException IO异常
   */
  @Override
  public int getLength(Text w) throws IOException {
    return w.getLength();
  }

  /**
   * 将Text对象序列化输出到DataOutput流
   * @param w 待序列化的Text对象
   * @param out 输出流
   * @throws IOException IO异常
   */
  @Override
  public void serialize(Text w, DataOutput out) throws IOException {
     out.write(w.getBytes(), 0, w.getLength());
  }

  /**
   * 从DataInput流反序列化指定长度的数据到Text对象
   * @param in 输入流
   * @param length 待读取数据长度
   * @param w 目标Text对象
   * @throws IOException IO异常
   */
  @Override
  public void deserialize(DataInput in, int length, Text w) throws IOException {
    w.readWithKnownLength(in, length);
  }
}