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
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Java与Native任务之间的数据序列化反序列化接口，定义了数据跨语言传输的序列化契约。
 * 为MapReduce本地任务框架提供统一的序列化规范，默认实现可参考{@link DefaultSerializer}。
 * 
 * 注意：如果自定义实现该接口替换默认序列化，必须保证Native侧能够正确解析对应格式。
 * 
 * @param <T> 需要序列化的Java对象类型
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface INativeSerializer<T> {

  /**
   * 获取待序列化对象序列化后的字节长度。
   * 对于已知固定长度的类型（如IntWritable），实现自定义序列化可以提前返回长度，
   * 避免额外计算，提升序列化性能。
   * 
   * @param w 待序列化的Java对象
   * @return 序列化后的字节长度
   * @throws IOException IO异常时抛出
   */
  public int getLength(T w) throws IOException;

  /**
   * 将Java对象序列化输出到DataOutput流
   * 
   * @param w 待序列化的Java对象
   * @param out 输出流
   * @throws IOException IO异常时抛出
   */
  public void serialize(T w, DataOutput out) throws IOException;

  /**
   * 从DataInput流反序列化字节到Java对象
   * 
   * @param in 输入流
   * @param length 需要读取的字节长度
   * @param w 反序列化结果填充的目标对象
   * @throws IOException IO异常时抛出
   */
  public void deserialize(DataInput in, int length, T w) throws IOException;
}