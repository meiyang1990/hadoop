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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/**
 * 原生任务的默认Writable类型序列化器，实现Writable对象的序列化/反序列化和长度计算
 * 为原生任务执行框架提供Java对象到二进制数据的转换能力
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DefaultSerializer implements INativeSerializer<Writable> {

  /**
   * 扩展ByteArrayOutputStream，暴露内部缓冲区数组避免数组拷贝
   */
  static class ModifiedByteArrayOutputStream extends ByteArrayOutputStream {

    /**
     * 获取内部原始缓冲区数组，无需拷贝
     * @return 内部存储字节的缓冲区数组
     */
    public byte[] getBuffer() {
      return this.buf;
    }
  }

  private final ModifiedByteArrayOutputStream outBuffer = new ModifiedByteArrayOutputStream();
  private final DataOutputStream outData = new DataOutputStream(outBuffer);
  private Writable buffered = null;
  private int bufferedLength = -1;

  /**
   * 计算Writable对象序列化后的字节长度
   * @param w 需要计算长度的Writable对象
   * @return 序列化后字节长度
   * @throws IOException 序列化过程IO异常
   */
  @Override
  public int getLength(Writable w) throws IOException {
    // 缓存优化代码已注释，当前禁用缓存复用
    // if (w == buffered) {
    // return bufferedLength;
    // }
    buffered = null;
    bufferedLength = -1;

    // 重置缓冲区准备序列化
    outBuffer.reset();
    // 将对象序列化到缓冲区
    w.write(outData);
    // 获取序列化后的字节长度
    bufferedLength = outBuffer.size();
    // 缓存当前计算的对象和长度
    buffered = w;
    return bufferedLength;
  }

  /**
   * 将Writable对象序列化输出到指定DataOutput
   * @param w 需要序列化的Writable对象
   * @param out 输出目标
   * @throws IOException 序列化过程IO异常
   */
  @Override
  public void serialize(Writable w, DataOutput out) throws IOException {
    w.write(out);
  }

  /**
   * 从DataInput反序列化读取数据到Writable对象
   * @param in 输入数据源
   * @param length 待读取数据长度
   * @param w 目标Writable对象
   * @throws IOException 反序列化过程IO异常
   */
  @Override
  public void deserialize(DataInput in, int length, Writable w) throws IOException {
    w.readFields(in);
  }
}