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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.IFileOutputStream;
import org.apache.hadoop.mapred.IFile.Writer;

/**
 * MapReduce Shuffle阶段内存合并写入器，将Map输出键值对写入内存缓冲区
 * 实现IFile.Writer接口，专门用于reduce端在内存中合并map输出数据
 * @param <K> 键类型
 * @param <V> 值类型
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InMemoryWriter<K, V> extends Writer<K, V> {
  private DataOutputStream out;
  
  /**
   * 构造基于内存字节流的写入器
   * @param arrayStream 限定大小的内存字节输出流
   */
  public InMemoryWriter(BoundedByteArrayOutputStream arrayStream) {
    super(null);
    this.out = 
      new DataOutputStream(new IFileOutputStream(arrayStream));
  }
  
  /**
   * 追加普通对象键值对，不支持此方法
   * @param key 键
   * @param value 值
   * @throws IOException 永远抛出不支持操作异常
   */
  public void append(K key, V value) throws IOException {
    throw new UnsupportedOperationException
    ("InMemoryWriter.append(K key, V value");
  }
  
  /**
   * 追加已经反序列化到缓冲区的键值对，写入IFile格式到内存
   * @param key 存储键数据的输入缓冲区
   * @param value 存储值数据的输入缓冲区
   * @throws IOException 写入出错或长度非法时抛出
   */
  public void append(DataInputBuffer key, DataInputBuffer value)
  throws IOException {
    // 计算剩余未读取的键长度
    int keyLength = key.getLength() - key.getPosition();
    if (keyLength < 0) {
      throw new IOException("Negative key-length not allowed: " + keyLength + 
                            " for " + key);
    }
    
    // 计算剩余未读取的值长度
    int valueLength = value.getLength() - value.getPosition();
    if (valueLength < 0) {
      throw new IOException("Negative value-length not allowed: " + 
                            valueLength + " for " + value);
    }

    // 写入变长编码的长度
    WritableUtils.writeVInt(out, keyLength);
    WritableUtils.writeVInt(out, valueLength);
    // 写入键字节数据
    out.write(key.getData(), key.getPosition(), keyLength); 
    // 写入值字节数据
    out.write(value.getData(), value.getPosition(), valueLength); 
  }

  /**
   * 关闭写入器，写入IFile结束标记并关闭流
   * @throws IOException 关闭流出错时抛出
   */
  public void close() throws IOException {
    // 写入IFile结束标记（两个EOF长度标记）
    WritableUtils.writeVInt(out, IFile.EOF_MARKER);
    WritableUtils.writeVInt(out, IFile.EOF_MARKER);
    
    // 关闭输出流
    out.close();
    out = null;
  }

}