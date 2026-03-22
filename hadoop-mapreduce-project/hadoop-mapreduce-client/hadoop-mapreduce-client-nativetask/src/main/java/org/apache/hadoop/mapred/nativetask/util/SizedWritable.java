// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this this file except in compliance
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
package org.apache.hadoop.mapred.nativetask.util;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 原生任务中带长度信息的Writable包装类
 * 用于在原生任务处理过程中存储Writable对象及其序列化后的长度
 * 为原生任务序列化反序列化提供长度信息支持
 */
@InterfaceAudience.Private
public class SizedWritable<T> {
  public static final int INVALID_LENGTH = -1;

  public int length = INVALID_LENGTH;
  public Writable v;

  /**
   * 构造方法，根据给定类型反射创建Writable实例
   * @param klass Writable对象的Class类型，若为null则不创建实例
   */
  public SizedWritable(Class<?> klass) {
    if (null != klass) {
      v = (Writable) ReflectionUtils.newInstance(klass, null);
    }
    length = INVALID_LENGTH;
  }

  /**
   * 从输入缓冲区读取Writable对象数据
   * @param key 输入数据缓冲区
   * @throws IOException 输入缓冲区为null或读取失败时抛出异常
   */
  public void readFields(DataInputBuffer key) throws IOException {
    if (null != key) {
      this.v.readFields(key);
      this.length = INVALID_LENGTH;
    } else {
      throw new IOException("input key is null");
    }

  }

  /**
   * 重置当前对象，替换内部存储的Writable实例并重置长度
   * @param w 新的Writable对象实例
   */
  public void reset(T w) {
    this.v = (Writable) w;
    this.length = INVALID_LENGTH;
  }
}