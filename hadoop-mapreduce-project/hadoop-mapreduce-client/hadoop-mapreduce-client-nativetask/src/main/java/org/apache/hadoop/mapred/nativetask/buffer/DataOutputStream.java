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
package org.apache.hadoop.mapred.nativetask.buffer;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 原生任务输出数据流抽象基类
 * 为NativeTask模块的缓冲区输出提供统一抽象接口，继承自标准OutputStream和DataOutput，
 * 增加了缓冲区空间检查和未刷新数据检查能力，供不同具体输出实现扩展。
 */
@InterfaceAudience.Private
public abstract class DataOutputStream extends OutputStream implements DataOutput {
  /**
   * 检查当前缓冲区是否有足够空间容纳指定长度的数据
   * @param length 需要存储的数据字节长度
   * @return true 空间不足，false 空间足够
   * @throws IOException IO异常
   */
  public abstract boolean shortOfSpace(int length) throws IOException;

  /**
   * 检查流中是否存在尚未刷新到下游的数据
   * @return true 存在未刷新数据，false 不存在未刷新数据
   */
  public abstract boolean hasUnFlushedData();
}