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
import org.apache.hadoop.mapred.nativetask.INativeComparable;

/**
 * ByteWritable 自定义序列化器，用于原生任务中字节类型可写对象的序列化，
 * 实现了原生可比接口，支持在Native层进行排序比较。
 */
@InterfaceAudience.Private
public class ByteWritableSerializer extends DefaultSerializer implements
    INativeComparable {

  /**
   * 获取 ByteWritable 对象序列化后的固定长度
   * @param w 待计算长度的可写对象
   * @return 固定长度，单字节永远返回1
   * @throws IOException IO异常
   */
  @Override
  public int getLength(Writable w) throws IOException {
    return 1;
  }
}