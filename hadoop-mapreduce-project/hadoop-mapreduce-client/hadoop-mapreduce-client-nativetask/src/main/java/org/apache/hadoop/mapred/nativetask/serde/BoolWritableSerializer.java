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
 * BoolWritable 本地任务序列化实现类
 * 为原生任务处理提供布尔类型可写对象的序列化能力，实现原生可比较接口
 */
@InterfaceAudience.Private
public class BoolWritableSerializer extends DefaultSerializer implements
    INativeComparable {

  /**
   * 获取 BoolWritable 对象序列化后的固定字节长度
   * @param w 待计算长度的可写对象
   * @return 序列化后占用的字节数，固定为1字节
   * @throws IOException IO异常
   */
  @Override
  public int getLength(Writable w) throws IOException {
    return 1;
  }
}