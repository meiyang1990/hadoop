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
 * NullWritable 类型的原生任务序列化器，适配原生任务对空值类型的序列化需求
 * 空值不占用任何序列化存储空间，实现 INativeComparable 接口支持原生排序
 */
@InterfaceAudience.Private
public class NullWritableSerializer extends DefaultSerializer implements
    INativeComparable {

  /**
   * 获取 NullWritable 对象序列化后的字节长度
   * @param w 待计算长度的 NullWritable 对象
   * @return 固定返回0，因为空值不占用存储空间
   * @throws IOException IO异常
   */
  @Override
  public int getLength(Writable w) throws IOException {
    return 0;
  }
}