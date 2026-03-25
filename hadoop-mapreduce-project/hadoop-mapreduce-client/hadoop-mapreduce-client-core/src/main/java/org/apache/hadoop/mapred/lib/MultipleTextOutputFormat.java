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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * 文件级注释：多文本输出格式实现，继承自MultipleOutputFormat，支持将MapReduce计算结果
 * 根据自定义规则输出到多个不同的文本格式输出文件，满足按关键字分文件输出的业务需求
 *
 * 该类扩展了MultipleOutputFormat，允许以文本格式将输出数据写入不同的输出文件。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MultipleTextOutputFormat<K, V>
    extends MultipleOutputFormat<K, V> {

  /** 单例实例化的文本输出格式对象，复用避免重复创建 */
  private TextOutputFormat<K, V> theTextOutputFormat = null;

  /**
   * 获取基础文本记录写入器，用于向指定输出文件写入文本格式的键值对
   * @param fs 文件系统对象
   * @param job 作业配置对象
   * @param name 输出文件名
   * @param arg3 进度上报回调对象
   * @return 文本格式的记录写入器
   * @throws IOException 文件操作异常
   */
  @Override
  protected RecordWriter<K, V> getBaseRecordWriter(FileSystem fs, JobConf job,
      String name, Progressable arg3) throws IOException {
    if (theTextOutputFormat == null) {
      theTextOutputFormat = new TextOutputFormat<K, V>();
    }
    return theTextOutputFormat.getRecordWriter(fs, job, name, arg3);
  }
}