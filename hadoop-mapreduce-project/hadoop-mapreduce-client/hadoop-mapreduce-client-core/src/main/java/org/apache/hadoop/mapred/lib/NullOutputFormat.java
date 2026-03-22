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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * 空输出格式实现，会丢弃所有作业输出，不写入任何持久化存储
 * 用于不需要输出结果的MapReduce作业场景，例如仅做数据加载计算
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class NullOutputFormat<K, V> implements OutputFormat<K, V> {
  
  /**
   * 获取空输出的记录写入器，该写入器会丢弃所有写入数据
   * @param ignored 文件系统对象，本实现不使用
   * @param job 作业配置对象
   * @param name 输出文件名，本实现不使用
   * @param progress 进度回调对象，本实现不使用
   * @return 不执行任何实际写入操作的空记录写入器
   */
  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, 
                                      String name, Progressable progress) {
    return new RecordWriter<K, V>(){
        public void write(K key, V value) { }
        public void close(Reporter reporter) { }
      };
  }
  
  /**
   * 检查输出规格，本实现不做任何检查
   * @param ignored 文件系统对象，本实现不使用
   * @param job 作业配置对象，本实现不使用
   */
  public void checkOutputSpecs(FileSystem ignored, JobConf job) { }
}