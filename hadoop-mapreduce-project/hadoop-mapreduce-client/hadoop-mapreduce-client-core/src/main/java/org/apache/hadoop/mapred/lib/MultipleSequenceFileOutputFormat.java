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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * 文件级注释：支持将MapReduce任务结果输出到多个SequenceFile格式文件的输出格式类
 * 
 * 本类继承MultipleOutputFormat，实现将不同分区的数据输出到独立的SequenceFile格式输出文件，
 * 满足任务需要按规则输出多个文件的业务场景。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MultipleSequenceFileOutputFormat <K,V>
extends MultipleOutputFormat<K, V> {

    private SequenceFileOutputFormat<K,V> theSequenceFileOutputFormat = null;
  
  /**
   * 获取基础的SequenceFile格式记录写入器，用于输出指定名称的结果文件
   * @param fs 文件系统对象
   * @param job 作业配置对象
   * @param name 输出文件名称
   * @param arg3 进度报告对象
   * @return 对应SequenceFile格式的RecordWriter
   * @throws IOException 文件操作异常
   */
  @Override
  protected RecordWriter<K, V> getBaseRecordWriter(FileSystem fs,
                                                   JobConf job,
                                                   String name,
                                                   Progressable arg3) 
  throws IOException {
    // 懒初始化SequenceFile输出格式实例
    if (theSequenceFileOutputFormat == null) {
      theSequenceFileOutputFormat = new SequenceFileOutputFormat<K,V>();
    }
    // 委托原生SequenceFileOutputFormat创建记录写入器
    return theSequenceFileOutputFormat.getRecordWriter(fs, job, name, arg3);
  }
}