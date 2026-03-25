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

package org.apache.hadoop.mapreduce.task;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * Map任务执行上下文实现类，为Mapper提供任务运行时的上下文环境和数据访问能力
 * @param <KEYIN> Mapper输入的Key类型
 * @param <VALUEIN> Mapper输入的Value类型
 * @param <KEYOUT> Mapper输出的Key类型
 * @param <VALUEOUT> Mapper输出的Value类型
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MapContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
    extends TaskInputOutputContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
    implements MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  // 当前Map任务的记录读取器
  private RecordReader<KEYIN,VALUEIN> reader;
  // 当前Map任务处理的输入分片
  private InputSplit split;

  /**
   * 构造Map任务上下文对象，初始化所有运行时依赖组件
   * @param conf 任务配置对象
   * @param taskid Map任务尝试ID
   * @param reader 输入记录读取器
   * @param writer 输出记录写入器
   * @param committer 输出提交器
   * @param reporter 状态上报器
   * @param split 当前Map需要处理的输入分片
   */
  public MapContextImpl(Configuration conf, TaskAttemptID taskid,
                        RecordReader<KEYIN,VALUEIN> reader,
                        RecordWriter<KEYOUT,VALUEOUT> writer,
                        OutputCommitter committer,
                        StatusReporter reporter,
                        InputSplit split) {
    super(conf, taskid, writer, committer, reporter);
    this.reader = reader;
    this.split = split;
  }

  /**
   * 获取当前Map任务处理的输入分片
   * @return 当前Map的输入分片对象
   */
  public InputSplit getInputSplit() {
    return split;
  }

  @Override
  public KEYIN getCurrentKey() throws IOException, InterruptedException {
    // 委托记录读取器获取当前输入记录的Key
    return reader.getCurrentKey();
  }

  @Override
  public VALUEIN getCurrentValue() throws IOException, InterruptedException {
    // 委托记录读取器获取当前输入记录的Value
    return reader.getCurrentValue();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // 委托记录读取器读取下一条输入键值对
    return reader.nextKeyValue();
  }

}