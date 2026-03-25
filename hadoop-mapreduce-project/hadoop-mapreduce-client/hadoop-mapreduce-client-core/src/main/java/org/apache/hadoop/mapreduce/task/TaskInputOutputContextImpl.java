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
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 任务输入输出上下文实现类，为MapReduce任务提供输入读取和输出写入能力
 * 仅提供给{@link Mapper}和{@link Reducer}使用，封装任务的输入输出操作
 * @param <KEYIN> 任务输入键类型
 * @param <VALUEIN> 任务输入值类型
 * @param <KEYOUT> 任务输出键类型
 * @param <VALUEOUT> 任务输出值类型
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class TaskInputOutputContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
       extends TaskAttemptContextImpl 
       implements TaskInputOutputContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  private RecordWriter<KEYOUT,VALUEOUT> output;
  private OutputCommitter committer;

  /**
   * 构造任务输入输出上下文实例
   * @param conf 任务配置对象
   * @param taskid 任务尝试ID
   * @param output 输出记录写入器
   * @param committer 输出提交器
   * @param reporter 状态上报器
   */
  public TaskInputOutputContextImpl(Configuration conf, TaskAttemptID taskid,
                                    RecordWriter<KEYOUT,VALUEOUT> output,
                                    OutputCommitter committer,
                                    StatusReporter reporter) {
    super(conf, taskid, reporter);
    this.output = output;
    this.committer = committer;
  }

  /**
   * 前进到下一个键值对，到达输入末尾时返回false
   * @return 是否还有下一个键值对，存在返回true，无更多数据返回false
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public abstract 
  boolean nextKeyValue() throws IOException, InterruptedException;
 
  /**
   * 获取当前输入键
   * @return 当前输入键对象，无数据时返回null
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public abstract 
  KEYIN getCurrentKey() throws IOException, InterruptedException;

  /**
   * 获取当前输入值
   * @return 当前输入值对象
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public abstract VALUEIN getCurrentValue() throws IOException, 
                                                   InterruptedException;

  /**
   * 写入输出键值对
   */
  public void write(KEYOUT key, VALUEOUT value
                    ) throws IOException, InterruptedException {
    output.write(key, value);
  }

  /**
   * 获取当前任务的输出提交器
   * @return 输出提交器实例
   */
  public OutputCommitter getOutputCommitter() {
    return committer;
  }
}