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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 空输出格式，丢弃所有MapReduce任务输出，相当于将输出写入/dev/null
 * 适用于仅需要计算过程、不需要保存输出结果的场景
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class NullOutputFormat<K, V> extends OutputFormat<K, V> {
  
  /**
   * 获取记录写入器，返回一个不执行任何实际写入操作的空实现
   * @param context 任务尝试上下文
   * @return 空实现的记录写入器
   */
  @Override
  public RecordWriter<K, V> 
         getRecordWriter(TaskAttemptContext context) {
    return new RecordWriter<K, V>(){
        public void write(K key, V value) { }
        public void close(TaskAttemptContext context) { }
      };
  }
  
  /**
   * 检查输出规格，空实现不做任何检查
   * @param context 作业上下文
   */
  @Override
  public void checkOutputSpecs(JobContext context) { }
  
  /**
   * 获取输出提交器，返回空实现的输出提交器，不处理任何提交逻辑
   * @param context 任务尝试上下文
   * @return 空实现的输出提交器
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return new OutputCommitter() {
      public void abortTask(TaskAttemptContext taskContext) { }
      public void cleanupJob(JobContext jobContext) { }
      public void commitTask(TaskAttemptContext taskContext) { }
      public boolean needsTaskCommit(TaskAttemptContext taskContext) {
        return false;
      }
      public void setupJob(JobContext jobContext) { }
      public void setupTask(TaskAttemptContext taskContext) { }

      @Override
      @Deprecated
      public boolean isRecoverySupported() {
        return true;
      }

      @Override
      public void recoverTask(TaskAttemptContext taskContext)
          throws IOException {
        // Nothing to do for recovering the task.
      }
    };
  }
}