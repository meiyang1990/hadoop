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

package org.apache.hadoop.mapred.pipes;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunner;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SkipBadRecords;
import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 * Hadoop Pipes框架C++ Mapper的运行适配器，负责将Java MapTask执行流程适配到C++ mapper进程。
 * 作为MapRunner的实现，对接Hadoop Old MapReduce API，管理与C++进程的交互生命周期。
 * @param <K1> Map输入键类型
 * @param <V1> Map输入值类型
 * @param <K2> Map输出键类型
 * @param <V2> Map输出值类型
 */
class PipesMapRunner<K1 extends WritableComparable, V1 extends Writable,
    K2 extends WritableComparable, V2 extends Writable>
    extends MapRunner<K1, V1, K2, V2> {
  private JobConf job;

  /**
   * 初始化配置，保存作业配置并关闭Map处理记录计数器的自动递增。
   * Pipes场景下处理的记录数可能和输入记录数不一致，由C++端自行统计。
   * @param job 作业配置对象
   */
  public void configure(JobConf job) {
    this.job = job;
    //disable the auto increment of the counter. For pipes, no of processed 
    //records could be different(equal or less) than the no of records input.
    SkipBadRecords.setAutoIncrMapperProcCount(job, false);
  }

  /**
   * 执行Map任务，启动与C++ mapper进程的交互，按模式处理输入并等待任务完成。
   * @param input 输入记录读取器
   * @param output Map输出收集器
   * @param reporter 任务状态上报器
   */
  @SuppressWarnings("unchecked")
  public void run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output,
                  Reporter reporter) throws IOException {
    Application<K1, V1, K2, V2> application = null;
    try {
      // 当Java RecordReader和Java Mapper都未启用时，构造伪输入用于非Java输入场景
      RecordReader<FloatWritable, NullWritable> fakeInput = 
        (!Submitter.getIsJavaRecordReader(job) && 
         !Submitter.getIsJavaMapper(job)) ? 
	  (RecordReader<FloatWritable, NullWritable>) input : null;
      // 初始化Pipes应用，建立与C++进程的连接
      application = new Application<K1, V1, K2, V2>(job, fakeInput, output, 
                                                    reporter,
          (Class<? extends K2>) job.getOutputKeyClass(), 
          (Class<? extends V2>) job.getOutputValueClass());
    } catch (InterruptedException ie) {
      throw new RuntimeException("interrupted", ie);
    }
    // 获取到C++进程的下行协议通道
    DownwardProtocol<K1, V1> downlink = application.getDownlink();
    // 判断是否使用Java端的RecordReader读取输入
    boolean isJavaInput = Submitter.getIsJavaRecordReader(job);
    // 向C++进程发送Map启动命令，传入分片信息、Reduce任务数和输入模式标识
    downlink.runMap(reporter.getInputSplit(), 
                    job.getNumReduceTasks(), isJavaInput);
    // 获取是否启用跳过坏记录模式
    boolean skipping = job.getBoolean(MRJobConfig.SKIP_RECORDS, false);
    try {
      if (isJavaInput) {
        // 分配可复用的键值对象
        K1 key = input.createKey();
        V1 value = input.createValue();
        // 向C++端发送输入键值的类型信息
        downlink.setInputTypes(key.getClass().getName(),
                               value.getClass().getName());
        
        // 循环读取每条输入记录
        while (input.next(key, value)) {
          // 将当前键值对发送给C++ mapper处理
          downlink.mapItem(key, value);
          if(skipping) {
            // 跳过模式下每次输入后都刷新流，避免坏记录附近的记录被缓存无法处理
            downlink.flush();
          }
        }
        // 通知C++端输入已全部读完
        downlink.endOfInput();
      }
      // 等待C++ mapper任务执行完成
      application.waitForFinish();
    } catch (Throwable t) {
      // 任务执行出错，中止应用并清理资源
      application.abort(t);
    } finally {
      // 最后清理应用资源，关闭连接
      application.cleanup();
    }
  }
  
}