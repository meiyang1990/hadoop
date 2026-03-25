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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SkipBadRecords;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Pipes框架中与C++ Reduce任务通信的Java桥接实现
 * 负责将Java层收到的Map输出数据转发给C++ Reduce进程，并将C++输出结果传回Java框架
 * 
 * @param <K2> Reduce输入key类型
 * @param <V2> Reduce输入value类型
 * @param <K3> Reduce输出key类型
 * @param <V3> Reduce输出value类型
 */
class PipesReducer<K2 extends WritableComparable, V2 extends Writable,
    K3 extends WritableComparable, V3 extends Writable>
    implements Reducer<K2, V2, K3, V3> {
  private static final Logger LOG =
      LoggerFactory.getLogger(PipesReducer.class.getName());
  private JobConf job;
  private Application<K2, V2, K3, V3> application = null;
  private DownwardProtocol<K2, V2> downlink = null;
  private boolean isOk = true;
  private boolean skipping = false;

  /**
   * 配置Reduce任务，初始化相关参数
   * @param job 作业配置对象
   */
  public void configure(JobConf job) {
    this.job = job;
    // 关闭Pipes场景下Reducer处理计数器的自动递增，因为C++端处理记录数可能和Java端输入不一致
    SkipBadRecords.setAutoIncrReducerProcCount(job, false);
    skipping = job.getBoolean(MRJobConfig.SKIP_RECORDS, false);
  }

  /**
   * 处理单个key对应的所有value，将数据转发给C++ Reduce任务
   * 若C++应用尚未启动，则先启动应用
   * @param key 输入key
   * @param values 该key对应的value迭代器
   * @param output 输出收集器
   * @param reporter 任务报告器
   * @throws IOException 转发过程中IO异常
   */
  public void reduce(K2 key, Iterator<V2> values, 
                     OutputCollector<K3, V3> output, Reporter reporter
                     ) throws IOException {
    isOk = false;
    startApplication(output, reporter);
    downlink.reduceKey(key);
    while (values.hasNext()) {
      downlink.reduceValue(values.next());
    }
    if(skipping) {
      // 跳过坏记录模式下每次输入后都刷新流，避免缓冲影响坏记录周边的正常记录
      downlink.flush();
    }
    isOk = true;
  }

  /**
   * 延迟启动C++ Reduce应用，仅在第一次处理数据时初始化
   * @param output 输出收集器
   * @param reporter 任务报告器
   * @throws IOException 启动过程IO异常
   */
  @SuppressWarnings("unchecked")
  private void startApplication(OutputCollector<K3, V3> output, Reporter reporter) throws IOException {
    if (application == null) {
      try {
        LOG.info("starting application");
        application = 
          new Application<K2, V2, K3, V3>(
              job, null, output, reporter, 
              (Class<? extends K3>) job.getOutputKeyClass(), 
              (Class<? extends V3>) job.getOutputValueClass());
        downlink = application.getDownlink();
      } catch (InterruptedException ie) {
        throw new RuntimeException("interrupted", ie);
      }
      int reduce=0;
      downlink.runReduce(reduce, Submitter.getIsJavaRecordWriter(job));
    }
  }

  /**
   * Reduce任务关闭方法，负责清理C++应用资源，处理任务结束逻辑
   * @throws IOException 关闭过程IO异常
   */
  public void close() throws IOException {
    // 如果还没启动应用，先启动空应用完成初始化
    if (isOk) {
      OutputCollector<K3, V3> nullCollector = new OutputCollector<K3, V3>() {
        public void collect(K3 key, 
                            V3 value) throws IOException {
          // 空实现，不收集任何输出
        }
      };
      startApplication(nullCollector, Reporter.NULL);
    }
    try {
      if (isOk) {
        // 正常结束，通知C++端输入已完成
        application.getDownlink().endOfInput();
      } else {
        // 异常结束，通知C++端终止任务，清理资源
        application.getDownlink().abort();
      }
      LOG.info("waiting for finish");
      application.waitForFinish();
      LOG.info("got done");
    } catch (Throwable t) {
      application.abort(t);
    } finally {
      application.cleanup();
    }
  }
}