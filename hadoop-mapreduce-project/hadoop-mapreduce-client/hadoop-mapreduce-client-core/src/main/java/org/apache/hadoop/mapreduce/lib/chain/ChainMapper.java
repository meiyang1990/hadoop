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
package org.apache.hadoop.mapreduce.lib.chain;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.Chain.ChainBlockingQueue;

/**
 * @file 文件路径：hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/lib/chain/ChainMapper.java
 * @description 链式Mapper实现类，允许在单个Map任务中串联执行多个Mapper类，形成管道式处理流程，减少磁盘IO开销
 * 
 * <p>
 * 多个Mapper按添加顺序以管道方式执行，前一个Mapper的输出作为后一个Mapper的输入，最后一个Mapper的输出作为整个Map任务的输出写入。
 * </p>
 * <p>
 * 核心优势是串联的Mapper不需要感知自己处于链式执行中，可复用独立编写的专用Mapper，组合完成单个任务内的复合处理。
 * </p>
 * <p>
 * 使用注意：需要保证链式中前一个Mapper的输出键值类型与下一个Mapper的输入键值类型匹配，链式框架不会自动进行类型转换。
 * </p>
 * <p>
 * 配合ChainReducer可以实现<code>[MAP+ / REDUCE MAP*]</code>的组合作业模式，显著减少中间结果的磁盘IO开销。
 * </p>
 * <p>
 * 重要说明：不需要为ChainMapper整体设置输出键值类，由链式中最后一个Mapper的addMapper调用自动完成设置。
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ChainMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends
    Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  /**
   * 将指定Mapper类添加到ChainMapper的执行链中
   * 
   * <p>
   * 键值按值传递从链中前一个元素传递到下一个；传入的mapperConf配置优先级高于Job全局配置，任务运行时生效。
   * </p>
   * <p>
   * 不需要为ChainMapper整体指定输出键值类，会由链式中最后一个Mapper的addMapper调用自动完成设置。
   * </p>
   * 
   * @param job 当前作业对象
   * @param klass 要添加的Mapper类
   * @param inputKeyClass 当前Mapper的输入键类型
   * @param inputValueClass 当前Mapper的输入值类型
   * @param outputKeyClass 当前Mapper的输出键类型
   * @param outputValueClass 当前Mapper的输出值类型
   * @param mapperConf 当前Mapper的专属配置，推荐使用不加载默认配置的空配置对象构造
   * @throws IOException 添加过程中发生IO异常时抛出
   */
  public static void addMapper(Job job, Class<? extends Mapper> klass,
      Class<?> inputKeyClass, Class<?> inputValueClass,
      Class<?> outputKeyClass, Class<?> outputValueClass,
      Configuration mapperConf) throws IOException {
    job.setMapperClass(ChainMapper.class);
    job.setMapOutputKeyClass(outputKeyClass);
    job.setMapOutputValueClass(outputValueClass);
    Chain.addMapper(true, job, klass, inputKeyClass, inputValueClass,
        outputKeyClass, outputValueClass, mapperConf);
  }

  private Chain chain;

  @Override
  protected void setup(Context context) {
    // 初始化链式执行容器
    chain = new Chain(true);
    // 根据配置初始化所有串联的Mapper实例
    chain.setup(context.getConfiguration());
  }

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    // 执行初始化
    setup(context);

    // 获取链式中Mapper的总数量
    int numMappers = chain.getAllMappers().size();
    // 无Mapper时直接返回
    if (numMappers == 0) {
      return;
    }

    ChainBlockingQueue<Chain.KeyValuePair<?, ?>> inputqueue;
    ChainBlockingQueue<Chain.KeyValuePair<?, ?>> outputqueue;
    // 仅一个Mapper时直接运行，无需多线程管道
    if (numMappers == 1) {
      chain.runMapper(context, 0);
    } else {
      // 初始化第一个Mapper，输出到阻塞队列
      outputqueue = chain.createBlockingQueue();
      chain.addMapper(context, outputqueue, 0);
      // 循环添加中间Mapper，从前置队列取输入，输出到后置队列
      for (int i = 1; i < numMappers - 1; i++) {
        inputqueue = outputqueue;
        outputqueue = chain.createBlockingQueue();
        chain.addMapper(inputqueue, outputqueue, context, i);
      }
      // 添加最后一个Mapper，直接输出到任务上下文
      chain.addMapper(outputqueue, context, numMappers - 1);
    }
    
    // 启动所有Mapper线程开始并行处理
    chain.startAllThreads();
    
    // 等待所有Mapper线程执行完成
    chain.joinAllThreads();
  }
}