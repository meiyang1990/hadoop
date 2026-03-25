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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.Chain.ChainBlockingQueue;

import java.io.IOException;

/**
 * 文件级注释：
 * ChainReducer 实现了Reduce任务内部的Reducer后串联多个Mapper的执行模式，
 * 允许将Reducer的输出依次经过多个Mapper处理后再输出，减少MapReduce作业的磁盘IO，
 * 支持构建 [MAP+ / REDUCE MAP*] 形式的复合作业，提升执行效率。
 *
 * The ChainReducer class allows to chain multiple Mapper classes after a
 * Reducer within the Reducer task.
 * 
 * <p>
 * For each record output by the Reducer, the Mapper classes are invoked in a
 * chained (or piped) fashion. The output of the reducer becomes the input of
 * the first mapper and output of first becomes the input of the second, and so
 * on until the last Mapper, the output of the last Mapper will be written to
 * the task's output.
 * </p>
 * <p>
 * The key functionality of this feature is that the Mappers in the chain do not
 * need to be aware that they are executed after the Reducer or in a chain. This
 * enables having reusable specialized Mappers that can be combined to perform
 * composite operations within a single task.
 * </p>
 * <p>
 * Special care has to be taken when creating chains that the key/values output
 * by a Mapper are valid for the following Mapper in the chain. It is assumed
 * all Mappers and the Reduce in the chain use matching output and input key and
 * value classes as no conversion is done by the chaining code.
 * </p>
 * <p> Using the ChainMapper and the ChainReducer classes is possible to
 * compose Map/Reduce jobs that look like <code>[MAP+ / REDUCE MAP*]</code>. And
 * immediate benefit of this pattern is a dramatic reduction in disk IO. </p>
 * <p>
 * IMPORTANT: There is no need to specify the output key/value classes for the
 * ChainReducer, this is done by the setReducer or the addMapper for the last
 * element in the chain.
 * </p>
 * ChainReducer usage pattern:
 * <p>
 * 
 * <pre>
 * ...
 * Job = new Job(conf);
 * ....
 *
 * Configuration reduceConf = new Configuration(false);
 * ...
 * ChainReducer.setReducer(job, XReduce.class, LongWritable.class, Text.class,
 *   Text.class, Text.class, true, reduceConf);
 *
 * ChainReducer.addMapper(job, CMap.class, Text.class, Text.class,
 *   LongWritable.class, Text.class, false, null);
 *
 * ChainReducer.addMapper(job, DMap.class, LongWritable.class, Text.class,
 *   LongWritable.class, LongWritable.class, true, null);
 *
 * ...
 *
 * job.waitForCompletion(true);
 * ...
 * </pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ChainReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends
    Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  /**
   * 向作业设置ChainReducer需要执行的核心Reducer，将其加入链式处理流程。
   * 传入的Reducer配置优先级高于作业全局配置，任务运行时生效。
   * 
   * <p>
   * The key and values are passed from one element of the chain to the next, by
   * value. For the added Reducer the configuration given for it,
   * <code>reducerConf</code>, have precedence over the job's Configuration.
   * This precedence is in effect when the task is running.
   * </p>
   * <p>
   * IMPORTANT: There is no need to specify the output key/value classes for the
   * ChainReducer, this is done by the setReducer or the addMapper for the last
   * element in the chain.
   * </p>
   * 
   * @param job
   *          the job
   * @param klass
   *          the Reducer class to add.
   * @param inputKeyClass
   *          reducer input key class.
   * @param inputValueClass
   *          reducer input value class.
   * @param outputKeyClass
   *          reducer output key class.
   * @param outputValueClass
   *          reducer output value class.
   * @param reducerConf
   *          a configuration for the Reducer class. It is recommended to use a
   *          configuration without default values using the
   *          <code>Configuration(boolean loadDefaults)</code> constructor with
   *          FALSE.
   */
  public static void setReducer(Job job, Class<? extends Reducer> klass,
      Class<?> inputKeyClass, Class<?> inputValueClass,
      Class<?> outputKeyClass, Class<?> outputValueClass,
      Configuration reducerConf) {
    job.setReducerClass(ChainReducer.class);
    job.setOutputKeyClass(outputKeyClass);
    job.setOutputValueClass(outputValueClass);
    Chain.setReducer(job, klass, inputKeyClass, inputValueClass,
        outputKeyClass, outputValueClass, reducerConf);
  }

  /**
   * 向ChainReducer的链式流程中添加一个Mapper，放在Reducer之后执行。
   * 传入的Mapper配置优先级高于作业全局配置，任务运行时生效。
   * 
   * <p>
   * The key and values are passed from one element of the chain to the next, by
   * value For the added Mapper the configuration given for it,
   * <code>mapperConf</code>, have precedence over the job's Configuration. This
   * precedence is in effect when the task is running.
   * </p>
   * <p>
   * IMPORTANT: There is no need to specify the output key/value classes for the
   * ChainMapper, this is done by the addMapper for the last mapper in the
   * chain.
   * </p>
   * 
   * @param job
   *          The job.
   * @param klass
   *          the Mapper class to add.
   * @param inputKeyClass
   *          mapper input key class.
   * @param inputValueClass
   *          mapper input value class.
   * @param outputKeyClass
   *          mapper output key class.
   * @param outputValueClass
   *          mapper output value class.
   * @param mapperConf
   *          a configuration for the Mapper class. It is recommended to use a
   *          configuration without default values using the
   *          <code>Configuration(boolean loadDefaults)</code> constructor with
   *          FALSE.
   */
  public static void addMapper(Job job, Class<? extends Mapper> klass,
      Class<?> inputKeyClass, Class<?> inputValueClass,
      Class<?> outputKeyClass, Class<?> outputValueClass,
      Configuration mapperConf) throws IOException {
    job.setOutputKeyClass(outputKeyClass);
    job.setOutputValueClass(outputValueClass);
    Chain.addMapper(false, job, klass, inputKeyClass, inputValueClass,
        outputKeyClass, outputValueClass, mapperConf);
  }

  private Chain chain;

  /**
   * 初始化链式处理环境，构建Chain实例并加载配置。
   */
  protected void setup(Context context) {
    chain = new Chain(false);
    chain.setup(context.getConfiguration());
  }

  /**
   * ChainReducer主执行方法，按顺序初始化并启动链式处理流程，协调所有Reducer和Mapper的并发执行。
   */
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);

    // 未设置Reducer时直接返回，不执行任何处理
    if (chain.getReducer() == null) {
      return;
    }
    int numMappers = chain.getAllMappers().size();
    // 没有串联Mapper时，直接执行Reducer后输出结果
    if (numMappers == 0) {
      chain.runReducer(context);
      return;
    }

    // 声明链式处理各环节的输入输出阻塞队列
    ChainBlockingQueue<Chain.KeyValuePair<?, ?>> inputqueue;
    ChainBlockingQueue<Chain.KeyValuePair<?, ?>> outputqueue;
    // 添加Reducer到链式处理，创建其输出队列
    outputqueue = chain.createBlockingQueue();
    chain.addReducer(context, outputqueue);
    // 添加除最后一个Mapper外的所有串联Mapper，每个Mapper的输出作为下一个的输入
    for (int i = 0; i < numMappers - 1; i++) {
      inputqueue = outputqueue;
      outputqueue = chain.createBlockingQueue();
      chain.addMapper(inputqueue, outputqueue, context, i);
    }
    // 添加最后一个Mapper，直接输出到任务上下文
    chain.addMapper(outputqueue, context, numMappers - 1);

    // 启动所有链式处理的工作线程
    chain.startAllThreads();
    
    // 等待所有线程执行完成，结束处理
    chain.joinAllThreads();
  }
}