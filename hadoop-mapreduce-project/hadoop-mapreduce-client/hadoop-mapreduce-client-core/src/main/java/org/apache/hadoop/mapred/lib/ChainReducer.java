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
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * ChainReducer类实现了在单个Reducer任务中，将多个Mapper类链式串联在Reducer之后执行的能力
 * <p>
 * 对于Reducer输出的每条记录，链式中的Mapper类会按顺序以流水线方式依次执行：
 * 前一个Mapper的输出会作为后一个Mapper的输入，直到最后一个Mapper，最终输出才会写入任务输出
 * <p>
 * 该设计的核心优势在于链式中的Mapper无需感知自己是在Reducer之后还是链式中执行，
 * 可以复用已有的专用Mapper实现，组合出单个任务内的复合操作，减少磁盘IO开销
 * <p>
 * 使用时需要保证链式中前一个组件输出的键值类型和下一个组件的输入类型匹配，链式框架本身不做类型转换
 * <p>
 * 配合ChainMapper使用，可以构建出形如 <code>[MAP+ / REDUCE MAP*]</code> 的组合式MapReduce作业，
 * 该模式最直接的收益是显著减少磁盘IO开销
 * <p>
 * 重要说明：不需要为ChainReducer单独指定输出键值类型，该信息由链式中最后一个元素（setReducer或addMapper）配置
 * <p>
 * ChainReducer使用示例：
 * <p>
 * <pre>
 * ...
 * conf.setJobName("chain");
 * conf.setInputFormat(TextInputFormat.class);
 * conf.setOutputFormat(TextOutputFormat.class);
 *
 * JobConf mapAConf = new JobConf(false);
 * ...
 * ChainMapper.addMapper(conf, AMap.class, LongWritable.class, Text.class,
 *   Text.class, Text.class, true, mapAConf);
 *
 * JobConf mapBConf = new JobConf(false);
 * ...
 * ChainMapper.addMapper(conf, BMap.class, Text.class, Text.class,
 *   LongWritable.class, Text.class, false, mapBConf);
 *
 * JobConf reduceConf = new JobConf(false);
 * ...
 * ChainReducer.setReducer(conf, XReduce.class, LongWritable.class, Text.class,
 *   Text.class, Text.class, true, reduceConf);
 *
 * ChainReducer.addMapper(conf, CMap.class, Text.class, Text.class,
 *   LongWritable.class, Text.class, false, null);
 *
 * ChainReducer.addMapper(conf, DMap.class, LongWritable.class, Text.class,
 *   LongWritable.class, LongWritable.class, true, null);
 *
 * FileInputFormat.setInputPaths(conf, inDir);
 * FileOutputFormat.setOutputPath(conf, outDir);
 * ...
 *
 * JobClient jc = new JobClient(conf);
 * RunningJob job = jc.submitJob(conf);
 * ...
 * </pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ChainReducer implements Reducer {

  /**
   * 将指定Reducer类设置到链式作业的JobConf中，作为链式的起始组件
   * <p>
   * 需要指定键值在链式节点间的传递方式：按值传递或按引用传递。
   * 如果Reducer期望键值不会被后续收集器修改，必须使用按值传递；
   * 如果不需要该语义保证，可以使用按引用传递优化，避免序列化/反序列化开销
   * <p>
   * 传入的reducerConf配置优先级高于作业全局的JobConf，任务运行时会优先使用reducerConf的配置
   * <p>
   * 重要说明：不需要为ChainReducer单独指定输出键值类型，该信息由链式中最后一个元素配置
   *
   * @param job              作业的JobConf对象
   * @param klass            要添加的Reducer类
   * @param inputKeyClass    Reducer输入键类型
   * @param inputValueClass  Reducer输入值类型
   * @param outputKeyClass   Reducer输出键类型
   * @param outputValueClass Reducer输出值类型
   * @param byValue          是否按值传递键值给链式中下一个Mapper
   * @param reducerConf      Reducer专属配置，优先级高于全局作业配置
   *                         推荐使用不加载默认配置的JobConf(boolean loadDefaults)构造，传入false
   */
  public static <K1, V1, K2, V2> void setReducer(JobConf job,
                           Class<? extends Reducer<K1, V1, K2, V2>> klass,
                           Class<? extends K1> inputKeyClass,
                           Class<? extends V1> inputValueClass,
                           Class<? extends K2> outputKeyClass,
                           Class<? extends V2> outputValueClass,
                           boolean byValue, JobConf reducerConf) {
    job.setReducerClass(ChainReducer.class);
    job.setOutputKeyClass(outputKeyClass);
    job.setOutputValueClass(outputValueClass);
    Chain.setReducer(job, klass, inputKeyClass, inputValueClass, outputKeyClass,
                     outputValueClass, byValue, reducerConf);
  }

  /**
   * 将指定Mapper类添加到链式作业的JobConf中，串联在已有组件之后
   * <p>
   * 需要指定键值在链式节点间的传递方式：按值传递或按引用传递。
   * 如果Mapper期望键值不会被后续收集器修改，必须使用按值传递；
   * 如果不需要该语义保证，可以使用按引用传递优化，避免序列化/反序列化开销
   * <p>
   * 传入的mapperConf配置优先级高于作业全局的JobConf，任务运行时会优先使用mapperConf的配置
   * <p>
   * 重要说明：不需要为ChainMapper单独指定输出键值类型，该信息由链式中最后一个Mapper配置
   *
   * @param job              链式作业的JobConf对象
   * @param klass            要添加的Mapper类
   * @param inputKeyClass    Mapper输入键类型
   * @param inputValueClass  Mapper输入值类型
   * @param outputKeyClass   Mapper输出键类型
   * @param outputValueClass Mapper输出值类型
   * @param byValue          是否按值传递键值给链式中下一个Mapper
   * @param mapperConf       Mapper专属配置，优先级高于全局作业配置
   *                         推荐使用不加载默认配置的JobConf(boolean loadDefaults)构造，传入false
   */
  public static <K1, V1, K2, V2> void addMapper(JobConf job,
                           Class<? extends Mapper<K1, V1, K2, V2>> klass,
                           Class<? extends K1> inputKeyClass,
                           Class<? extends V1> inputValueClass,
                           Class<? extends K2> outputKeyClass,
                           Class<? extends V2> outputValueClass,
                           boolean byValue, JobConf mapperConf) {
    job.setOutputKeyClass(outputKeyClass);
    job.setOutputValueClass(outputValueClass);
    Chain.addMapper(false, job, klass, inputKeyClass, inputValueClass,
                    outputKeyClass, outputValueClass, byValue, mapperConf);
  }

  private Chain chain;

  /**
   * 构造方法，初始化链式执行器
   */
  public ChainReducer() {
    chain = new Chain(false);
  }

  /**
   * 配置ChainReducer、内部Reducer以及链式中所有Mapper
   * <p>
   * 如果子类重写该方法，必须在重写方法开头调用super.configure(...)
   * @param job 作业配置对象
   */
  public void configure(JobConf job) {
    chain.configure(job);
  }

  /**
   * 执行链式处理：先调用配置的Reducer的reduce方法，再依次调用所有链式Mapper的map方法
   */
  @SuppressWarnings({"unchecked"})
  public void reduce(Object key, Iterator values, OutputCollector output,
                     Reporter reporter) throws IOException {
    Reducer reducer = chain.getReducer();
    if (reducer != null) {
      reducer.reduce(key, values, chain.getReducerCollector(output, reporter),
                     reporter);
    }
  }

  /**
   * 关闭ChainReducer、内部Reducer以及链式中所有Mapper，释放资源
   * <p>
   * 如果子类重写该方法，必须在重写方法末尾调用super.close()
   */
  public void close() throws IOException {
    chain.close();
  }

}