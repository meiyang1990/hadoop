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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * 文件: ChainMapper.java
 * 所属模块: hadoop-mapreduce-client-core
 * 核心职责: 支持在单个Map任务中链式执行多个Mapper组件，实现多步Map处理串行化执行，减少磁盘IO开销
 * 
 * 功能说明: 多个Mapper按顺序链式执行，前一个Mapper的输出直接作为后一个Mapper的输入，
 * 最后一个Mapper的输出才会写入任务输出。所有Mapper无需感知链式执行，保持可复用性。
 * 该模式可以显著减少Map阶段中间结果的磁盘IO，适合组合多个专用Mapper完成复合处理。
 * 
 * 使用场景: 需要在Map阶段进行多步数据处理（过滤、转换、提取等），避免每个步骤单独启动任务，提升执行效率。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ChainMapper implements Mapper {

  /**
   * 向作业配置中添加一个Mapper到链式执行队列
   * 
   * @param job 作业配置对象，用于保存链式Mapper信息
   * @param klass 要添加的Mapper类
   * @param inputKeyClass 当前Mapper输入键类型
   * @param inputValueClass 当前Mapper输入值类型
   * @param outputKeyClass 当前Mapper输出键类型
   * @param outputValueClass 当前Mapper输出值类型
   * @param byValue 是否按值传递键值对给下一个Mapper，true为按值传递（拷贝后传递，避免修改冲突），false为按引用传递（性能优化，避免序列化）
   * @param mapperConf 当前Mapper的专属配置，优先级高于作业全局配置，推荐使用不加载默认配置的空JobConf
   */
  public static <K1, V1, K2, V2> void addMapper(JobConf job,
                           Class<? extends Mapper<K1, V1, K2, V2>> klass,
                           Class<? extends K1> inputKeyClass,
                           Class<? extends V1> inputValueClass,
                           Class<? extends K2> outputKeyClass,
                           Class<? extends V2> outputValueClass,
                           boolean byValue, JobConf mapperConf) {
    // 设置当前作业的Mapper为ChainMapper，由它负责调度链式执行
    job.setMapperClass(ChainMapper.class);
    // 设置整个ChainMapper的输出类型为当前添加的Mapper输出类型，最后添加的Mapper会覆盖此配置
    job.setMapOutputKeyClass(outputKeyClass);
    job.setMapOutputValueClass(outputValueClass);
    // 调用Chain通用方法添加Mapper到链式队列
    Chain.addMapper(true, job, klass, inputKeyClass, inputValueClass,
                    outputKeyClass, outputValueClass, byValue, mapperConf);
  }

  // 链式执行核心实例，管理所有Mapper的生命周期和调用逻辑
  private Chain chain;

  /**
   * ChainMapper构造方法，初始化链式执行容器
   */
  public ChainMapper() {
    chain = new Chain(true);
  }

  /**
   * 配置ChainMapper，初始化链中所有Mapper实例和配置
   * 
   * 子类覆盖此方法时，必须在开头调用super.configure()保证初始化正确
   * @param job 作业配置对象
   */
  public void configure(JobConf job) {
    chain.configure(job);
  }

  /**
   * 执行链式Map处理，调用链中第一个Mapper，后续由链式收集器自动触发后续Mapper
   * @param key Map任务输入键
   * @param value Map任务输入值
   * @param output 任务输出收集器
   * @param reporter 任务进度报告器
   * @throws IOException 处理过程IO异常
   */
  @SuppressWarnings({"unchecked"})
  public void map(Object key, Object value, OutputCollector output,
                  Reporter reporter) throws IOException {
    // 获取链中第一个Mapper
    Mapper mapper = chain.getFirstMap();
    if (mapper != null) {
      // 调用第一个Mapper，传入封装好的链式收集器，自动触发后续Mapper执行
      mapper.map(key, value, chain.getMapperCollector(0, output, reporter),
                 reporter);
    }
  }

  /**
   * 关闭ChainMapper，依次关闭链中所有Mapper，释放资源
   * 
   * 子类覆盖此方法时，必须在结尾调用super.close()保证资源正确释放
   * @throws IOException 关闭过程IO异常
   */
  public void close() throws IOException {
    chain.close();
  }

}