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
 * Licensed to the Apache Software Foundation (ASF) under one
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件：DelegatingMapper.java
 * 所属模块：Hadoop MapReduce 客户端核心模块
 * 核心职责：实现多输入路径场景下的Mapper委托转发，支持不同输入路径使用不同的Mapper处理逻辑
 * 
 * 支持多输入路径场景：当一个作业配置了多个输入路径，每个路径绑定不同的Mapper实现时，
 * 该类作为统一入口，根据输入分片动态加载对应路径配置的Mapper，并将map操作委托给目标Mapper处理
 * 
 * @see MultipleInputs#addInputPath(JobConf, Path, Class, Class)
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
/**
 * 多输入场景的Mapper委托转发类，根据输入分片动态选择对应Mapper处理
 * @param <K1> Map输入键类型
 * @param <V1> Map输入值类型
 * @param <K2> Map输出键类型
 * @param <V2> Map输出值类型
 */
public class DelegatingMapper<K1, V1, K2, V2> implements Mapper<K1, V1, K2, V2> {

  private JobConf conf;

  private Mapper<K1, V1, K2, V2> mapper;

  /**
   * 执行map操作，延迟初始化目标Mapper并将请求转发给目标Mapper处理
   * @param key 输入键
   * @param value 输入值
   * @param outputCollector 输出收集器
   * @param reporter 任务报告器
   * @throws IOException IO异常
   */
  @SuppressWarnings("unchecked")
  public void map(K1 key, V1 value, OutputCollector<K2, V2> outputCollector,
      Reporter reporter) throws IOException {

    if (mapper == null) {
      // 从Reporter获取标记过的输入分片，获取目标Mapper类型
      TaggedInputSplit inputSplit = (TaggedInputSplit) reporter.getInputSplit();
      // 通过反射实例化目标Mapper对象
      mapper = (Mapper<K1, V1, K2, V2>) ReflectionUtils.newInstance(inputSplit
         .getMapperClass(), conf);
    }
    // 委托目标Mapper执行实际的map处理
    mapper.map(key, value, outputCollector, reporter);
  }

  /**
   * 配置Mapper，保存作业配置对象
   * @param conf 作业配置对象
   */
  public void configure(JobConf conf) {
    this.conf = conf;
  }

  /**
   * 关闭Mapper，如果目标Mapper已初始化则调用其close方法进行资源清理
   * @throws IOException IO异常
   */
  public void close() throws IOException {
    if (mapper != null) {
      mapper.close();
    }
  }

}