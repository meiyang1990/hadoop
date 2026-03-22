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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件路径: hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/lib/input/DelegatingMapper.java
 * <p>
 * 委托型Mapper基类，用于支持多输入路径对应不同Mapper处理的场景
 * 核心职责是根据当前输入分片动态创建并调用对应的实际Mapper，实现多输入类型的兼容处理
 * 
 * @see MultipleInputs#addInputPath(Job, Path, Class, Class)
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DelegatingMapper<K1, V1, K2, V2> extends Mapper<K1, V1, K2, V2> {

  private Mapper<K1, V1, K2, V2> mapper;

  /**
   * 初始化委托Mapper，从输入分片中获取实际Mapper类型并实例化
   * 
   * @param context Map任务运行上下文
   * @throws IOException
   * @throws InterruptedException
   */
  @SuppressWarnings("unchecked")
  protected void setup(Context context)
      throws IOException, InterruptedException {
    // 从上下文获取带标签的输入分片，从中获取Mapper信息
    TaggedInputSplit inputSplit = (TaggedInputSplit) context.getInputSplit();
    // 通过反射实例化输入分片指定的实际Mapper
    mapper = (Mapper<K1, V1, K2, V2>) ReflectionUtils.newInstance(inputSplit
       .getMapperClass(), context.getConfiguration());
    
  }

  /**
   * 委托执行完整的Mapper生命周期，将处理流程转发给实际Mapper
   * 
   * @param context Map任务运行上下文
   * @throws IOException
   * @throws InterruptedException
   */
  @SuppressWarnings("unchecked")
  public void run(Context context) 
      throws IOException, InterruptedException {
    // 执行本类的初始化逻辑，创建实际Mapper
    setup(context);
    // 调用实际Mapper的完整执行流程
    mapper.run(context);
    // 执行本类的清理逻辑
    cleanup(context);
  }
}