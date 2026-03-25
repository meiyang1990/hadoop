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

package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;

/** 
 * MapReduce Map阶段的基类，用户自定义Mapper需要继承此类。
 * 负责将输入的键值对转换为一组中间键值对，供后续Reduce阶段处理。
 * 
 * <p>每个Map任务对应输入分片{@link InputSplit}，框架会为每个分片生成一个Mapper实例执行。
 * 执行流程为：调用{@link #setup(Context)}进行初始化 -> 对分片中每个键值对调用{@link #map(Object, Object, Context)} -> 最终调用{@link #cleanup(Context)}清理资源。</p>
 * 
 * <p>输出的中间键值对会按key分组后传递给{@link Reducer}处理，用户可通过自定义{@link RawComparator}控制分组排序逻辑。
 * 可通过自定义{@link Partitioner}控制key分配到哪个Reducer，也可通过指定Combiner实现本地聚合减少数据传输量。
 * 如果作业不需要Reduce阶段，Mapper输出会直接写入输出格式，无需按key排序。</p>
 * 
 * @see InputFormat
 * @see JobContext
 * @see Partitioner  
 * @see Reducer
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  /**
   * Mapper执行上下文，封装了Map任务的运行环境，提供读写输出、获取配置等能力。
   */
  public abstract class Context
    implements MapContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
  }
  
  /**
   * Map任务开始执行前的初始化方法，在整个Map任务生命周期只调用一次。
   * 可用于加载全局资源、初始化变量等准备工作。
   * @param context Mapper执行上下文
   * @throws IOException
   * @throws InterruptedException
   */
  protected void setup(Context context
                       ) throws IOException, InterruptedException {
    // NOTHING
  }

  /**
   * 对输入分片中每个键值对调用一次，是Map阶段核心处理逻辑，用户业务需要重写此方法。
   * 默认实现是直接将输入键值对作为输出写出（身份映射）。
   * @param key 输入键
   * @param value 输入值
   * @param context Mapper执行上下文，用于写出中间结果
   * @throws IOException
   * @throws InterruptedException
   */
  @SuppressWarnings("unchecked")
  protected void map(KEYIN key, VALUEIN value, 
                     Context context) throws IOException, InterruptedException {
    context.write((KEYOUT) key, (VALUEOUT) value);
  }

  /**
   * Map任务所有键值对处理完成后调用，在整个Map任务生命周期只调用一次。
   * 可用于清理资源、关闭连接等收尾工作。
   * @param context Mapper执行上下文
   * @throws IOException
   * @throws InterruptedException
   */
  protected void cleanup(Context context
                         ) throws IOException, InterruptedException {
    // NOTHING
  }
  
  /**
   * Mapper任务的主执行方法，高级用户可重写此方法实现完全自定义的Map处理流程（例如多线程Map）。
   * 默认实现按setup -> 循环处理所有键值对调用map -> cleanup的标准流程执行。
   * @param context Mapper执行上下文
   * @throws IOException
   * @throws InterruptedException
   */
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    try {
      // 循环遍历当前分片所有输入键值对，逐行调用map方法处理
      while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
      }
    } finally {
      // 确保无论处理是否异常，都会执行cleanup
      cleanup(context);
    }
  }
}