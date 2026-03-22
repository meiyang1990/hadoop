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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Map任务执行器通用接口，为高级用户自定义Map处理逻辑提供扩展能力。
 * 
 * <p>自定义实现该接口可以对Map处理过程获得更大的控制权限，例如可以实现多线程Map、异步Map等高级处理场景。</p>
 * 
 * @see Mapper
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface MapRunnable<K1, V1, K2, V2>
    extends JobConfigurable {
  
  /** 
   * 启动Map任务执行，处理输入键值对并输出映射结果。
   *  
   * <p>当该方法返回时，表示当前分片的所有输入记录映射处理全部完成。</p>
   * 
   * @param input 输入记录读取器，用于从输入分片读取键值对
   * @param output 输出收集器，用于收集Map处理产生的中间输出键值对
   * @param reporter 进度报告器，用于向框架报告Map任务进度、更新状态
   * @throws IOException 读取输入或写出输出时发生IO异常抛出
   */
  void run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output,
           Reporter reporter)
    throws IOException;
}