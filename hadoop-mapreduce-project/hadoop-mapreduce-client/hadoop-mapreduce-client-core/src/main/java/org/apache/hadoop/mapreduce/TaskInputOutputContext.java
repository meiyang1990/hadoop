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

/**
 * MapReduce任务输入输出处理的上下文接口，提供任务读取输入、写入输出的核心能力
 * 仅提供给{@link Mapper}和{@link Reducer}使用，封装任务的输入输出操作
 * @param <KEYIN> 任务输入键类型
 * @param <VALUEIN> 任务输入值类型
 * @param <KEYOUT> 任务输出键类型
 * @param <VALUEOUT> 任务输出值类型
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TaskInputOutputContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
       extends TaskAttemptContext {

  /**
   * 移动到下一个键值对，用于遍历任务的所有输入数据
   * @return 如果还有下一个键值对返回true，到达输入末尾返回false
   * @throws IOException 读取输入时IO异常
   * @throws InterruptedException 线程中断异常
   */
  public boolean nextKeyValue() throws IOException, InterruptedException;
 
  /**
   * 获取当前遍历到的输入键对象
   * @return 当前输入键，没有更多数据时返回null
   * @throws IOException 读取键时IO异常
   * @throws InterruptedException 线程中断异常
   */
  public KEYIN getCurrentKey() throws IOException, InterruptedException;

  /**
   * 获取当前遍历到的输入值对象
   * @return 当前输入值，没有更多数据时返回null
   * @throws IOException 读取值时IO异常
   * @throws InterruptedException 线程中断异常
   */
  public VALUEIN getCurrentValue() throws IOException, InterruptedException;

  /**
   * 输出一个键值对到任务结果
   * @param key 输出键
   * @param value 输出值
   * @throws IOException 写入输出时IO异常
   * @throws InterruptedException 线程中断异常
   */
  public void write(KEYOUT key, VALUEOUT value) 
      throws IOException, InterruptedException;

  /**
   * 获取当前任务尝试的输出提交器，用于输出结果的提交和回滚
   * @return 当前任务尝试的OutputCommitter实例
   */
  public OutputCommitter getOutputCommitter();
}