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
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Reduce任务执行上下文接口，为Reducer提供运行时环境和数据访问能力
 * 提供输入键值对访问、输出结果写入等核心能力，是Reducer与MapReduce框架交互的入口
 * @param <KEYIN> 输入键的类型
 * @param <VALUEIN> 输入值的类型
 * @param <KEYOUT> 输出键的类型
 * @param <VALUEOUT> 输出值的类型
 */
@InterfaceAudience.PPublic
@InterfaceStability.Evolving
public interface ReduceContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    extends TaskInputOutputContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

  /**
   * 移动到下一个唯一输入键，准备处理该键对应的所有值
   * @return 是否存在下一个可处理的唯一键，true表示存在，false表示所有键处理完成
   * @throws IOException 当IO操作异常时抛出
   * @throws InterruptedException 当线程被中断时抛出
   */
  public boolean nextKey() throws IOException,InterruptedException;

  /**
   * 获取当前键对应的所有值的可迭代对象，复用同一个值对象减少GC
   * @return 当前键关联的所有值的可迭代对象，迭代返回的对象会被框架复用
   * @throws IOException 当IO操作异常时抛出
   * @throws InterruptedException 当线程被中断时抛出
   */
  public Iterable<VALUEIN> getValues() throws IOException, InterruptedException;

  /**
   * 值迭代器接口，用于遍历同一个键分组下的所有值
   * 继承可标记迭代器接口，支持备份与恢复迭代位置
   */
  interface ValueIterator<VALUEIN> extends MarkableIteratorInterface<VALUEIN> {

    /**
     * 当Reducer处理完当前键、切换到下一个键时调用该方法
     * 重置备份存储，为下一个键的值迭代做准备
     * @throws IOException 当IO操作异常时抛出
     */
    void resetBackupStore() throws IOException;
  }
}