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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.util.Progressable;

/** 
 * MapReduce旧API接口，为MapReduce应用提供任务进度上报、计数器更新、状态更新能力。
 * 
 * <p>Mapper和Reducer可以通过Reporter上报进度，或声明任务仍在正常运行。
 * 当处理单个键值对需要较长时间时，该机制非常关键，可避免框架误判任务超时并杀死任务。
 * 
 * <p>应用也可以通过Reporter更新全局计数器，统计任务运行指标。</p>
 * 
 * @see Progressable
 * @see Counters
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Reporter extends Progressable {
  
  /**
   * 空实现的Reporter常量，所有操作都不执行任何逻辑，用于不需要上报能力的场景。
   */
  public static final Reporter NULL = new Reporter() {
      public void setStatus(String s) {
      }
      public void progress() {
      }
      public Counter getCounter(Enum<?> name) {
        return null;
      }
      public Counter getCounter(String group, String name) {
        return null;
      }
      public void incrCounter(Enum<?> key, long amount) {
      }
      public void incrCounter(String group, String counter, long amount) {
      }
      public InputSplit getInputSplit() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("NULL reporter has no input");
      }
      @Override
      public float getProgress() {
        return 0;
      }
    };

  /**
   * 设置当前任务的状态描述信息。
   * 
   * @param status 当前状态的简要描述
   */
  public abstract void setStatus(String status);
  
  /**
   * 根据枚举类型获取对应计数器对象。
   * 
   * @param name 枚举类型的计数器名称
   * @return 对应分组/名称的计数器对象
   */
  public abstract Counter getCounter(Enum<?> name);

  /**
   * 根据分组和名称获取对应计数器对象。
   * 
   * @param group 计数器分组名称
   * @param name 计数器名称
   * @return 对应分组/名称的计数器对象
   */
  public abstract Counter getCounter(String group, String name);
  
  /**
   * 根据枚举标识，将对应计数器增加指定数值。
   * 
   * @param key 用于标识计数器的枚举类型
   * @param amount 需要增加的数值，非负
   */
  public abstract void incrCounter(Enum<?> key, long amount);
  
  /**
   * 根据分组和计数器名称，将对应计数器增加指定数值。
   * 
   * @param group 计数器分组名称
   * @param counter 计数器名称
   * @param amount 需要增加的数值，非负
   */
  public abstract void incrCounter(String group, String counter, long amount);
  
  /**
   * 获取当前Map任务对应的输入分片对象。
   * 
   * @return 当前Map任务正在读取的输入分片
   * @throws UnsupportedOperationException 如果在Reduce端调用该方法会抛出异常
   */
  public abstract InputSplit getInputSplit() 
    throws UnsupportedOperationException;
  
  /**
   * 获取当前任务的进度。进度范围是0到1之间（包含边界）。
   * @return 当前任务进度值
   */
  public float getProgress();
}