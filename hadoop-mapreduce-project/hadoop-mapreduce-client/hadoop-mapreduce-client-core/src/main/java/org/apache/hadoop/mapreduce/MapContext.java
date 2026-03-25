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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Map任务执行上下文接口，提供给{@link Mapper}使用，封装Map任务的运行环境信息
 * 提供输入输出、配置、计数器等核心能力的访问，继承TaskInputOutputContext通用能力
 * @param <KEYIN> Mapper输入键类型
 * @param <VALUEIN> Mapper输入值类型
 * @param <KEYOUT> Mapper输出键类型
 * @param <VALUEOUT> Mapper输出值类型
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface MapContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
  extends TaskInputOutputContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

  /**
   * 获取当前Map任务处理的输入分片
   * @return 当前Map任务对应的输入分片对象
   */
  public InputSplit getInputSplit();
  
}