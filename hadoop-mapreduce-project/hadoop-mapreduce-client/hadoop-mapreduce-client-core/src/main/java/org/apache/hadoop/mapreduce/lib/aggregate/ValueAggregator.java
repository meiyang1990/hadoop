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

package org.apache.hadoop.mapreduce.lib.aggregate;

import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 数值聚合器接口，定义了MapReduce聚合计算中聚合器需要实现的基础协议
 * 用于MapReduce聚合框架中，对相同key的多个value执行自定义聚合计算
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ValueAggregator<E> {

  /**
   * 添加一个新值到聚合器，参与聚合计算
   * @param val 待添加聚合的输入值
   */
  public void addNextValue(Object val);

  /**
   * 重置聚合器状态，清空当前已聚合的所有数据
   */
  public void reset();

  /**
   * 获取聚合结果的字符串报表输出
   * @return 聚合计算结果的字符串表示
   */
  public String getReport();

  /**
   * 获取combiner阶段的输出值列表
   * @return 作为combiner输出的聚合结果数组
   */
  public ArrayList<E> getCombinerOutput();

}