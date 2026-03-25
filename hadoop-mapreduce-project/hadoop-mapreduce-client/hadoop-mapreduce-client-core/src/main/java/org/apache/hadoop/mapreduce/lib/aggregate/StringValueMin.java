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
 * 字符串最小值聚合器，用于在MapReduce聚合任务中维护输入字符串序列的字典序最小值
 * 是ValueAggregator接口的实现类，用于MapReduce端聚合计算场景
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class StringValueMin implements ValueAggregator<String> {

  String minVal = null;
    
  /**
   * 默认构造函数，初始化后重置聚合器状态
   */
  public StringValueMin() {
    reset();
  }

  /**
   * 添加新的字符串值，更新当前维护的最小值
   * @param val 待添加的新值对象
   */
  public void addNextValue(Object val) {
    String newVal = val.toString();
    if (this.minVal == null || this.minVal.compareTo(newVal) > 0) {
      this.minVal = newVal;
    }
  }
    
    
  /**
   * 获取当前聚合得到的最小值
   * @return 字典序最小的字符串
   */
  public String getVal() {
    return this.minVal;
  }
    
  /**
   * 获取聚合结果的字符串表示
   * @return 聚合结果字符串
   */
  public String getReport() {
    return minVal;
  }

  /**
   * 重置聚合器状态，清空当前最小值
   */
  public void reset() {
    minVal = null;
  }

  /**
   * 生成供Combiner使用的输出列表
   * @return 仅包含一个元素的ArrayList，元素为当前聚合得到的最小值字符串
   */
  public ArrayList<String> getCombinerOutput() {
    ArrayList<String> retv = new ArrayList<String>(1);
    retv.add(minVal);
    return retv;
  }
}