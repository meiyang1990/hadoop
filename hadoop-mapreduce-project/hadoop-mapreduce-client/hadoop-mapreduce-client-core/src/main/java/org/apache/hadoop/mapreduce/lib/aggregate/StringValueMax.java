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
 * 字符串最大值聚合器实现，用于在MapReduce聚合任务中维护字符串序列的字典序最大值
 * 作为ValueAggregator的实现类，配合MapReduce聚合框架完成同key字符串的最大值计算
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class StringValueMax implements ValueAggregator<String> {

  String maxVal = null;
    
  /**
   * 默认构造函数，初始化时重置聚合状态
   */
  public StringValueMax() {
    reset();
  }

  /**
   * 添加新的待聚合值，更新当前维护的最大值
   * @param val 待添加的字符串值
   */
  public void addNextValue(Object val) {
    String newVal = val.toString();
    if (this.maxVal == null || this.maxVal.compareTo(newVal) < 0) {
      this.maxVal = newVal;
    }
  }
    
    
  /**
   * 获取聚合得到的最大值结果
   * @return 字典序最大的字符串
   */
  public String getVal() {
    return this.maxVal;
  }
    
  /**
   * 获取聚合结果的字符串表示形式
   * @return 聚合结果最大值字符串
   */
  public String getReport() {
    return maxVal;
  }

  /**
   * 重置聚合器状态，清空当前保存的最大值
   */
  public void reset() {
    maxVal = null;
  }

  /**
   * 生成供Combiner使用的聚合输出结果
   * @return 包含一个元素的ArrayList，元素为当前聚合得到的最大值字符串
   */
  public ArrayList<String> getCombinerOutput() {
    ArrayList<String> retv = new ArrayList<String>(1);
    retv.add(maxVal);
    return retv;
  }
}