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
 * 长整型最大值聚合器，用于在MapReduce聚合计算中维护一组长整型数值的最大值
 * 属于MapReduce聚合框架的内置实现，适用于需要计算分组最大值的场景
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LongValueMax implements ValueAggregator<String> {

  long maxVal = Long.MIN_VALUE;
    
  /**
   * 默认构造函数，初始化时重置最大值
   */
  public LongValueMax() {
    reset();
  }

  /**
   * 添加一个新的数值到聚合器，更新当前最大值
   * @param val 待添加的数值对象，其字符串表示对应一个长整型值
   */
  public void addNextValue(Object val) {
    long newVal = Long.parseLong(val.toString());
    if (this.maxVal < newVal) {
      this.maxVal = newVal;
    }
  }
    
  /**
   * 添加一个新的长整型值到聚合器，更新当前最大值
   * @param newVal 待添加的长整型值
   */
  public void addNextValue(long newVal) {
    if (this.maxVal < newVal) {
      this.maxVal = newVal;
    };
  }
    
  /**
   * 获取当前聚合得到的最大值
   * @return 聚合后的最大值
   */
  public long getVal() {
    return this.maxVal;
  }
    
  /**
   * 获取聚合结果的字符串表示，用于输出报告
   * @return 最大值的字符串形式
   */
  public String getReport() {
    return ""+maxVal;
  }

  /**
   * 重置聚合器，清空当前计算的最大值，恢复初始状态
   */
  public void reset() {
    maxVal = Long.MIN_VALUE;
  }

  /**
   * 生成Combiner阶段的输出，供MapReduce聚合框架使用
   * @return 包含一个元素的ArrayList，元素是当前聚合结果的字符串表示
   */
  public ArrayList<String> getCombinerOutput() {
    ArrayList<String> retv = new ArrayList<String>(1);
    retv.add("" + maxVal);
    return retv;
  }
}