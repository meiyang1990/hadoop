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
 * 长整型最小值聚合器，用于MapReduce聚合框架中维护一组长整型值的最小值
 * 配合ValueAggregator框架实现分组聚合计算，常用于离线统计场景
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LongValueMin implements ValueAggregator<String> {

  long minVal = Long.MAX_VALUE;
    
  /**
   *  默认构造方法，初始化聚合器并重置状态
   */
  public LongValueMin() {
    reset();
  }

  /**
   * 添加新的待聚合值，更新当前最小值
   * @param val 待添加的值，其字符串形式表示一个长整型
   */
  public void addNextValue(Object val) {
    long newVal = Long.parseLong(val.toString());
    if (this.minVal > newVal) {
      this.minVal = newVal;
    }
  }
    
  /**
   * 添加新的长整型值，更新当前最小值
   * @param newVal 待添加的长整型值
   */
  public void addNextValue(long newVal) {
    if (this.minVal > newVal) {
      this.minVal = newVal;
    };
  }
    
  /**
   * 获取聚合后的最小值结果
   * @return 当前聚合得到的最小值
   */
  public long getVal() {
    return this.minVal;
  }
    
  /**
   * 获取聚合结果的字符串表示，用于最终输出报告
   * @return 最小值的字符串形式
   */
  public String getReport() {
    return ""+minVal;
  }

  /**
   * 重置聚合器状态，将最小值恢复为初始最大值
   */
  public void reset() {
    minVal = Long.MAX_VALUE;
  }

  /**
   * 生成Combiner阶段的输出结果，供聚合框架合并map端局部结果
   * @return 只包含一个元素的列表，元素为当前聚合结果的字符串表示
   */
  public ArrayList<String> getCombinerOutput() {
    ArrayList<String> retv = new ArrayList<String>(1);
    retv.add(""+minVal);
    return retv;
  }
}