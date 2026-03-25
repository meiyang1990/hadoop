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
 * Double类型值累加聚合器实现，用于对一组double值执行求和聚合计算。
 * 属于MapReduce聚合框架的内置实现，用于分组聚合场景下的数值求和。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DoubleValueSum implements ValueAggregator<String> {

  double sum = 0;

  /**
   * 构造函数，创建Double值累加聚合器并初始化。
   */
  public DoubleValueSum() {
    reset();
  }

  /**
   * 添加新的待聚合值，将其解析为double后累加到总和中。
   * @param val 待累加的对象，其字符串表示为一个double数值
   */
  public void addNextValue(Object val) {
    this.sum += Double.parseDouble(val.toString());
  }

  /**
   * 添加新的待聚合double值，直接累加到总和中。
   * @param val 待累加的double数值
   */
  public void addNextValue(double val) {
    this.sum += val;
  }

  /**
   * 获取聚合结果的字符串表示。
   * @return 累加总和的字符串形式
   */
  public String getReport() {
    return "" + sum;
  }

  /**
   * 获取最终的累加总和结果。
   * @return 所有输入值的累加和
   */
  public double getSum() {
    return this.sum;
  }

  /**
   * 重置累加器，将总和清零，准备下一轮聚合计算。
   */
  public void reset() {
    sum = 0;
  }

  /**
   * 生成供Combiner使用的聚合结果输出。
   * @return 包含一个元素的ArrayList，元素为累加总和的字符串表示
   */
  public ArrayList<String> getCombinerOutput() {
    ArrayList<String> retv = new ArrayList<String>(1);
    retv.add("" + sum);
    return retv;
  }

}