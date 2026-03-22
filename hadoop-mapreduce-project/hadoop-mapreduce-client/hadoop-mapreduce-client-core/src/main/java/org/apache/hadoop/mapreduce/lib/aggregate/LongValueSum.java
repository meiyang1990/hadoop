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
 * 长整型数值求和聚合器，为MapReduce聚合框架实现对一系列长整型值的累加求和功能
 * 用于MapReduce的Combiner阶段对相同key的数值进行预聚合，减少网络传输量
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LongValueSum implements ValueAggregator<String> {

  long sum = 0;
    
  /**
   * 默认构造函数，初始化时重置求和结果
   *
   */
  public LongValueSum() {
    reset();
  }

  /**
   * 添加一个新的数值到累加器中，将字符串表示的长整型值解析后累加
   * 
   * @param val 需要累加的对象，其字符串表示对应一个长整型值
   * 
   */
  public void addNextValue(Object val) {
    this.sum += Long.parseLong(val.toString());
  }
    
  /**
   * 添加一个长整型值到累加器中直接累加
   * 
   * @param val 需要累加的长整型值
   * 
   */
  public void addNextValue(long val) {
    this.sum += val;
  }
    
  /**
   * 获取当前累加得到的总和
   * @return 累加求和的最终结果
   */
  public long getSum() {
    return this.sum;
  }
    
  /**
   * 获取聚合结果的字符串报告
   * @return 累加总和的字符串表示
   */
  public String getReport() {
    return ""+sum;
  }

  /**
   * 重置聚合器，清空当前累加结果
   */
  public void reset() {
    sum = 0;
  }

  /**
   * 生成供Combiner使用的聚合输出结果
   * @return 只包含一个元素的字符串数组，元素为累加总和的字符串表示，供Combiner处理
   */
  public ArrayList<String> getCombinerOutput() {
    ArrayList<String> retv = new ArrayList<String>(1);
    retv.add(""+sum);
    return retv;
  }
}