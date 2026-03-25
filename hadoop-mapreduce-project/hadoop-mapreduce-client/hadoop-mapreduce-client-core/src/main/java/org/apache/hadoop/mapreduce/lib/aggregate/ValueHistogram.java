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
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 文件：值直方图聚合器实现
 * 功能：为MapReduce聚合框架提供字符串值序列的直方图统计能力，计算并输出直方图的基础统计信息
 * 所属模块：MapReduce客户端核心，用于离线聚合分析场景
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ValueHistogram implements ValueAggregator<String> {

  TreeMap<Object, Object> items = null;

  /**
   * 构造空直方图聚合器，初始化空TreeMap存储值频率
   */
  public ValueHistogram() {
    items = new TreeMap<Object, Object>();
  }

  /**
   * 将新输入值添加到直方图聚合器中，累计对应值的出现次数
   * @param val 待添加的值，格式为"xxxx\tnum"表示xxxx出现num次，不指定num时默认次数为1
   */
  public void addNextValue(Object val) {
    String valCountStr = val.toString();
    // 拆分值和次数的制表符分隔位置
    int pos = valCountStr.lastIndexOf("\t");
    String valStr = valCountStr;
    String countStr = "1";
    if (pos >= 0) {
      // 拆分得到值和增量次数
      valStr = valCountStr.substring(0, pos);
      countStr = valCountStr.substring(pos + 1);
    }
    
    Long count = (Long) this.items.get(valStr);
    long inc = Long.parseLong(countStr);

    if (count == null) {
      count = inc;
    } else {
      count = count.longValue() + inc;
    }
    // 更新当前值的总次数
    items.put(valStr, count);
  }

  /**
   * 生成直方图的汇总统计报告，包含基本统计指标
   * @return 制表符分隔的统计结果，依次为：唯一值数量、最小次数、中位数次数、最大次数、平均次数、标准差
   */
  public String getReport() {
    // 提取所有唯一值对应的次数数组
    long[] counts = new long[items.size()];

    StringBuilder sb = new StringBuilder();
    Iterator<Object> iter = items.values().iterator();
    int i = 0;
    while (iter.hasNext()) {
      Long count = (Long) iter.next();
      counts[i] = count.longValue();
      i += 1;
    }
    // 对次数数组排序，方便计算统计量
    Arrays.sort(counts);
    sb.append(counts.length);
    i = 0;
    long acc = 0;
    // 计算总次数和
    while (i < counts.length) {
      long nextVal = counts[i];
      int j = i + 1;
      while (j < counts.length && counts[j] == nextVal) {
        j++;
      }
      acc += nextVal * (j - i);
      i = j;
    }
    double average = 0.0;
    double sd = 0.0;
    if (counts.length > 0) {
      // 添加最小次数
      sb.append("\t").append(counts[0]);
      // 添加中位数次数
      sb.append("\t").append(counts[counts.length / 2]);
      // 添加最大次数
      sb.append("\t").append(counts[counts.length - 1]);

      // 计算平均次数
      average = acc * 1.0 / counts.length;
      sb.append("\t").append(average);

      // 计算方差和标准差
      i = 0;
      while (i < counts.length) {
        double nextDiff = counts[i] - average;
        sd += nextDiff * nextDiff;
        i += 1;
      }
      sd = Math.sqrt(sd / counts.length);
      sb.append("\t").append(sd);

    }
    return sb.toString();
  }

  /**
   * 生成直方图所有值频率对的详细报告
   * @return 每行一个值频率对，格式为"\t值\t次数\n"的详细字符串
   */
  public String getReportDetails() {
    StringBuilder sb = new StringBuilder();
    Iterator<Entry<Object,Object>> iter = items.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<Object,Object> en = iter.next();
      Object val = en.getKey();
      Long count = (Long) en.getValue();
      sb.append("\t").append(val.toString()).append("\t").
         append(count.longValue()).append("\n");
    }
    return sb.toString();
  }

  /**
   * 生成Combiner阶段的输出列表，供Reduce阶段聚合使用
   * @return 每个元素为"值\t次数"格式字符串的列表，所有值频率对
   */
  public ArrayList<String> getCombinerOutput() {
    ArrayList<String> retv = new ArrayList<String>();
    Iterator<Entry<Object,Object>> iter = items.entrySet().iterator();

    while (iter.hasNext()) {
      Entry<Object,Object> en =  iter.next();
      Object val = en.getKey();
      Long count = (Long) en.getValue();
      retv.add(val.toString() + "\t" + count.longValue());
    }
    return retv;
  }

  /**
   * 获取直方图内部存储的完整TreeMap结构
   * @return 存储所有值和对应次数的TreeMap，键为值，值为次数
   */
  public TreeMap<Object,Object> getReportItems() {
    return items;
  }

  /**
   * 重置聚合器，清空所有统计数据，准备下一轮聚合
   */
  public void reset() {
    items = new TreeMap<Object, Object>();
  }

}