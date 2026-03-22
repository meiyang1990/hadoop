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

package org.apache.hadoop.mapred.lib.aggregate;

import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

/** 
 * 提供ValueAggregatorDescriptor子类需要复用的通用基础功能，兼容旧版Mapred API
 * 是MapReduce聚合框架中聚合器描述符的基础实现类
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ValueAggregatorBaseDescriptor extends org.apache.hadoop.mapreduce.
    lib.aggregate.ValueAggregatorBaseDescriptor 
    implements ValueAggregatorDescriptor {

  static public final String UNIQ_VALUE_COUNT = org.apache.hadoop.mapreduce.
    lib.aggregate.ValueAggregatorBaseDescriptor.UNIQ_VALUE_COUNT;

  static public final String LONG_VALUE_SUM = org.apache.hadoop.mapreduce.
    lib.aggregate.ValueAggregatorBaseDescriptor.LONG_VALUE_SUM;

  static public final String DOUBLE_VALUE_SUM = org.apache.hadoop.mapreduce.
    lib.aggregate.ValueAggregatorBaseDescriptor.DOUBLE_VALUE_SUM;

  static public final String VALUE_HISTOGRAM = org.apache.hadoop.mapreduce.
    lib.aggregate.ValueAggregatorBaseDescriptor.VALUE_HISTOGRAM;
  
  static public final String LONG_VALUE_MAX = org.apache.hadoop.mapreduce.
    lib.aggregate.ValueAggregatorBaseDescriptor.LONG_VALUE_MAX;
  
  static public final String LONG_VALUE_MIN = org.apache.hadoop.mapreduce.
    lib.aggregate.ValueAggregatorBaseDescriptor.LONG_VALUE_MIN;
  
  static public final String STRING_VALUE_MAX = org.apache.hadoop.mapreduce.
    lib.aggregate.ValueAggregatorBaseDescriptor.STRING_VALUE_MAX;
  
  static public final String STRING_VALUE_MIN = org.apache.hadoop.mapreduce.
    lib.aggregate.ValueAggregatorBaseDescriptor.STRING_VALUE_MIN;

  // 唯一值统计的最大允许数量，默认无上限
  private static long maxNumItems = Long.MAX_VALUE; 
  
  /**
   * 生成带聚合类型前缀的聚合键值对，用于Map输出阶段标识不同聚合任务
   * @param type 聚合类型
   * @param id 聚合ID
   * @param val 需要聚合的值
   * @return 键为聚合ID加聚合类型前缀的键值对Entry
   */
  public static Entry<Text, Text> generateEntry(String type, String id, Text val) {
    return org.apache.hadoop.mapreduce.lib.aggregate.
      ValueAggregatorBaseDescriptor.generateEntry(type, id, val);
  }

  /**
   * 根据给定聚合类型创建对应的聚合器实例
   * @param type 聚合类型
   * @return 对应类型的聚合器对象
   */
  static public ValueAggregator generateValueAggregator(String type) {
    ValueAggregator retv = null;
    if (type.compareToIgnoreCase(LONG_VALUE_SUM) == 0) {
      retv = new LongValueSum();
    } if (type.compareToIgnoreCase(LONG_VALUE_MAX) == 0) {
      retv = new LongValueMax();
    } else if (type.compareToIgnoreCase(LONG_VALUE_MIN) == 0) {
      retv = new LongValueMin();
    } else if (type.compareToIgnoreCase(STRING_VALUE_MAX) == 0) {
      retv = new StringValueMax();
    } else if (type.compareToIgnoreCase(STRING_VALUE_MIN) == 0) {
      retv = new StringValueMin();
    } else if (type.compareToIgnoreCase(DOUBLE_VALUE_SUM) == 0) {
      retv = new DoubleValueSum();
    } else if (type.compareToIgnoreCase(UNIQ_VALUE_COUNT) == 0) {
      retv = new UniqValueCount(maxNumItems);
    } else if (type.compareToIgnoreCase(VALUE_HISTOGRAM) == 0) {
      retv = new ValueHistogram();
    }
    return retv;
  }

  /**
   * 从作业配置中初始化参数，读取唯一值统计的最大数量限制
   * @param job 作业配置对象
   */
  public void configure(JobConf job) {
    super.configure(job);
    // 从配置获取唯一值统计的最大允许数量，默认无上限
    maxNumItems = job.getLong("aggregate.max.num.unique.values",
                              Long.MAX_VALUE);
  }
}