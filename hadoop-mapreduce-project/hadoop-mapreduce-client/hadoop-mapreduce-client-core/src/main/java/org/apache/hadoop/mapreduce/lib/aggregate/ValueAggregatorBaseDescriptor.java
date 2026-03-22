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
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;

/** 
 * ValueAggregatorDescriptor的基础实现类，为所有子类提供公共基础功能
 * 是MapReduce聚合框架中描述聚合器的基础抽象基类
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ValueAggregatorBaseDescriptor 
    implements ValueAggregatorDescriptor {

  /** 唯一值计数聚合类型 */
  static public final String UNIQ_VALUE_COUNT = "UniqValueCount";

  /** 长整型求和聚合类型 */
  static public final String LONG_VALUE_SUM = "LongValueSum";

  /** 双精度浮点型求和聚合类型 */
  static public final String DOUBLE_VALUE_SUM = "DoubleValueSum";

  /** 值直方图聚合类型 */
  static public final String VALUE_HISTOGRAM = "ValueHistogram";
  
  /** 长整型最大值聚合类型 */
  static public final String LONG_VALUE_MAX = "LongValueMax";
  
  /** 长整型最小值聚合类型 */
  static public final String LONG_VALUE_MIN = "LongValueMin";
  
  /** 字符串最大值聚合类型（按字典序） */
  static public final String STRING_VALUE_MAX = "StringValueMax";
  
  /** 字符串最小值聚合类型（按字典序） */
  static public final String STRING_VALUE_MIN = "StringValueMin";
  
  /** 当前处理的输入文件路径 */
  public String inputFile = null;

  /**
   * 实现Entry接口的私有内部类，用于封装聚合后的键值对
   */
  private static class MyEntry implements Entry<Text, Text> {
    Text key;

    Text val;

    public Text getKey() {
      return key;
    }

    public Text getValue() {
      return val;
    }

    public Text setValue(Text val) {
      this.val = val;
      return val;
    }

    public MyEntry(Text key, Text val) {
      this.key = key;
      this.val = val;
    }
  }

  /**
   * 根据聚合类型、ID和值生成聚合框架使用的键值对Entry
   * 键由聚合类型和聚合ID拼接而成，用于后续reduce阶段选择正确的聚合器
   * @param type 聚合类型
   * @param id 聚合ID
   * @param val 待聚合的值
   * @return 封装好的聚合键值对Entry
   */
  public static Entry<Text, Text> generateEntry(String type, 
      String id, Text val) {
    Text key = new Text(type + TYPE_SEPARATOR + id);
    return new MyEntry(key, val);
  }

  /**
   * 根据指定聚合类型创建对应的ValueAggregator实例
   * @param type 聚合类型
   * @param uniqCount UNIQ_VALUE_COUNT类型下的唯一值保留上限，其他类型无用
   * @return 创建好的聚合器实例，类型不匹配时返回null
   */
  static public ValueAggregator generateValueAggregator(String type, long uniqCount) {
    if (type.compareToIgnoreCase(LONG_VALUE_SUM) == 0) {
      return new LongValueSum();
    } if (type.compareToIgnoreCase(LONG_VALUE_MAX) == 0) {
      return new LongValueMax();
    } else if (type.compareToIgnoreCase(LONG_VALUE_MIN) == 0) {
      return new LongValueMin();
    } else if (type.compareToIgnoreCase(STRING_VALUE_MAX) == 0) {
      return new StringValueMax();
    } else if (type.compareToIgnoreCase(STRING_VALUE_MIN) == 0) {
      return new StringValueMin();
    } else if (type.compareToIgnoreCase(DOUBLE_VALUE_SUM) == 0) {
      return new DoubleValueSum();
    } else if (type.compareToIgnoreCase(UNIQ_VALUE_COUNT) == 0) {
      return new UniqValueCount(uniqCount);
    } else if (type.compareToIgnoreCase(VALUE_HISTOGRAM) == 0) {
      return new ValueHistogram();
    }
    return null;
  }

  /**
   * 为输入键值对生成对应的聚合键值对列表
   * 用于计数场景：默认会生成全局记录数计数；如果存在输入文件信息，额外生成按文件的记录数计数
   * @param key 输入键
   * @param val 输入值
   * @return 生成的聚合键值对列表，用于后续reduce聚合
   */
  public ArrayList<Entry<Text, Text>> generateKeyValPairs(Object key,
                                                          Object val) {
    ArrayList<Entry<Text, Text>> retv = new ArrayList<Entry<Text, Text>>();
    String countType = LONG_VALUE_SUM;
    String id = "record_count";
    Entry<Text, Text> e = generateEntry(countType, id, ONE);
    if (e != null) {
      retv.add(e);
    }
    if (this.inputFile != null) {
      e = generateEntry(countType, this.inputFile, ONE);
      if (e != null) {
        retv.add(e);
      }
    }
    return retv;
  }

  /**
   * 从配置中读取当前Map任务处理的输入文件路径，初始化实例
   * @param conf 作业配置对象
   */
  public void configure(Configuration conf) {
    this.inputFile = conf.get(MRJobConfig.MAP_INPUT_FILE);
  }
}