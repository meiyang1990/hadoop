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

package org.apache.hadoop.mapreduce.lib.fieldsel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 实现类似Unix cut命令的字段选择功能的Reducer类。
 * <p>
 * 输入数据会按用户指定分隔符切分成为多个字段，默认分隔符为"\t"。
 * 用户可分别指定输出key和输出value需要保留的字段列表，所有字段来自输入key和输入value的合并。
 * <p>
 * 配置参数说明：
 * <ul>
 * <li>mapreduce.fieldsel.data.field.separator：字段分隔符</li>
 * <li>mapreduce.fieldsel.reduce.output.key.value.fields.spec：输出字段配置，格式为keyFieldsSpec:valueFieldsSpec。
 * keyFieldsSpec和valueFieldsSpec都是逗号分隔的字段规则，支持：单个字段编号、范围（如2-5）、开区间范围（如3-表示从3开始到末尾，仅对value有效）</li>
 * </ul>
 * 示例配置："4,3,0,1:6,5,1-3,7-" 表示key使用4、3、0、1字段，value使用6、5、1、2、3、7及之后的所有字段。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FieldSelectionReducer<K, V>
    extends Reducer<Text, Text, Text, Text> {

  private String fieldSeparator = "\t";

  private String reduceOutputKeyValueSpec;

  private List<Integer> reduceOutputKeyFieldList = new ArrayList<Integer>();

  private List<Integer> reduceOutputValueFieldList = new ArrayList<Integer>();

  private int allReduceValueFieldsFrom = -1;

  public static final Logger LOG =
      LoggerFactory.getLogger("FieldSelectionMapReduce");

  /**
   * 初始化方法，从配置中读取字段分隔符和输出字段规则，解析并保存字段选择列表。
   * @param context Reducer上下文对象
   * @throws IOException 如果IO操作失败抛出
   * @throws InterruptedException 如果线程被中断抛出
   */
  public void setup(Context context) 
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    
    // 从配置获取字段分隔符，默认使用\t
    this.fieldSeparator = 
      conf.get(FieldSelectionHelper.DATA_FIELD_SEPARATOR, "\t");
    
    // 从配置获取reduce输出key/value字段规则，默认规则为0-:
    this.reduceOutputKeyValueSpec = 
      conf.get(FieldSelectionHelper.REDUCE_OUTPUT_KEY_VALUE_SPEC, "0-:");
    
    // 解析输出字段规则，得到key、value字段列表和value开区间起始位置
    allReduceValueFieldsFrom = FieldSelectionHelper.parseOutputKeyValueSpec(
      reduceOutputKeyValueSpec, reduceOutputKeyFieldList,
      reduceOutputValueFieldList);

    // 输出解析后的配置信息到日志
    LOG.info(FieldSelectionHelper.specToString(fieldSeparator,
      reduceOutputKeyValueSpec, allReduceValueFieldsFrom,
      reduceOutputKeyFieldList, reduceOutputValueFieldList));
  }

  /**
   * Reduce核心方法，对每个key对应的所有value执行字段选择，输出选择后的key和value。
   * @param key 输入key
   * @param values 输入value迭代器
   * @param context Reducer上下文对象
   * @throws IOException 如果IO操作失败抛出
   * @throws InterruptedException 如果线程被中断抛出
   */
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    // 将输入key拼接分隔符，便于后续统一切分字段
    String keyStr = key.toString() + this.fieldSeparator;
    
    // 遍历当前key对应的所有value
    for (Text val : values) {
      // 创建字段选择工具类实例
      FieldSelectionHelper helper = new FieldSelectionHelper();
      // 根据规则提取需要输出的key和value字段
      helper.extractOutputKeyValue(keyStr, val.toString(),
        fieldSeparator, reduceOutputKeyFieldList,
        reduceOutputValueFieldList, allReduceValueFieldsFrom, false, false);
      // 写出结果
      context.write(helper.getKey(), helper.getValue());
    }
  }
}