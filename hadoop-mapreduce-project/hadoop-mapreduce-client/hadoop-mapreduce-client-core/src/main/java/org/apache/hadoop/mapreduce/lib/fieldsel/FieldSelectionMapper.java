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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 实现类似Unix cut命令的字段选择Mapper，用于从输入数据中按规则提取指定字段作为Map输出的键和值。
 * <p>
 * 核心功能：将输入文本按指定分隔符切分为多个字段，根据用户配置选择部分字段作为输出键、部分作为输出值。
 * <ul>
 * <li>若输入格式为{@link TextInputFormat}，则仅从输入值中提取字段，忽略输入键；否则从输入键和输入值中共同提取字段</li>
 * <li>配置参数{@value FieldSelectionHelper#DATA_FIELD_SEPARATOR}指定字段分隔符，默认为制表符"\t"</li>
 * <li>配置参数{@value FieldSelectionHelper#MAP_OUTPUT_KEY_VALUE_SPEC}指定输出键值字段选择规则，格式为"keyFieldsSpec:valueFieldsSpec"</li>
 * <li>字段规则支持：单个字段编号、范围(如2-5)、开区间范围(如3-，仅对值字段生效，代表从3开始的所有字段)</li>
 * </ul>
 * 示例规则："4,3,0,1:6,5,1-3,7-" 表示使用4、3、0、1号字段作为输出键，使用6、5、1、2、3、7及之后字段作为输出值
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FieldSelectionMapper<K, V>
    extends Mapper<K, V, Text, Text> {

  private String mapOutputKeyValueSpec;

  private boolean ignoreInputKey;

  private String fieldSeparator = "\t";

  private List<Integer> mapOutputKeyFieldList = new ArrayList<Integer>();

  private List<Integer> mapOutputValueFieldList = new ArrayList<Integer>();

  private int allMapValueFieldsFrom = -1;

  public static final Logger LOG =
      LoggerFactory.getLogger("FieldSelectionMapReduce");

  /**
   * 初始化Mapper，从配置加载字段分隔符和选择规则，解析规则并判断是否忽略输入键
   * @param context Mapper运行上下文
   * @throws IOException 加载配置或获取输入格式类失败时抛出
   * @throws InterruptedException 线程中断时抛出
   */
  public void setup(Context context) 
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    // 从配置获取字段分隔符，默认使用制表符
    this.fieldSeparator = 
      conf.get(FieldSelectionHelper.DATA_FIELD_SEPARATOR, "\t");
    // 从配置获取输出键值字段规则，默认规则为"0-:"
    this.mapOutputKeyValueSpec = 
      conf.get(FieldSelectionHelper.MAP_OUTPUT_KEY_VALUE_SPEC, "0-:");
    // 判断输入格式是否为TextInputFormat，如果是则忽略输入键，仅从值提取字段
    try {
      this.ignoreInputKey = TextInputFormat.class.getCanonicalName().equals(
        context.getInputFormatClass().getCanonicalName());
    } catch (ClassNotFoundException e) {
      throw new IOException("Input format class not found", e);
    }
    // 解析输出键值规则，得到输出键字段列表、输出值字段列表，以及值字段开区间起始位置
    allMapValueFieldsFrom = FieldSelectionHelper.parseOutputKeyValueSpec(
      mapOutputKeyValueSpec, mapOutputKeyFieldList, mapOutputValueFieldList);
    // 打印解析后的配置信息日志
    LOG.info(FieldSelectionHelper.specToString(fieldSeparator,
      mapOutputKeyValueSpec, allMapValueFieldsFrom, mapOutputKeyFieldList,
      mapOutputValueFieldList) + "\nignoreInputKey:" + ignoreInputKey);
  }

  /**
   * 对每个输入键值对执行字段提取，按规则生成输出键值并写出
   * @param key 输入键
   * @param val 输入值
   * @param context Mapper运行上下文
   * @throws IOException 写出数据失败时抛出
   * @throws InterruptedException 线程中断时抛出
   */
  public void map(K key, V val, Context context) 
      throws IOException, InterruptedException {
    // 创建字段提取工具类实例
    FieldSelectionHelper helper = new FieldSelectionHelper(
      FieldSelectionHelper.emptyText, FieldSelectionHelper.emptyText);
    // 根据规则提取输出键和值
    helper.extractOutputKeyValue(key.toString(), val.toString(),
      fieldSeparator, mapOutputKeyFieldList, mapOutputValueFieldList,
      allMapValueFieldsFrom, ignoreInputKey, true);
    // 写出提取后的键值对
    context.write(helper.getKey(), helper.getValue());
  }
}