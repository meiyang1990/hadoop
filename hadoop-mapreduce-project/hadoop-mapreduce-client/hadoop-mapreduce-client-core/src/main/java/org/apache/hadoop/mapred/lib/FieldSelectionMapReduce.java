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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.fieldsel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 实现类似Unix cut命令的字段选择功能，同时实现了Mapper和Reducer接口，可以在Map阶段和Reduce阶段完成字段抽取
 * 
 * 输入数据按用户指定分隔符切分为多个字段，用户可分别指定哪些字段作为输出key、哪些作为输出value
 * 如果输入格式是TextInputFormat，Mapper会忽略输入key，仅从输入value中提取字段；否则会合并key和value的所有字段
 * 
 * 分隔符配置项：mapreduce.fieldsel.data.field.separator
 * Map输出字段规则配置项：mapreduce.fieldsel.map.output.key.value.fields.spec
 * 格式为 "keyFieldsSpec:valueFieldsSpec"，key/valueFieldsSpec是逗号分隔的字段定义：
 * 每个字段定义可以是单个字段编号（如5）、字段范围（如2-5）、开放范围（如3-，表示从3开始的所有字段）
 * 开放范围仅对value字段生效，对key字段无效
 * 
 * 示例："4,3,0,1:6,5,1-3,7-" 表示key使用4、3、0、1号字段，value使用6、5、1、2、3、7及之后的所有字段
 * 
 * Reduce输出字段规则配置项：mapreduce.fieldsel.reduce.output.key.value.fields.spec
 * Reduce阶段提取输出key/value规则类似，但不会忽略输入key
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FieldSelectionMapReduce<K, V>
    implements Mapper<K, V, Text, Text>, Reducer<Text, Text, Text, Text> {

  private String mapOutputKeyValueSpec;

  private boolean ignoreInputKey;

  private String fieldSeparator = "\t";

  private List<Integer> mapOutputKeyFieldList = new ArrayList<Integer>();

  private List<Integer> mapOutputValueFieldList = new ArrayList<Integer>();

  private int allMapValueFieldsFrom = -1;

  private String reduceOutputKeyValueSpec;

  private List<Integer> reduceOutputKeyFieldList = new ArrayList<Integer>();

  private List<Integer> reduceOutputValueFieldList = new ArrayList<Integer>();

  private int allReduceValueFieldsFrom = -1;


  public static final Logger LOG =
      LoggerFactory.getLogger("FieldSelectionMapReduce");

  /**
   * 将当前字段选择配置转换为字符串，用于日志输出
   * @return 配置详情字符串
   */
  private String specToString() {
    StringBuilder sb = new StringBuilder();
    sb.append("fieldSeparator: ").append(fieldSeparator).append("\n");

    sb.append("mapOutputKeyValueSpec: ").append(mapOutputKeyValueSpec).append(
        "\n");
    sb.append("reduceOutputKeyValueSpec: ").append(reduceOutputKeyValueSpec)
        .append("\n");

    sb.append("allMapValueFieldsFrom: ").append(allMapValueFieldsFrom).append(
        "\n");

    sb.append("allReduceValueFieldsFrom: ").append(allReduceValueFieldsFrom)
        .append("\n");

    int i = 0;

    sb.append("mapOutputKeyFieldList.length: ").append(
        mapOutputKeyFieldList.size()).append("\n");
    for (i = 0; i < mapOutputKeyFieldList.size(); i++) {
      sb.append("\t").append(mapOutputKeyFieldList.get(i)).append("\n");
    }
    sb.append("mapOutputValueFieldList.length: ").append(
        mapOutputValueFieldList.size()).append("\n");
    for (i = 0; i < mapOutputValueFieldList.size(); i++) {
      sb.append("\t").append(mapOutputValueFieldList.get(i)).append("\n");
    }

    sb.append("reduceOutputKeyFieldList.length: ").append(
        reduceOutputKeyFieldList.size()).append("\n");
    for (i = 0; i < reduceOutputKeyFieldList.size(); i++) {
      sb.append("\t").append(reduceOutputKeyFieldList.get(i)).append("\n");
    }
    sb.append("reduceOutputValueFieldList.length: ").append(
        reduceOutputValueFieldList.size()).append("\n");
    for (i = 0; i < reduceOutputValueFieldList.size(); i++) {
      sb.append("\t").append(reduceOutputValueFieldList.get(i)).append("\n");
    }
    return sb.toString();
  }

  /**
   * Map阶段字段选择处理，根据配置从输入中抽取指定字段输出为key和value
   */
  public void map(K key, V val,
      OutputCollector<Text, Text> output, Reporter reporter) 
      throws IOException {
    // 创建字段选择助手，初始化为空文本
    FieldSelectionHelper helper = new FieldSelectionHelper(
      FieldSelectionHelper.emptyText, FieldSelectionHelper.emptyText);
    // 根据配置抽取key和value字段
    helper.extractOutputKeyValue(key.toString(), val.toString(),
      fieldSeparator, mapOutputKeyFieldList, mapOutputValueFieldList,
      allMapValueFieldsFrom, ignoreInputKey, true);
    // 输出抽取结果
    output.collect(helper.getKey(), helper.getValue());
  }

  /**
   * 解析Map和Reduce的输出字段规则，提取出选中的字段编号列表和开放范围起始位置
   */
  private void parseOutputKeyValueSpec() {
    // 解析Map输出字段规则，获取value开放范围起始位置
    allMapValueFieldsFrom = FieldSelectionHelper.parseOutputKeyValueSpec(
      mapOutputKeyValueSpec, mapOutputKeyFieldList, mapOutputValueFieldList);
    
    // 解析Reduce输出字段规则，获取value开放范围起始位置
    allReduceValueFieldsFrom = FieldSelectionHelper.parseOutputKeyValueSpec(
      reduceOutputKeyValueSpec, reduceOutputKeyFieldList,
      reduceOutputValueFieldList);
  }

  /**
   * 初始化配置，从JobConf中读取字段选择相关配置并解析
   * @param job 作业配置对象
   */
  public void configure(JobConf job) {
    // 读取字段分隔符，默认使用制表符
    this.fieldSeparator = job.get(FieldSelectionHelper.DATA_FIELD_SEPARATOR,
        "\t");
    // 读取Map输出字段规则，默认所有字段都作为key输出，value为空
    this.mapOutputKeyValueSpec = job.get(
        FieldSelectionHelper.MAP_OUTPUT_KEY_VALUE_SPEC, "0-:");
    // 如果输入格式是TextInputFormat，则忽略输入key
    this.ignoreInputKey = TextInputFormat.class.getCanonicalName().equals(
        job.getInputFormat().getClass().getCanonicalName());
    // 读取Reduce输出字段规则，默认所有字段都作为key输出，value为空
    this.reduceOutputKeyValueSpec = job.get(
        FieldSelectionHelper.REDUCE_OUTPUT_KEY_VALUE_SPEC, "0-:");
    // 解析Map和Reduce输出字段规则
    parseOutputKeyValueSpec();
    // 打印配置信息到日志
    LOG.info(specToString());
  }

  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  /**
   * Reduce阶段字段选择处理，对每个输入value抽取指定字段输出
   */
  public void reduce(Text key, Iterator<Text> values,
                     OutputCollector<Text, Text> output, Reporter reporter)
    throws IOException {
    // 将输入key加上分隔符，拼接到value字段前一起提取
    String keyStr = key.toString() + this.fieldSeparator;
    // 遍历所有输入value
    while (values.hasNext()) {
        // 创建字段选择助手
        FieldSelectionHelper helper = new FieldSelectionHelper();
        // 根据配置从key和输入value中抽取输出字段
        helper.extractOutputKeyValue(keyStr, values.next().toString(),
          fieldSeparator, reduceOutputKeyFieldList,
          reduceOutputValueFieldList, allReduceValueFieldsFrom, false, false);
        // 输出抽取结果
      output.collect(helper.getKey(), helper.getValue());
    }
  }
}