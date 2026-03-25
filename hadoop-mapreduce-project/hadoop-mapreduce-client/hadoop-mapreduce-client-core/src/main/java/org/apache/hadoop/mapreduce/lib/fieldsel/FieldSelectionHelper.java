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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;

/**
 * 文件级注释：字段选择工具类，为MapReduce作业提供类似Unix cut命令的字段选择功能，
 * 支持从输入文本中按指定分隔符拆分字段，分别选择输出键和输出值对应的字段集合。
 * 支持单个字段、范围字段和从指定位置开始的开放范围字段多种选择方式。
 *
 * This class implements a mapper/reducer class that can be used to perform
 * field selections in a manner similar to unix cut. The input data is treated
 * as fields separated by a user specified separator (the default value is
 * "\t"). The user can specify a list of fields that form the map output keys,
 * and a list of fields that form the map output values. If the inputformat is
 * TextInputFormat, the mapper will ignore the key to the map function. and the
 * fields are from the value only. Otherwise, the fields are the union of those
 * from the key and those from the value.
 * 
 * The field separator is under attribute "mapreduce.fieldsel.data.field.separator"
 * 
 * The map output field list spec is under attribute 
 * "mapreduce.fieldsel.map.output.key.value.fields.spec".
 * The value is expected to be like "keyFieldsSpec:valueFieldsSpec"
 * key/valueFieldsSpec are comma (,) separated field spec: fieldSpec,fieldSpec,fieldSpec ...
 * Each field spec can be a simple number (e.g. 5) specifying a specific field, or a range
 * (like 2-5) to specify a range of fields, or an open range (like 3-) specifying all 
 * the fields starting from field 3. The open range field spec applies value fields only.
 * They have no effect on the key fields.
 * 
 * Here is an example: "4,3,0,1:6,5,1-3,7-". It specifies to use fields 4,3,0 and 1 for keys,
 * and use fields 6,5,1,2,3,7 and above for values.
 * 
 * The reduce output field list spec is under attribute 
 * "mapreduce.fieldsel.reduce.output.key.value.fields.spec".
 * 
 * The reducer extracts output key/value pairs in a similar manner, except that
 * the key is never ignored.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FieldSelectionHelper {

  public static Text emptyText = new Text("");
  public static final String DATA_FIELD_SEPARATOR =
    "mapreduce.fieldsel.data.field.separator";
  /**
   * @deprecated Use {@link #DATA_FIELD_SEPARATOR}
   */
  @Deprecated
  public static final String DATA_FIELD_SEPERATOR = DATA_FIELD_SEPARATOR;
  public static final String MAP_OUTPUT_KEY_VALUE_SPEC = 
    "mapreduce.fieldsel.map.output.key.value.fields.spec";
  public static final String REDUCE_OUTPUT_KEY_VALUE_SPEC = 
    "mapreduce.fieldsel.reduce.output.key.value.fields.spec";


  /**
   * 从字段规格数组中解析提取出需要选择的字段编号，同时识别开放范围规格（n-）并返回起始位置
   * @param fieldListSpec 字段规格数组
   * @param fieldList 存储解析出的字段编号列表
   * @return 如果存在开放范围规格则返回起始位置n，否则返回-1
   */
  private static int extractFields(String[] fieldListSpec,
      List<Integer> fieldList) {
    int allFieldsFrom = -1;
    int i = 0;
    int j = 0;
    int pos = -1;
    String fieldSpec = null;
    for (i = 0; i < fieldListSpec.length; i++) {
      fieldSpec = fieldListSpec[i];
      // 跳过空规格
      if (fieldSpec.length() == 0) {
        continue;
      }
      // 查找连字符，判断是否为范围规格
      pos = fieldSpec.indexOf('-');
      // 单个字段规格，直接添加字段编号
      if (pos < 0) {
        Integer fn = Integer.valueOf(fieldSpec);
        fieldList.add(fn);
      } else {
        // 拆分范围起始和结束
        String start = fieldSpec.substring(0, pos);
        String end = fieldSpec.substring(pos + 1);
        // 起始为空默认从0开始
        if (start.length() == 0) {
          start = "0";
        }
        // 结束为空说明是开放范围，记录起始位置
        if (end.length() == 0) {
          allFieldsFrom = Integer.parseInt(start);
          continue;
        }
        // 解析闭范围的起止位置，添加范围内所有字段编号
        int startPos = Integer.parseInt(start);
        int endPos = Integer.parseInt(end);
        for (j = startPos; j <= endPos; j++) {
          fieldList.add(j);
        }
      }
    }
    return allFieldsFrom;
  }

  /**
   * 根据选择的字段编号和开放范围，从已拆分的字段数组中拼接生成结果字符串
   * @param fields 按分隔符拆分后的所有字段数组
   * @param fieldList 解析得到的需要选择的字段编号列表
   * @param allFieldsFrom 开放范围的起始位置，小于0表示无开放范围
   * @param separator 结果字符串中字段之间使用的分隔符
   * @return 拼接完成的结果字符串
   */
  private static String selectFields(String[] fields, List<Integer> fieldList,
      int allFieldsFrom, String separator) {
    String retv = null;
    int i = 0;
    StringBuilder sb = null;
    // 处理固定选择的字段列表
    if (fieldList != null && fieldList.size() > 0) {
      if (sb == null) {
        sb = new StringBuilder();
      }
      for (Integer index : fieldList) {
        if (index < fields.length) {
          sb.append(fields[index]);
        }
        sb.append(separator);
      }
    }
    // 处理开放范围的字段（从allFieldsFrom开始到末尾所有字段）
    if (allFieldsFrom >= 0) {
      if (sb == null) {
        sb = new StringBuilder();
      }
      for (i = allFieldsFrom; i < fields.length; i++) {
        sb.append(fields[i]).append(separator);
      }
    }
    // 移除末尾多余的分隔符
    if (sb != null) {
      retv = sb.toString();
      if (retv.length() > 0) {
        retv = retv.substring(0, retv.length() - 1);
      }
    }
    return retv;
  }
  
  /**
   * 解析键值字段规格字符串，拆分出键字段规格和值字段规格，分别提取字段编号
   * @param keyValueSpec 完整的键值规格字符串，格式为"键规格:值规格"
   * @param keyFieldList 存储解析后的键字段编号列表
   * @param valueFieldList 存储解析后的值字段编号列表
   * @return 值部分的开放范围起始位置，如果没有则返回-1
   */
  public static int parseOutputKeyValueSpec(String keyValueSpec,
      List<Integer> keyFieldList, List<Integer> valueFieldList) {
    // 按冒号拆分键和值部分规格，保留空字符串
    String[] keyValSpecs = keyValueSpec.split(":", -1);
    
    // 按逗号拆分键部分规格数组
    String[] keySpec = keyValSpecs[0].split(",");
    
    // 解析值部分规格
    String[] valSpec = new String[0];
    if (keyValSpecs.length > 1) {
      valSpec = keyValSpecs[1].split(",");
    }

    FieldSelectionHelper.extractFields(keySpec, keyFieldList);
    return FieldSelectionHelper.extractFields(valSpec, valueFieldList);
  }

  /**
   * 将当前字段选择配置转换为调试用的字符串，方便日志输出和问题排查
   * @param fieldSeparator 字段分隔符
   * @param keyValueSpec 原始键值规格字符串
   * @param allValueFieldsFrom 值开放范围起始位置
   * @param keyFieldList 键字段编号列表
   * @param valueFieldList 值字段编号列表
   * @return 格式化的调试字符串
   */
  public static String specToString(String fieldSeparator, String keyValueSpec,
      int allValueFieldsFrom, List<Integer> keyFieldList,
      List<Integer> valueFieldList) {
    StringBuilder sb = new StringBuilder();
    sb.append("fieldSeparator: ").append(fieldSeparator).append("\n");

    sb.append("keyValueSpec: ").append(keyValueSpec).append("\n");
    sb.append("allValueFieldsFrom: ").append(allValueFieldsFrom);
    sb.append("\n");
    sb.append("keyFieldList.length: ").append(keyFieldList.size());
    sb.append("\n");
    for (Integer field : keyFieldList) {
      sb.append("\t").append(field).append("\n");
    }
    sb.append("valueFieldList.length: ").append(valueFieldList.size());
    sb.append("\n");
    for (Integer field : valueFieldList) {
      sb.append("\t").append(field).append("\n");
    }
    return sb.toString();
  }

  private Text key = null;
  private Text value = null;
  
  /**
   * 空构造函数
   */
  public FieldSelectionHelper() {
  }

  /**
   * 带初始键值的构造函数
   * @param key 初始输出键
   * @param val 初始输出值
   */
  public FieldSelectionHelper(Text key, Text val) {
    this.key = key;
    this.value = val;
  }
  
  /**
   * 获取选择后的输出键
   * @return 选择后的输出键Text对象
   */
  public Text getKey() {
    return key;
  }
 
  /**
   * 获取选择后的输出值
   * @return 选择后的输出值Text对象
   */
  public Text getValue() {
    return value;
  }

  /**
   * 根据字段选择配置从输入键值中提取生成新的输出键和输出值
   * @param key 输入键字符串
   * @param val 输入值字符串
   * @param fieldSep 字段分隔符
   * @param keyFieldList 需要选择的键字段编号列表
   * @param valFieldList 需要选择的值字段编号列表
   * @param allValueFieldsFrom 值开放范围起始位置
   * @param ignoreKey 是否忽略输入键（TextInputFormat场景下忽略行偏移键）
   * @param isMap 是否是Map阶段处理
   */
  public void extractOutputKeyValue(String key, String val,
      String fieldSep, List<Integer> keyFieldList, List<Integer> valFieldList,
      int allValueFieldsFrom, boolean ignoreKey, boolean isMap) {
    // 不忽略输入键则将输入键拼接到值前面一起拆分
    if (!ignoreKey) {
      val = key + val;
    }
    // 按分隔符拆分所有字段
    String[] fields = val.split(fieldSep);
    
    // 选择生成新键，键部分不支持开放范围
    String newKey = selectFields(fields, keyFieldList, -1, fieldSep);
    // 选择生成新值，值部分支持开放范围
    String newVal = selectFields(fields, valFieldList, allValueFieldsFrom,
      fieldSep);
    // Map阶段如果键为空，将值作为键，值设为空
    if (isMap && newKey == null) {
      newKey = newVal;
      newVal = null;
    }
    
    // 更新实例中存储的输出键和输出值
    if (newKey != null) {
      this.key = new Text(newKey);
    }
    if (newVal != null) {
      this.value = new Text(newVal);
    }
  }
}