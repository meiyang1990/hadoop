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

package org.apache.hadoop.mapreduce.lib.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 * 文件级注释：文本类型数据库字段分片实现类，属于MapReduce数据库输入组件，用于对文本类型分区分割键生成并行输入分片
 *
 * 实现基于文本字符串的数据库分片分割器，将文本范围均匀拆分为多个分片供MapReduce并行读取
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TextSplitter extends BigDecimalSplitter {

  private static final Logger LOG = LoggerFactory.getLogger(TextSplitter.class);

  /**
   * 对文本类型的分区列范围生成多个输入分片，核心算法是将文本转换为BigDecimal后使用父类均匀分片，再转换回文本
   * 算法说明：将每个字符视为65536进制的小数位，转换为0~1区间的浮点数进行均分，再转换回字符串
   * @param conf 作业配置对象
   * @param results 包含分区列最小值和最大值的结果集
   * @param colName 分区列名
   * @return 生成的输入分片列表
   * @throws SQLException 数据库访问异常
   */
  public List<InputSplit> split(Configuration conf, ResultSet results, String colName)
      throws SQLException {

    LOG.warn("Generating splits for a textual index column.");
    LOG.warn("If your database sorts in a case-insensitive order, "
        + "this may result in a partial import or duplicate records.");
    LOG.warn("You are strongly encouraged to choose an integral split column.");

    String minString = results.getString(1);
    String maxString = results.getString(2);

    boolean minIsNull = false;

    // 处理最小值为null的情况，替换为空字符串，后续单独添加null分片
    if (null == minString) {
      minString = "";
      minIsNull = true;
    }

    // 处理最大值为null的情况，此时所有值都是null，返回单个null分片
    if (null == maxString) {
      List<InputSplit> splits = new ArrayList<InputSplit>();
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
      return splits;
    }

    // 获取配置的Map任务数，作为分片数量参考
    int numSplits = conf.getInt(MRJobConfig.NUM_MAPS, 1);

    String lowClausePrefix = colName + " >= '";
    String highClausePrefix = colName + " < '";

    // 计算最大公共前缀长度，减少后续处理长度
    int maxPrefixLen = Math.min(minString.length(), maxString.length());
    int sharedLen;
    for (sharedLen = 0; sharedLen < maxPrefixLen; sharedLen++) {
      char c1 = minString.charAt(sharedLen);
      char c2 = maxString.charAt(sharedLen);
      if (c1 != c2) {
        break;
      }
    }

    // 提取公共前缀，从min和max中移除公共部分，只处理差异部分
    String commonPrefix = minString.substring(0, sharedLen);
    minString = minString.substring(sharedLen);
    maxString = maxString.substring(sharedLen);

    // 对差异部分生成分割点字符串列表
    List<String> splitStrings = split(numSplits, minString, maxString, commonPrefix);
    List<InputSplit> splits = new ArrayList<InputSplit>();

    // 将分割点转换为实际输入分片，添加SQL条件
    String start = splitStrings.get(0);
    for (int i = 1; i < splitStrings.size(); i++) {
      String end = splitStrings.get(i);

      if (i == splitStrings.size() - 1) {
        // 最后一个分片使用闭区间，包含最大值
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            lowClausePrefix + start + "'", colName + " <= '" + end + "'"));
      } else {
        // 普通分片使用左闭右开区间
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            lowClausePrefix + start + "'", highClausePrefix + end + "'"));
      }
    }

    if (minIsNull) {
      // 单独添加null值对应的分片
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
    }

    return splits;
  }

  /**
   * 根据指定分片数量和字符串范围，生成包含公共前缀的分割点字符串列表
   * @param numSplits 期望分片数量
   * @param minString 最小值去掉公共前缀后的字符串
   * @param maxString 最大值去掉公共前缀后的字符串
   * @param commonPrefix 公共前缀字符串
   * @return 排序后的分割点字符串列表
   * @throws SQLException 转换过程异常
   */
  List<String> split(int numSplits, String minString, String maxString, String commonPrefix)
      throws SQLException {

    BigDecimal minVal = stringToBigDecimal(minString);
    BigDecimal maxVal = stringToBigDecimal(maxString);

    // 调用父类BigDecimalSplitter生成均匀分割点
    List<BigDecimal> splitPoints = split(new BigDecimal(numSplits), minVal, maxVal);
    List<String> splitStrings = new ArrayList<String>();

    // 将BigDecimal分割点转换回带公共前缀的字符串
    for (BigDecimal bd : splitPoints) {
      splitStrings.add(commonPrefix + bigDecimalToString(bd));
    }

    // 确保用户指定的边界值一定在分割点列表首尾，避免边界溢出
    if (splitStrings.size() == 0 || !splitStrings.get(0).equals(commonPrefix + minString)) {
      splitStrings.add(0, commonPrefix + minString);
    }
    if (splitStrings.size() == 1
        || !splitStrings.get(splitStrings.size() - 1).equals(commonPrefix + maxString)) {
      splitStrings.add(commonPrefix + maxString);
    }

    return splitStrings;
  }

  private final static BigDecimal ONE_PLACE = new BigDecimal(65536);

  // 最大转换字符数，限制长度避免精度误差和无限循环，仍可支持足够大的范围
  private final static int MAX_CHARS = 8;

  /**
   * 将输入字符串转换为符合排序顺序的BigDecimal表示
   * 把每个字符作为65536进制小数位，转换为0~1区间的BigDecimal
   * @param str 输入字符串
   * @return 转换后的BigDecimal
   */
  BigDecimal stringToBigDecimal(String str) {
    BigDecimal result = BigDecimal.ZERO;
    BigDecimal curPlace = ONE_PLACE; // 初始权重为1/65536，对应第一位小数

    int len = Math.min(str.length(), MAX_CHARS);

    for (int i = 0; i < len; i++) {
      int codePoint = str.codePointAt(i);
      result = result.add(tryDivide(new BigDecimal(codePoint), curPlace));
      // 权重乘以65536，对应下一个更低有效位
      curPlace = curPlace.multiply(ONE_PLACE);
    }

    return result;
  }

  /**
   * 将BigDecimal转换回原字符串表示
   * 重复乘以65536取出整数部分得到字符编码，直到没有更多有效数据
   * @param bd 待转换的BigDecimal值
   * @return 转换后的字符串
   */
  String bigDecimalToString(BigDecimal bd) {
    BigDecimal cur = bd.stripTrailingZeros();
    StringBuilder sb = new StringBuilder();

    for (int numConverted = 0; numConverted < MAX_CHARS; numConverted++) {
      cur = cur.multiply(ONE_PLACE);
      int curCodePoint = cur.intValue();
      if (0 == curCodePoint) {
        break;
      }

      cur = cur.subtract(new BigDecimal(curCodePoint));
      sb.append(Character.toChars(curCodePoint));
    }

    return sb.toString();
  }
}