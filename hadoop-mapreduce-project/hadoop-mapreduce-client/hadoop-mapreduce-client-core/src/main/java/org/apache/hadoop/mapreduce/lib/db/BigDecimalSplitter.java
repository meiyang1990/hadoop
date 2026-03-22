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

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
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
 * 文件概要：基于BigDecimal数值类型的数据库分片器实现，为数据库并行输入处理生成分片
 * 功能：针对BigDecimal类型的分片列，将数据库表按数值范围切分成多个输入分片，供MapReduce任务并行读取
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BigDecimalSplitter implements DBSplitter {
  private static final Logger LOG =
      LoggerFactory.getLogger(BigDecimalSplitter.class);

  /**
   * 根据配置和数据库中分片列的最值，生成多个输入分片
   * @param conf 作业配置对象
   * @param results 包含分片列最小值和最大值的查询结果集
   * @param colName 用于分片的列名
   * @return 生成的输入分片列表
   * @throws SQLException 数据库查询异常
   */
  public List<InputSplit> split(Configuration conf, ResultSet results, String colName)
      throws SQLException {

    // 获取分片列最小值
    BigDecimal minVal = results.getBigDecimal(1);
    // 获取分片列最大值
    BigDecimal maxVal = results.getBigDecimal(2);

    String lowClausePrefix = colName + " >= ";
    String highClausePrefix = colName + " < ";

    // 获取配置的Map任务数量，作为分片数的基础
    BigDecimal numSplits = new BigDecimal(conf.getInt(MRJobConfig.NUM_MAPS, 1));

    if (minVal == null && maxVal == null) {
      // 所有值都为null，生成仅包含null条件的分片
      List<InputSplit> splits = new ArrayList<InputSplit>();
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
      return splits;
    }

    if (minVal == null || maxVal == null) {
      // 只有一端为null，无法确定合理范围，返回空
      LOG.error("Cannot find a range for NUMERIC or DECIMAL fields with one end NULL.");
      return null;
    }

    // 计算所有分片的分界点
    List<BigDecimal> splitPoints = split(numSplits, minVal, maxVal);
    List<InputSplit> splits = new ArrayList<InputSplit>();

    // 将分界点转换为分片区间
    BigDecimal start = splitPoints.get(0);
    for (int i = 1; i < splitPoints.size(); i++) {
      BigDecimal end = splitPoints.get(i);

      if (i == splitPoints.size() - 1) {
        // 最后一个分片，使用闭区间包含最大值
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            lowClausePrefix + start.toString(),
            colName + " <= " + end.toString()));
      } else {
        // 普通分片，左闭右开区间
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            lowClausePrefix + start.toString(),
            highClausePrefix + end.toString()));
      }

      start = end;
    }

    return splits;
  }

  // 分片最小步长，避免步长为0导致无限循环
  private static final BigDecimal MIN_INCREMENT = new BigDecimal(10000 * Double.MIN_VALUE);

  /**
   * 尝试对BigDecimal做除法，无法整除时使用四舍五入处理
   * @param numerator 被除数
   * @param denominator 除数
   * @return 除法结果
   */
  protected BigDecimal tryDivide(BigDecimal numerator, BigDecimal denominator) {
    try {
      return numerator.divide(denominator);
    } catch (ArithmeticException ae) {
      return numerator.divide(denominator, BigDecimal.ROUND_HALF_UP);
    }
  }

  /**
   * 根据分片数、最小值和最大值计算分片分界点
   * @param numSplits 期望分片数量
   * @param minVal 分片列最小值
   * @param maxVal 分片列最大值
   * @return 包含所有分界点的BigDecimal列表，长度为分片数+1
   * @throws SQLException 计算过程异常
   */
  List<BigDecimal> split(BigDecimal numSplits, BigDecimal minVal, BigDecimal maxVal)
      throws SQLException {

    List<BigDecimal> splits = new ArrayList<BigDecimal>();

    // 计算单个分片的步长
    BigDecimal splitSize = tryDivide(maxVal.subtract(minVal), (numSplits));
    if (splitSize.compareTo(MIN_INCREMENT) < 0) {
      // 如果步长太小，设置为最小允许步长，避免死循环
      splitSize = MIN_INCREMENT;
      LOG.warn("Set BigDecimal splitSize to MIN_INCREMENT");
    }

    // 从最小值开始生成分界点
    BigDecimal curVal = minVal;

    while (curVal.compareTo(maxVal) <= 0) {
      splits.add(curVal);
      curVal = curVal.add(splitSize);
    }

    if (splits.get(splits.size() - 1).compareTo(maxVal) != 0 || splits.size() == 1) {
      // 如果当前最后一个分界点不是最大值，补充最大值到分界点列表末尾
      splits.add(maxVal);
    }

    return splits;
  }
}