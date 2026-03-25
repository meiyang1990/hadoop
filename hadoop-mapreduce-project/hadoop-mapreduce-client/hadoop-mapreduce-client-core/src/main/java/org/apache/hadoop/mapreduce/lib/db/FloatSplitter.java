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
 * 文件说明：基于浮点型数值的数据库分片实现类，用于对数据库中浮点类型拆分列进行分块，生成多个Map输入分片
 * 实现DBSplitter接口，支持对浮点型索引列进行分片分割
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FloatSplitter implements DBSplitter {

  private static final Logger LOG =
      LoggerFactory.getLogger(FloatSplitter.class);

  // 允许的最小分片步长，避免步长过小导致精度问题
  private static final double MIN_INCREMENT = 10000 * Double.MIN_VALUE;

  /**
   * 对数据库中的浮点型拆分列进行分片，生成多个输入分片
   * @param conf 作业配置对象，包含Map任务数量等配置
   * @param results 包含拆分列最小值和最大值的结果集
   * @param colName 拆分列的名称
   * @return 生成的输入分片列表
   * @throws SQLException 当从ResultSet读取数据时发生SQL异常
   */
  public List<InputSplit> split(Configuration conf, ResultSet results, String colName)
      throws SQLException {

    LOG.warn("Generating splits for a floating-point index column. Due to the");
    LOG.warn("imprecise representation of floating-point values in Java, this");
    LOG.warn("may result in an incomplete import.");
    LOG.warn("You are strongly encouraged to choose an integral split column.");

    List<InputSplit> splits = new ArrayList<InputSplit>();

    // 最小值和最大值都为null，仅返回一个全空分片
    if (results.getString(1) == null && results.getString(2) == null) {
      // Range is null to null. Return a null split accordingly.
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
      return splits;
    }

    // 获取拆分列的最小和最大值
    double minVal = results.getDouble(1);
    double maxVal = results.getDouble(2);

    // Use this as a hint. May need an extra task if the size doesn't
    // divide cleanly.
    // 从配置中获取目标Map任务数量（分片数）
    int numSplits = conf.getInt(MRJobConfig.NUM_MAPS, 1);
    // 计算每个分片的步长
    double splitSize = (maxVal - minVal) / (double) numSplits;

    // 如果计算出的步长小于最小允许值，使用最小步长
    if (splitSize < MIN_INCREMENT) {
      splitSize = MIN_INCREMENT;
    }

    // 构造上下边界条件前缀
    String lowClausePrefix = colName + " >= ";
    String highClausePrefix = colName + " < ";

    // 当前分片的上下边界
    double curLower = minVal;
    double curUpper = curLower + splitSize;

    // 循环生成分片，直到上边界超过最大值
    while (curUpper < maxVal) {
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          lowClausePrefix + Double.toString(curLower),
          highClausePrefix + Double.toString(curUpper)));

      curLower = curUpper;
      curUpper += splitSize;
    }

    // Catch any overage and create the closed interval for the last split.
    // 处理最后一个分片，包含最大值
    if (curLower <= maxVal || splits.size() == 1) {
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          lowClausePrefix + Double.toString(curLower),
          colName + " <= " + Double.toString(maxVal)));
    }

    // 至少存在一个边界为null，额外添加null值分片
    if (results.getString(1) == null || results.getString(2) == null) {
      // At least one extrema is null; add a null split.
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
    }

    return splits;
  }
}