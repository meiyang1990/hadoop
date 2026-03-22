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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 * 基于整数范围的数据库分片拆分器，用于将数据库表按整数主键区间拆分为多个输入分片，支持MapReduce并行读取数据库数据。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class IntegerSplitter implements DBSplitter {

  /**
   * 根据整数范围拆分数据库查询，生成多个输入分片。
   * @param conf 作业配置对象
   * @param results 包含拆分范围的查询结果集，第一列为最小值，第二列为最大值
   * @param colName 用于拆分的整数列名
   * @return 拆分完成的输入分片列表
   * @throws SQLException 数据库查询异常
   */
  public List<InputSplit> split(Configuration conf, ResultSet results, String colName)
      throws SQLException {

    long minVal = results.getLong(1);
    long maxVal = results.getLong(2);

    String lowClausePrefix = colName + " >= ";
    String highClausePrefix = colName + " < ";

    // 从配置获取预期map任务数量，默认1个分片
    int numSplits = conf.getInt(MRJobConfig.NUM_MAPS, 1);
    if (numSplits < 1) {
      numSplits = 1;
    }

    // 最小值和最大值都为null，返回仅包含空值查询的单个分片
    if (results.getString(1) == null && results.getString(2) == null) {
      List<InputSplit> splits = new ArrayList<InputSplit>();
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
      return splits;
    }

    // 计算所有分片边界点
    List<Long> splitPoints = split(numSplits, minVal, maxVal);
    List<InputSplit> splits = new ArrayList<InputSplit>();

    // 根据边界点生成分片
    long start = splitPoints.get(0);
    for (int i = 1; i < splitPoints.size(); i++) {
      long end = splitPoints.get(i);

      if (i == splitPoints.size() - 1) {
        // 最后一个分片使用闭区间，包含最大值
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            lowClausePrefix + Long.toString(start),
            colName + " <= " + Long.toString(end)));
      } else {
        // 普通分片使用左闭右开区间
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            lowClausePrefix + Long.toString(start),
            highClausePrefix + Long.toString(end)));
      }

      start = end;
    }

    // 如果存在空值，新增一个专门查询空值的分片
    if (results.getString(1) == null || results.getString(2) == null) {
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
    }

    return splits;
  }

  /**
   * 根据指定分片数量和整数范围，计算所有分片的边界点。
   * 生成的边界点列表长度比分片数量多1，所有分片区间除最后一个外均为左闭右开。
   * 例如列表[0, 5, 8, 12, 18]对应分片: [0,5), [5,8), [8,12), [12,18]
   * @param numSplits 期望分片数量
   * @param minVal 拆分范围最小值
   * @param maxVal 拆分范围最大值
   * @return 分片边界点列表
   * @throws SQLException 无异常抛出，方法签名保留该声明
   */
  List<Long> split(long numSplits, long minVal, long maxVal)
      throws SQLException {

    List<Long> splits = new ArrayList<Long>();

    // Use numSplits as a hint. May need an extra task if the size doesn't
    // divide cleanly.

    // 计算每个分片的区间大小
    long splitSize = (maxVal - minVal) / numSplits;
    if (splitSize < 1) {
      splitSize = 1;
    }

    long curVal = minVal;

    // 按步长生成所有分片边界
    while (curVal <= maxVal) {
      splits.add(curVal);
      curVal += splitSize;
    }

    // 如果最后一个边界不等于最大值，补充最大值到边界列表保证覆盖全范围
    if (splits.get(splits.size() - 1) != maxVal || splits.size() == 1) {
      splits.add(maxVal);
    }

    return splits;
  }
}