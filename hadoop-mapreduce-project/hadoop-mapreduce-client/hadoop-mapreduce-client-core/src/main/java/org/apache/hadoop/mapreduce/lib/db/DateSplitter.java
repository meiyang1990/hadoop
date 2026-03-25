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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 * 基于日期/时间类型的数据库数据分片实现类，用于数据驱动的数据库输入分片
 * 复用IntegerSplitter的分片逻辑，因为日期时间在Java中本质就是long类型
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DateSplitter extends IntegerSplitter {

  private static final Logger LOG = LoggerFactory.getLogger(DateSplitter.class);

  /**
   * 对日期类型列进行数据分片，生成多个输入分片
   * @param conf 作业配置对象
   * @param results 包含查询最小、最大值结果集
   * @param colName 用于分片的日期列名
   * @return 分片后的输入分片列表
   * @throws SQLException 数据库查询异常
   */
  public List<InputSplit> split(Configuration conf, ResultSet results, String colName)
      throws SQLException {

    long minVal;
    long maxVal;

    // 获取日期列的SQL数据类型
    int sqlDataType = results.getMetaData().getColumnType(1);
    // 将最小日期转换为时间戳long值
    minVal = resultSetColToLong(results, 1, sqlDataType);
    // 将最大日期转换为时间戳long值
    maxVal = resultSetColToLong(results, 2, sqlDataType);

    String lowClausePrefix = colName + " >= ";
    String highClausePrefix = colName + " < ";

    // 从配置获取map任务数量，默认1个
    int numSplits = conf.getInt(MRJobConfig.NUM_MAPS, 1);
    if (numSplits < 1) {
      numSplits = 1;
    }

    if (minVal == Long.MIN_VALUE && maxVal == Long.MIN_VALUE) {
      // 所有日期都是NULL，直接生成一个NULL分片
      List<InputSplit> splits = new ArrayList<InputSplit>();
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
      return splits;
    }

    // 计算分片分界点
    List<Long> splitPoints = split(numSplits, minVal, maxVal);
    List<InputSplit> splits = new ArrayList<InputSplit>();

    // 生成分片，处理纳秒精度
    long start = splitPoints.get(0);
    Date startDate = longToDate(start, sqlDataType);
    if (sqlDataType == Types.TIMESTAMP) {
      // 保留原始下界的纳秒值
      try {
        ((java.sql.Timestamp) startDate).setNanos(results.getTimestamp(1).getNanos());
      } catch (NullPointerException npe) {
        // 下界为NULL，忽略NPE，不设置纳秒
      }
    }

    // 遍历所有分界点生成分片
    for (int i = 1; i < splitPoints.size(); i++) {
      long end = splitPoints.get(i);
      Date endDate = longToDate(end, sqlDataType);

      if (i == splitPoints.size() - 1) {
        if (sqlDataType == Types.TIMESTAMP) {
          // 保留原始上界的纳秒值
          try {
            ((java.sql.Timestamp) endDate).setNanos(results.getTimestamp(2).getNanos());
          } catch (NullPointerException npe) {
            // 上界为NULL，忽略NPE，不设置纳秒
          }
        }
        // 最后一个分片使用闭区间
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            lowClausePrefix + dateToString(startDate),
            colName + " <= " + dateToString(endDate)));
      } else {
        // 普通分片使用左闭右开区间
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            lowClausePrefix + dateToString(startDate),
            highClausePrefix + dateToString(endDate)));
      }

      start = end;
      startDate = endDate;
    }

    if (minVal == Long.MIN_VALUE || maxVal == Long.MIN_VALUE) {
      // 存在NULL值，额外添加一个NULL分片
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
    }

    return splits;
  }

  /**
   * 从结果集取出日期列，转换为自纪元以来的时间戳，NULL值返回Long.MIN_VALUE
   * @param rs 结果集对象
   * @param colNum 列序号
   * @param sqlDataType SQL数据类型
   * @return 时间戳long值，NULL返回Long.MIN_VALUE
   * @throws SQLException 数据库读取异常
   */
  private long resultSetColToLong(ResultSet rs, int colNum, int sqlDataType) throws SQLException {
    try {
      switch (sqlDataType) {
      case Types.DATE:
        return rs.getDate(colNum).getTime();
      case Types.TIME:
        return rs.getTime(colNum).getTime();
      case Types.TIMESTAMP:
        return rs.getTimestamp(colNum).getTime();
      default:
        throw new SQLException("Not a date-type field");
      }
    } catch (NullPointerException npe) {
      // 空值，返回Long.MIN_VALUE标识
      LOG.warn("Encountered a NULL date in the split column. Splits may be poorly balanced.");
      return Long.MIN_VALUE;
    }
  }

  /**
   * 将long时间戳转换为对应SQL日期类型的Date对象
   * @param val 时间戳long值
   * @param sqlDataType SQL数据类型
   * @return 对应类型的Date对象
   */
  private Date longToDate(long val, int sqlDataType) {
    switch (sqlDataType) {
    case Types.DATE:
      return new java.sql.Date(val);
    case Types.TIME:
      return new java.sql.Time(val);
    case Types.TIMESTAMP:
      return new java.sql.Timestamp(val);
    default: // Shouldn't ever hit this case.
      return null;
    }
  }

  /**
   * 将日期对象格式化为SQL可识别的日期字符串
   * @param d 待格式化日期
   * @return 带单引号的SQL日期字符串
   */
  protected String dateToString(Date d) {
    return "'" + d.toString() + "'";
  }
}