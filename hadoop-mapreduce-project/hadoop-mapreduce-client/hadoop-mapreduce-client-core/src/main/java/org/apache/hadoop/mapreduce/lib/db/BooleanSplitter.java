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

/**
 * 针对布尔类型字段的数据库分片实现，用于将布尔类型字段的数据分片，实现MapReduce并行读取数据库数据
 * 实现了基于布尔值的数据库分片策略，根据true/false/null三种情况生成输入分片
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BooleanSplitter implements DBSplitter {

  /**
   * 根据布尔类型字段的取值范围生成输入分片，实现并行读取
   * @param conf Hadoop配置对象
   * @param results 包含字段最大最小值的结果集
   * @param colName 分片依据的布尔字段名
   * @return 生成的输入分片列表
   * @throws SQLException 数据库访问异常
   */
  public List<InputSplit> split(Configuration conf, ResultSet results, String colName)
      throws SQLException {

    List<InputSplit> splits = new ArrayList<InputSplit>();

    if (results.getString(1) == null && results.getString(2) == null) {
      // 所有值都为null，只生成一个null值的分片
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
      return splits;
    }

    boolean minVal = results.getBoolean(1);
    boolean maxVal = results.getBoolean(2);

    // 存在false值，生成false值分片
    if (!minVal) {
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " = FALSE", colName + " = FALSE"));
    }

    // 存在true值，生成true值分片
    if (maxVal) {
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " = TRUE", colName + " = TRUE"));
    }

    if (results.getString(1) == null || results.getString(2) == null) {
      // 存在null值，额外添加null值分片
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
    }

    return splits;
  }
}