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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * 文件说明: MapReduce关系型数据库数据导入模块的分片分割接口，针对不同数据类型实现不同的分片逻辑
 *
 * 接口功能描述: 为DataDrivenDBInputFormat生成数据库输入分片。
 * DataDrivenDBInputFormat需要根据拆分列的最小值和最大值进行分片插值，
 * 不同数据类型的拆分逻辑不同，本接口定义了通用的拆分行为，不同数据类型通过实现本接口提供对应拆分逻辑。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface DBSplitter {
  /**
   * 根据拆分列的最小最大值范围，生成一组覆盖全范围的输入分片
   * @param conf Hadoop作业配置对象
   * @param results 包含拆分列最小、最大值的结果集，已经定位到目标记录，包含两列分别对应低值和高值，且类型一致
   * @param colName 用于拆分的列名
   * @return 生成的输入分片列表
   * @throws SQLException 当从ResultSet读取数据出错时抛出
   */
  List<InputSplit> split(Configuration conf, ResultSet results, String colName) throws SQLException;
}