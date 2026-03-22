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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * MySQL数据库专用数据记录读取器，为MapReduce任务从MySQL表中读取数据分片。
 * 继承通用DBRecordReader，针对MySQL特性做流式读取优化，避免一次性加载全量结果集。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MySQLDBRecordReader<T extends DBWritable> extends DBRecordReader<T> {

  /**
   * 构造MySQL数据库记录读取器实例
   * @param split 要读取的输入数据分片
   * @param inputClass 输出数据类型，需实现DBWritable接口
   * @param conf Hadoop作业配置对象
   * @param conn 数据库连接对象
   * @param dbConfig 数据库连接配置对象
   * @param cond 查询条件(WHERE子句)
   * @param fields 要读取的字段列表
   * @param table 要读取的目标表名
   * @throws SQLException 数据库连接异常时抛出
   */
  public MySQLDBRecordReader(DBInputFormat.DBInputSplit split, 
      Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConfig,
      String cond, String [] fields, String table) throws SQLException {
    super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
  }

  /**
   * 针对MySQL特性执行查询，开启流式读取避免OOM
   * @param query 要执行的查询SQL语句
   * @return 查询结果集ResultSet
   * @throws SQLException 数据库查询异常时抛出
   */
  // Execute statements for mysql in unbuffered mode.
  protected ResultSet executeQuery(String query) throws SQLException {
    // 创建仅向前只读结果集的预处理语句
    statement = getConnection().prepareStatement(query,
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    // MySQL特定设置：设为Integer.MIN_VALUE开启逐行流式读取，不一次性加载全量结果
    statement.setFetchSize(Integer.MIN_VALUE); // MySQL: read row-at-a-time.
    // 执行查询返回结果集
    return statement.executeQuery();
  }
}