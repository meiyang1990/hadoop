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
 * 文件说明: MySQL数据库数据驱动分片读取实现，基于DataDrivenDBRecordReader扩展，提供MySQL专属的数据读取能力
 * 用于MapReduce任务从MySQL数据库分片读取数据，支持大数据量分页查询避免OOM
 * 
 * 实现MySQL数据记录读取功能，通过数据驱动分片方式读取MySQL表数据
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MySQLDataDrivenDBRecordReader<T extends DBWritable>
    extends DataDrivenDBRecordReader<T> {

  /**
   * 构造MySQL数据驱动记录读取器，初始化父类并指定数据库类型为MYSQL
   * @param split 数据库输入分片，对应本次读取的数据范围
   * @param inputClass 输出数据类型，需要实现DBWritable接口
   * @param conf Hadoop作业配置对象
   * @param conn 数据库连接对象
   * @param dbConfig 数据库连接配置
   * @param cond 查询条件语句
   * @param fields 需要读取的字段数组
   * @param table 读取的数据表名
   * @throws SQLException 数据库操作异常时抛出
   */
  public MySQLDataDrivenDBRecordReader(DBInputFormat.DBInputSplit split,
      Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConfig,
      String cond, String [] fields, String table) throws SQLException {
    super(split, inputClass, conf, conn, dbConfig, cond, fields, table, "MYSQL");
  }

  /**
   * 执行MySQL查询，使用流式读取模式避免一次性加载全量数据到内存
   * @param query 要执行的查询SQL语句
   * @return 查询结果集对象
   * @throws SQLException 数据库查询执行异常时抛出
   */
  protected ResultSet executeQuery(String query) throws SQLException {
    // 创建只读、仅向前滚动的预处理语句
    statement = getConnection().prepareStatement(query,
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    // 设置MySQL流式读取参数：逐行读取结果集，避免全量加载导致OOM
    statement.setFetchSize(Integer.MIN_VALUE); // MySQL: read row-at-a-time.
    // 执行查询并返回结果集
    return statement.executeQuery();
  }
}