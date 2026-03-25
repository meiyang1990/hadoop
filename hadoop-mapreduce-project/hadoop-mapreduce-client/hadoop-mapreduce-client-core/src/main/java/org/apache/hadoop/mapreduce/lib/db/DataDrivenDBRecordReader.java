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
import java.sql.SQLException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件描述: 基于数据驱动分片的关系型数据库数据读取器，属于MapReduce从关系型数据库读取数据的组件
 * 核心功能: 基于数据生成的WHERE子句分片读取数据库数据，输出记录编号作为键、数据库记录作为值
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DataDrivenDBRecordReader<T extends DBWritable> extends DBRecordReader<T> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataDrivenDBRecordReader.class);

  private String dbProductName; // 数据库产品厂商名称

  /**
   * 构造函数，初始化数据驱动分片数据库读取器
   * @param split 要读取数据的输入分片
   * @param inputClass 输出值类型，需实现DBWritable接口
   * @param conf Hadoop作业配置对象
   * @param conn 数据库连接对象
   * @param dbConfig 数据库配置封装对象
   * @param cond 用户自定义查询条件
   * @param fields 要读取的字段数组
   * @param table 要读取的表名
   * @param dbProduct 数据库产品名称
   * @throws SQLException 数据库操作异常时抛出
   */
  public DataDrivenDBRecordReader(DBInputFormat.DBInputSplit split,
      Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConfig,
      String cond, String [] fields, String table, String dbProduct)
      throws SQLException {
    super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
    this.dbProductName = dbProduct;
  }

  /**
   * 生成当前分片对应的SQL查询语句，子类可覆盖实现自定义查询逻辑
   * @return 生成完成的SQL查询字符串
   */
  @SuppressWarnings("unchecked")
  protected String getSelectQuery() {
    StringBuilder query = new StringBuilder();
    // 转换为数据驱动分片类型，获取分片对应的上下界条件
    DataDrivenDBInputFormat.DataDrivenDBInputSplit dataSplit =
        (DataDrivenDBInputFormat.DataDrivenDBInputSplit) getSplit();
    DBConfiguration dbConf = getDBConf();
    String [] fieldNames = getFieldNames();
    String tableName = getTableName();
    String conditions = getConditions();

    // 先拼接当前分片对应的范围条件，后续主查询会使用这个条件
    StringBuilder conditionClauses = new StringBuilder();
    conditionClauses.append("( ").append(dataSplit.getLowerClause());
    conditionClauses.append(" ) AND ( ").append(dataSplit.getUpperClause());
    conditionClauses.append(" )");

    // 用户未指定自定义查询语句，从头生成完整查询SQL
    if(dbConf.getInputQuery() == null) {
      // 拼接SELECT语句
      query.append("SELECT ");

      // 遍历拼接所有需要查询的字段名
      for (int i = 0; i < fieldNames.length; i++) {
        query.append(fieldNames[i]);
        // 不是最后一个字段添加逗号分隔
        if (i != fieldNames.length -1) {
          query.append(", ");
        }
      }

      // 拼接表名部分
      query.append(" FROM ").append(tableName);
      // 非Oracle数据库需要添加表别名，Oracle不支持该语法
      if (!dbProductName.startsWith("ORACLE")) {
        // Seems to be necessary for hsqldb? Oracle explicitly does *not*
        // use this clause.
        query.append(" AS ").append(tableName);
      }
      // 拼接WHERE子句开头
      query.append(" WHERE ");
      // 存在用户自定义条件，先拼接用户条件
      if (conditions != null && conditions.length() > 0) {
        // Put the user's conditions first.
        query.append("( ").append(conditions).append(" ) AND ");
      }

      // 拼接分片范围条件
      query.append(conditionClauses.toString());

    } else {
      // 用户已提供自定义查询，使用占位符替换方式插入分片条件
      String inputQuery = dbConf.getInputQuery();
      // 查询语句中没有占位符，打印错误日志提示分片可能不生效
      if (inputQuery.indexOf(DataDrivenDBInputFormat.SUBSTITUTE_TOKEN) == -1) {
        LOG.error("Could not find the clause substitution token "
            + DataDrivenDBInputFormat.SUBSTITUTE_TOKEN + " in the query: ["
            + inputQuery + "]. Parallel splits may not work correctly.");
      }

      // 将占位符替换为实际分片条件，生成最终查询SQL
      query.append(inputQuery.replace(DataDrivenDBInputFormat.SUBSTITUTE_TOKEN,
          conditionClauses.toString()));
    }

    // 调试日志输出最终生成的查询语句
    LOG.debug("Using query: " + query.toString());

    return query.toString();
  }
}