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

package org.apache.hadoop.yarn.server.federation.store.sql;

import org.apache.hadoop.classification.InterfaceAudience.Private;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * YARN联邦状态存储SQL数据库工具类，提供数据库类型识别、方言适配等功能。
 * 为不同数据库提供统一的SQL语法适配，支持联邦存储跨数据库兼容。
 */
@Private
public final class DatabaseProduct {

  /**
   * 支持的数据库类型枚举。
   */
  public enum DbType {MYSQL, SQLSERVER, POSTGRES, UNDEFINED, HSQLDB}

  private static final String SQL_SERVER_NAME = "sqlserver";
  private static final String MYSQL_NAME = "mysql";
  private static final String MARIADB_NAME = "mariadb";
  private static final String HSQLDB_NAME = "hsqldatabase";

  private DatabaseProduct() {
  }

  /**
   * 根据JDBC连接识别数据库类型。
   * @param conn JDBC连接
   * @return 识别出的数据库类型
   * @throws SQLException 获取连接元数据失败时抛出
   */
  public static DbType getDbType(Connection conn) throws SQLException {
    if (conn == null) {
      return DbType.UNDEFINED;
    }
    String productName = getProductName(conn);
    return getDbType(productName);
  }

  /**
   * 根据数据库产品名称匹配数据库类型。
   * @param productName 数据库产品名称
   * @return 匹配到的数据库类型
   */
  private static DbType getDbType(String productName) {
    DbType dbt;
    // 格式化产品名称：去除空格转小写，方便匹配
    productName = productName.replaceAll("\\s+", "").toLowerCase();
    if (productName.contains(SQL_SERVER_NAME)) {
      dbt = DbType.SQLSERVER;
    } else if (productName.contains(MYSQL_NAME) || productName.contains(MARIADB_NAME)) {
      // MariaDB兼容MySQL处理
      dbt = DbType.MYSQL;
    } else if (productName.contains(HSQLDB_NAME)) {
      dbt = DbType.HSQLDB;
    } else {
      dbt = DbType.UNDEFINED;
    }
    return dbt;
  }

  /**
   * 从JDBC连接获取数据库产品名称。
   * @param conn JDBC连接
   * @return 数据库产品名称
   * @throws SQLException 获取元数据失败时抛出
   */
  private static String getProductName(Connection conn) throws SQLException {
    return conn.getMetaData().getDatabaseProductName();
  }

  /**
   * 根据数据库类型，为查询语句添加行级锁语法，实现并发更新的互斥控制。
   * 确保同一行记录同一时间只能被一个事务更新，避免并发冲突。
   * @param dbType 数据库类型
   * @param selectStatement 原始查询SQL
   * @return 添加了行级锁后的SQL语句
   * @throws SQLException 不支持的数据库类型抛出异常
   */
  public static String addForUpdateClause(DbType dbType, String selectStatement)
      throws SQLException {
    switch (dbType) {
    case MYSQL:
    case HSQLDB:
      // MySQL/HSQLDB直接添加标准for update语法
      return selectStatement + " for update";
    case SQLSERVER:
      // SQL Server使用updlock锁提示实现行锁定
      String modifier = " with (updlock)";
      // 找到WHERE子句位置，在WHERE之前插入锁提示
      int wherePos = selectStatement.toUpperCase().indexOf(" WHERE ");
      if (wherePos < 0) {
        // 无WHERE子句直接追加到末尾
        return selectStatement + modifier;
      }
      // 在WHERE子句前插入锁提示
      return selectStatement.substring(0, wherePos) + modifier +
          selectStatement.substring(wherePos, selectStatement.length());
    default:
      String msg = "Unrecognized database product name <" + dbType + ">";
      throw new SQLException(msg);
    }
  }

  /**
   * 判断SQL异常是否为唯一键冲突错误（重复插入数据）。
   * 根据不同数据库的错误码和SQL状态码判断重复键异常。
   * @param dbType 数据库类型
   * @param ex 捕获的SQL异常
   * @return true表示是重复键冲突错误，false否则
   */
  public static boolean isDuplicateKeyError(DbType dbType, SQLException ex) {
    switch (dbType) {
    case MYSQL:
      // MySQL重复键错误码：1022(键重复)、1062(主键重复)、1586(唯一键冲突)
      // SQL状态统一为23000表示完整性约束冲突
      if((ex.getErrorCode() == 1022 || ex.getErrorCode() == 1062 || ex.getErrorCode() == 1586) &&
          "23000".equals(ex.getSQLState())) {
        return true;
      }
      break;
    case SQLSERVER:
      // SQL Server重复键错误码：2627(主键约束冲突)、2601(唯一索引冲突)
      // SQL状态统一为23000
      if ((ex.getErrorCode() == 2627 || ex.getErrorCode() == 2601)
          && "23000".equals(ex.getSQLState())) {
        return true;
      }
      break;
    default:
      return false;
    }
    return false;
  }
}