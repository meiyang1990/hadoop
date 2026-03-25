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

import org.apache.hadoop.classification.VisibleForTesting;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.util.Arrays;

import org.apache.hadoop.yarn.server.federation.store.sql.DatabaseProduct.DbType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.server.federation.store.sql.DatabaseProduct.isDuplicateKeyError;

/**
 * YARN联邦元数据存储SQL执行器，负责执行存储过程和通用SQL操作，处理结果解析和异常包装。
 */
public class FederationQueryRunner {

  public final static String YARN_ROUTER_SEQUENCE_NUM = "YARN_ROUTER_SEQUENCE_NUM";

  public final static String YARN_ROUTER_CURRENT_KEY_ID = "YARN_ROUTER_CURRENT_KEY_ID";

  public final static String QUERY_SEQUENCE_TABLE_SQL =
      "SELECT nextVal FROM sequenceTable WHERE sequenceName = %s";

  public final static String INSERT_SEQUENCE_TABLE_SQL =
      "INSERT INTO sequenceTable(sequenceName, nextVal) VALUES(%s, %d)";

  public final static String UPDATE_SEQUENCE_TABLE_SQL =
      "UPDATE sequenceTable SET nextVal = %d WHERE sequenceName = %s";

  public final static String DELETE_QUEUE_SQL = "DELETE FROM policies WHERE queue = %s";

  public static final Logger LOG = LoggerFactory.getLogger(FederationQueryRunner.class);

  /**
   * 执行存储过程，处理输出参数并通过处理器返回结果。
   *
   * @param conn      数据库连接
   * @param procedure 存储过程SQL语句
   * @param rsh       结果集处理器
   * @param params    存储过程参数列表，支持输入输出参数
   * @param <T>       结果泛型
   * @return 存储过程执行结果
   * @throws SQLException 调用存储过程异常时抛出
   */
  public <T> T execute(Connection conn, String procedure, ResultSetHandler<T> rsh, Object... params)
      throws SQLException {
    if (conn == null) {
      throw new SQLException("Null connection");
    }

    if (procedure == null) {
      throw new SQLException("Null Procedure SQL statement");
    }

    if (rsh == null) {
      throw new SQLException("Null ResultSetHandler");
    }

    CallableStatement stmt = null;
    T results = null;

    try {
      stmt = this.getCallableStatement(conn, procedure);
      this.fillStatement(stmt, params);
      stmt.executeUpdate();
      this.retrieveOutParameters(stmt, params);
      results = rsh.handle(params);
    } catch (SQLException e) {
      this.rethrow(e, procedure, params);
    } finally {
      close(stmt);
    }
    return results;
  }

  /**
   * 从数据库连接创建CallableStatement对象。
   *
   * @param conn 数据库连接
   * @param procedure 存储过程SQL语句
   * @return 创建好的CallableStatement
   * @throws SQLException 创建失败时抛出
   */
  @VisibleForTesting
  protected CallableStatement getCallableStatement(Connection conn, String procedure)
      throws SQLException {
    return conn.prepareCall(procedure);
  }

  /**
   * 填充存储过程的输入参数，注册输出参数。
   *
   * @param stmt CallableStatement对象
   * @param params 存储过程参数列表
   * @throws SQLException 参数设置异常时抛出
   */
  public void fillStatement(CallableStatement stmt, Object... params)
      throws SQLException {
    for (int i = 0; i < params.length; i++) {
      if (params[i] != null) {
        if (stmt != null) {
          if (params[i] instanceof FederationSQLOutParameter) {
            FederationSQLOutParameter sqlOutParameter = (FederationSQLOutParameter) params[i];
            sqlOutParameter.register(stmt, i + 1);
          } else {
            stmt.setObject(i + 1, params[i]);
          }
        }
      }
    }
  }

  /**
   * 关闭Statement语句对象。
   *
   * @param stmt 待关闭的CallableStatement
   * @throws SQLException 关闭异常时抛出
   */
  public void close(Statement stmt) throws SQLException {
    if (stmt != null) {
      stmt.close();
      stmt = null;
    }
  }

  /**
   * 从CallableStatement中提取存储过程的输出参数值。
   *
   * @param stmt CallableStatement对象
   * @param params 存储过程参数列表
   * @throws SQLException 获取输出参数异常时抛出
   */
  private void retrieveOutParameters(CallableStatement stmt, Object[] params) throws SQLException {
    if (params != null && stmt != null) {
      for (int i = 0; i < params.length; i++) {
        if (params[i] instanceof FederationSQLOutParameter) {
          FederationSQLOutParameter sqlOutParameter = (FederationSQLOutParameter) params[i];
          sqlOutParameter.setValue(stmt, i + 1);
        }
      }
    }
  }

  /**
   * 包装并重新抛出SQL异常，添加SQL语句和参数信息方便排查。
   *
   * @param cause 原始SQLException
   * @param sql 执行的存储过程SQL
   * @param params 存储过程参数
   * @throws SQLException 包装后的SQLException
   */
  protected void rethrow(SQLException cause, String sql, Object... params)
      throws SQLException {

    String causeMessage = cause.getMessage();
    if (causeMessage == null) {
      causeMessage = "";
    }

    StringBuilder msg = new StringBuilder(causeMessage);
    msg.append(" Query: ");
    msg.append(sql);
    msg.append(" Parameters: ");

    if (params == null) {
      msg.append("[]");
    } else {
      msg.append(Arrays.deepToString(params));
    }

    SQLException e = new SQLException(msg.toString(), cause.getSQLState(), cause.getErrorCode());
    e.setNextException(cause);
    throw e;
  }

  /**
   * 查询或更新自增序列表，获取下一个序列值。
   *
   * @param connection 数据库连接
   * @param sequenceName 序列名称，目前支持YARN_ROUTER_SEQUENCE_NUM和YARN_ROUTER_CURRENT_KEY_ID
   * @param isUpdate true表示更新序列值自增，false表示仅查询当前值
   *
   * @return 序列值，更新后返回自增后的新值
   * @throws SQLException 操作数据库异常时抛出
   */
  public int selectOrUpdateSequenceTable(Connection connection, String sequenceName,
      boolean isUpdate) throws SQLException {

    int maxSequenceValue = 0;
    boolean insertDone = false;
    boolean committed = false;
    Statement statement = null;

    try {

      // Step1. 查询当前序列值
      while (maxSequenceValue == 0) {
        // 构造查询SQL
        String sql = String.format(QUERY_SEQUENCE_TABLE_SQL, quoteString(sequenceName));
        DbType dbType = DatabaseProduct.getDbType(connection);
        // 添加行锁语法适配不同数据库
        String forUpdateSQL = DatabaseProduct.addForUpdateClause(dbType, sql);
        statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(forUpdateSQL);
        if (rs.next()) {
          // 查到当前序列值
          maxSequenceValue = rs.getInt("nextVal");
        } else if (insertDone) {
          // 插入后仍未查到，状态异常
          throw new SQLException("Invalid state of SEQUENCE_TABLE for " + sequenceName);
        } else {
          // 序列不存在，尝试插入初始值
          insertDone = true;
          close(statement);
          statement = connection.createStatement();
          String insertSQL = String.format(INSERT_SEQUENCE_TABLE_SQL, quoteString(sequenceName), 1);
          try {
            statement.executeUpdate(insertSQL);
          } catch (SQLException e) {
            // 重复键错误说明其他线程已经插入，继续循环查询即可
            if (isDuplicateKeyError(dbType, e)) {
              continue;
            }
            LOG.error("Unable to insert into SEQUENCE_TABLE for {}.", sequenceName, e);
            throw e;
          } finally {
            close(statement);
          }
        }
      }

      // Step2. 如果需要更新，序列值自增1
      if (isUpdate) {
        int nextSequenceValue = maxSequenceValue + 1;
        close(statement);
        statement = connection.createStatement();
        String updateSQL =
            String.format(UPDATE_SEQUENCE_TABLE_SQL, nextSequenceValue, quoteString(sequenceName));
        statement.executeUpdate(updateSQL);
        maxSequenceValue = nextSequenceValue;
      }

      // 提交事务返回结果
      connection.commit();
      committed = true;
      return maxSequenceValue;
    } catch(SQLException e){
      throw new SQLException("Unable to selectOrUpdateSequenceTable due to: " + e.getMessage(), e);
    } finally {
      // 未提交则回滚事务
      if (!committed) {
        rollbackDBConn(connection);
      }
      close(statement);
    }
  }

  /**
   * 直接更新序列表指定序列的值。
   * @param connection 数据库连接
   * @param sequenceName 序列名称
   * @param sequenceValue 新的序列值
   * @throws SQLException 操作数据库异常时抛出
   */
  public void updateSequenceTable(Connection connection, String sequenceName, int sequenceValue)
      throws SQLException {
    String updateSQL =
        String.format(UPDATE_SEQUENCE_TABLE_SQL, sequenceValue, quoteString(sequenceName));
    boolean committed = false;
    Statement statement = null;
    try {
      statement = connection.createStatement();
      statement.executeUpdate(updateSQL);
      connection.commit();
      committed = true;
    } catch (SQLException e) {
      throw new SQLException("Unable to updateSequenceTable due to: " + e.getMessage());
    } finally {
      if (!committed) {
        rollbackDBConn(connection);
      }
      close(statement);
    }
  }

  /**
   * 根据队列名称删除路由策略记录。
   * @param connection 数据库连接
   * @param queue 队列名称
   * @throws SQLException 操作数据库异常时抛出
   */
  public void deletePolicyByQueue(Connection connection, String queue)
      throws SQLException {
    String deleteSQL = String.format(DELETE_QUEUE_SQL, quoteString(queue));
    boolean committed = false;
    Statement statement = null;
    try {
      statement = connection.createStatement();
      statement.executeUpdate(deleteSQL);
      connection.commit();
      committed = true;
    } catch (SQLException e) {
      throw new SQLException("Unable to deletePolicyByQueue due to: " + e.getMessage());
    } finally {
      if (!committed) {
        rollbackDBConn(connection);
      }
      close(statement);
    }
  }

  /**
   * 清空指定表的所有数据。
   * @param connection 数据库连接
   * @param tableName 表名
   * @throws SQLException 操作数据库异常时抛出
   */
  public void truncateTable(Connection connection, String tableName)
      throws SQLException {
    DbType dbType = DatabaseProduct.getDbType(connection);
    String deleteSQL = getTruncateStatement(dbType, tableName);
    boolean committed = false;
    Statement statement = null;
    try {
      statement = connection.createStatement();
      statement.execute(deleteSQL);
      connection.commit();
      committed = true;
    } catch (SQLException e) {
      throw new SQLException("Unable to truncateTable due to: " + e.getMessage());
    } finally {
      if (!committed) {
        rollbackDBConn(connection);
      }
      close(statement);
    }
  }

  /**
   * 根据数据库类型生成清空表语句。
   * @param dbType 数据库类型
   * @param tableName 表名
   * @return 清空表SQL语句
   */
  private String getTruncateStatement(DbType dbType, String tableName) {
    if (isMYSQL(dbType)) {
      return ("DELETE FROM \"" + tableName + "\"");
    } else {
      return("DELETE FROM " + tableName);
    }
  }

  /**
   * 判断当前数据库是否为MySQL。
   * @param dbType 数据库类型
   * @return 是MySQL返回true，否则返回false
   */
  private boolean isMYSQL(DbType dbType) {
    return dbType == DbType.MYSQL;
  }

  /**
   * 回滚数据库连接事务，捕获并记录回滚异常。
   * @param dbConn 数据库连接
   */
  static void rollbackDBConn(Connection dbConn) {
    try {
      if (dbConn != null && !dbConn.isClosed()) {
        dbConn.rollback();
      }
    } catch (SQLException e) {
      LOG.warn("Failed to rollback db connection ", e);
    }
  }

  /**
   * 给字符串添加SQL单引号转义。
   * @param input 输入字符串
   * @return 包裹了单引号的字符串
   */
  static String quoteString(String input) {
    return "'" + input + "'";
  }
}