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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明: MapReduce 关系型数据库输出实现，将Reduce任务输出写入SQL数据库表
 * <p>
 * A OutputFormat that sends the reduce output to a SQL table.
 * <p> 
 * {@link DBOutputFormat} accepts &lt;key,value&gt; pairs, where 
 * key has a type extending DBWritable. Returned {@link RecordWriter} 
 * writes <b>only the key</b> to the database with a batch SQL query.  
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
/**
 * 数据库输出格式类，将MapReduce计算结果批量写入关系型数据库表
 * @param <K> 输出键类型，必须继承DBWritable接口
 * @param <V> 输出值类型，本实现中不使用该值，仅写入键
 */
public class DBOutputFormat<K  extends DBWritable, V> 
extends OutputFormat<K,V> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DBOutputFormat.class);
  /** 数据库产品名称，用于生成适配不同数据库的SQL语法 */
  public String dbProductName = "DEFAULT";

  /**
   * 检查输出规格配置，本实现无需额外检查
   * {@inheritDoc}
   */
  public void checkOutputSpecs(JobContext context) 
      throws IOException, InterruptedException {}

  /**
   * 获取输出提交器，复用文件输出提交器实现任务提交逻辑
   * {@inheritDoc}
   */
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) 
      throws IOException, InterruptedException {
    return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
                                   context);
  }

  /**
   * 将Reduce输出写入SQL表的记录写入器实现类
   * A RecordWriter that writes the reduce output to a SQL table
   */
  @InterfaceStability.Evolving
  public class DBRecordWriter 
      extends RecordWriter<K, V> {

    private Connection connection;
    private PreparedStatement statement;

    public DBRecordWriter() throws SQLException {
    }

    /**
     * 构造带数据库连接和预编译语句的写入器
     * @param connection 数据库连接
     * @param statement 预编译插入语句
     * @throws SQLException 数据库操作异常
     */
    public DBRecordWriter(Connection connection
        , PreparedStatement statement) throws SQLException {
      this.connection = connection;
      this.statement = statement;
      // 关闭自动提交，开启批量事务
      this.connection.setAutoCommit(false);
    }

    public Connection getConnection() {
      return connection;
    }
    
    public PreparedStatement getStatement() {
      return statement;
    }
    
    /** {@inheritDoc} */
    public void close(TaskAttemptContext context) throws IOException {
      try {
        // 执行批量插入
        statement.executeBatch();
        // 提交事务
        connection.commit();
      } catch (SQLException e) {
        try {
          // 执行失败回滚事务
          connection.rollback();
        }
        catch (SQLException ex) {
          LOG.warn(StringUtils.stringifyException(ex));
        }
        throw new IOException(e.getMessage());
      } finally {
        try {
          // 关闭资源
          statement.close();
          connection.close();
        }
        catch (SQLException ex) {
          throw new IOException(ex.getMessage());
        }
      }
    }

    /** {@inheritDoc} */
    public void write(K key, V value) throws IOException {
      try {
        // 由DBWritable实现类将键值写入预编译语句
        key.write(statement);
        // 添加到批量任务
        statement.addBatch();
      } catch (SQLException e) {
        throw new IOException("Failed to execute SQL statement for key " + key, e);
      }
    }
  }

  /**
   * 构造插入数据的预编译SQL查询语句
   * 
   * @param table 要插入的目标表名
   * @param fieldNames 要插入的字段名数组，如果字段名未知传入null数组
   * @return 生成的INSERT SQL语句
   */
  public String constructQuery(String table, String[] fieldNames) {
    if(fieldNames == null) {
      throw new IllegalArgumentException("Field names may not be null");
    }

    StringBuilder query = new StringBuilder();
    query.append("INSERT INTO ").append(table);

    // 如果存在有效字段名，添加字段列表
    if (fieldNames.length > 0 && fieldNames[0] != null) {
      query.append(" (");
      for (int i = 0; i < fieldNames.length; i++) {
        query.append(fieldNames[i]);
        if (i != fieldNames.length - 1) {
          query.append(",");
        }
      }
      query.append(")");
    }
    query.append(" VALUES (");

    // 添加占位符
    for (int i = 0; i < fieldNames.length; i++) {
      query.append("?");
      if(i != fieldNames.length - 1) {
        query.append(",");
      }
    }

    // 根据数据库产品调整SQL语法：DB2和Oracle不需要结尾分号
    if (dbProductName.startsWith("DB2") || dbProductName.startsWith("ORACLE")) {
      query.append(")");
    } else {
      query.append(");");
    }

    return query.toString();
  }

  /** {@inheritDoc} */
  /**
   * 获取用于写入数据库的记录写入器实例
   * {@inheritDoc}
   */
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) 
      throws IOException {
    // 从任务配置中创建数据库配置对象
    DBConfiguration dbConf = new DBConfiguration(context.getConfiguration());
    // 获取输出表名
    String tableName = dbConf.getOutputTableName();
    // 获取输出字段名数组
    String[] fieldNames = dbConf.getOutputFieldNames();
    
    // 如果未指定字段名，根据字段数量创建空数组
    if(fieldNames == null) {
      fieldNames = new String[dbConf.getOutputFieldCount()];
    }
    
    try {
      // 获取数据库连接
      Connection connection = dbConf.getConnection();
      PreparedStatement statement = null;

      // 获取数据库元信息，识别数据库产品类型
      DatabaseMetaData dbMeta = connection.getMetaData();
      this.dbProductName = dbMeta.getDatabaseProductName().toUpperCase();

      // 预编译插入语句
      statement = connection.prepareStatement(
                    constructQuery(tableName, fieldNames));
      return new DBRecordWriter(connection, statement);
    } catch (Exception ex) {
      throw new IOException(ex.getMessage());
    }
  }

  /**
   * 初始化作业输出配置，指定输出数据库表和字段
   * 
   * @param job 作业对象
   * @param tableName 要插入的目标表名
   * @param fieldNames 表中目标字段名数组
   * @throws IOException 配置初始化异常
   */
  public static void setOutput(Job job, String tableName, 
      String... fieldNames) throws IOException {
    if(fieldNames.length > 0 && fieldNames[0] != null) {
      DBConfiguration dbConf = setOutput(job, tableName);
      dbConf.setOutputFieldNames(fieldNames);
    } else {
      if (fieldNames.length > 0) {
        setOutput(job, tableName, fieldNames.length);
      }
      else { 
        throw new IllegalArgumentException(
          "Field names must be greater than 0");
      }
    }
  }
  
  /**
   * 初始化作业输出配置，仅指定输出表和字段数量（不指定具体字段名）
   * 
   * @param job 作业对象
   * @param tableName 要插入的目标表名
   * @param fieldCount 表中字段数量
   * @throws IOException 配置初始化异常
   */
  public static void setOutput(Job job, String tableName, 
      int fieldCount) throws IOException {
    DBConfiguration dbConf = setOutput(job, tableName);
    dbConf.setOutputFieldCount(fieldCount);
  }
  
  /**
   * 基础输出配置初始化，设置输出格式类和表名
   * 
   * @param job 作业对象
   * @param tableName 输出目标表名
   * @return 初始化后的DBConfiguration对象
   * @throws IOException 配置初始化异常
   */
  private static DBConfiguration setOutput(Job job,
      String tableName) throws IOException {
    // 设置当前输出格式为DBOutputFormat
    job.setOutputFormatClass(DBOutputFormat.class);
    // 关闭Reduce推测执行，避免重复插入数据
    job.setReduceSpeculativeExecution(false);

    DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
    
    dbConf.setOutputTableName(tableName);
    return dbConf;
  }
}