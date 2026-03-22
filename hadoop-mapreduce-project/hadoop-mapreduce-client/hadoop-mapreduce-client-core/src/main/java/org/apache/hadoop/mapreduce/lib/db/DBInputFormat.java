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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明: 从关系型数据库读取数据作为MapReduce输入的InputFormat实现类
 * 核心功能: 将数据库表数据拆分为多个输入分片，供Map任务并行读取，支持多种数据库适配
 * <p>
 * 输出键值对: 键为包含行号的LongWritable，值为封装了行数据的DBWritable
 * 
 * 支持通过两种setInput方法配置查询和输入类
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DBInputFormat<T extends DBWritable>
    extends InputFormat<LongWritable, T> implements Configurable {

  private static final Logger LOG =
      LoggerFactory.getLogger(DBInputFormat.class);
  
  // 数据库产品名称，用于选择适配的RecordReader实现，默认使用通用实现
  protected String dbProductName = "DEFAULT";

  /**
   * 空DBWritable实现，占位类，不做任何实际数据读写
   */
  @InterfaceStability.Evolving
  public static class NullDBWritable implements DBWritable, Writable {
    @Override
    public void readFields(DataInput in) throws IOException { }
    @Override
    public void readFields(ResultSet arg0) throws SQLException { }
    @Override
    public void write(DataOutput out) throws IOException { }
    @Override
    public void write(PreparedStatement arg0) throws SQLException { }
  }
  
  /**
   * 数据库输入分片，代表一个分片需要读取的行范围
   */
  @InterfaceStability.Evolving
  public static class DBInputSplit extends InputSplit implements Writable {

    private long end = 0;
    private long start = 0;

    /**
     * 默认构造函数
     */
    public DBInputSplit() {
    }

    /**
     * 构造函数，指定分片的起止行索引
     * @param start 第一个要读取的行的索引
     * @param end 最后一个要读取的行的索引
     */
    public DBInputSplit(long start, long end) {
      this.start = start;
      this.end = end;
    }

    /** {@inheritDoc} */
    public String[] getLocations() throws IOException {
      // TODO Add a layer to enable SQL "sharding" and support locality
      return new String[] {};
    }

    /**
     * @return 分片起始行索引
     */
    public long getStart() {
      return start;
    }

    /**
     * @return 分片结束行索引
     */
    public long getEnd() {
      return end;
    }

    /**
     * @return 分片包含的总行数
     */
    public long getLength() throws IOException {
      return end - start;
    }

    /** {@inheritDoc} */
    public void readFields(DataInput input) throws IOException {
      start = input.readLong();
      end = input.readLong();
    }

    /** {@inheritDoc} */
    public void write(DataOutput output) throws IOException {
      output.writeLong(start);
      output.writeLong(end);
    }
  }

  // 查询条件
  protected String conditions;

  // 数据库连接
  protected Connection connection;

  // 要读取的表名
  protected String tableName;

  // 要读取的字段名数组
  protected String[] fieldNames;

  // 数据库配置对象
  protected DBConfiguration dbConf;

  /** {@inheritDoc} */
  public void setConf(Configuration conf) {
    // 初始化数据库配置
    dbConf = new DBConfiguration(conf);

    try {
      // 创建数据库连接
      this.connection = createConnection();

      // 获取数据库元信息，提取数据库产品名
      DatabaseMetaData dbMeta = connection.getMetaData();
      this.dbProductName =
          StringUtils.toUpperCase(dbMeta.getDatabaseProductName());
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    // 从配置中读取输入表名、字段名、查询条件
    tableName = dbConf.getInputTableName();
    fieldNames = dbConf.getInputFieldNames();
    conditions = dbConf.getInputConditions();
  }

  public Configuration getConf() {
    return dbConf.getConf();
  }
  
  public DBConfiguration getDBConf() {
    return dbConf;
  }

  /**
   * 获取数据库连接，处理向后兼容场景，连接不存在时自动创建
   * @return 数据库连接对象
   */
  public Connection getConnection() {
    // TODO Remove this code that handles backward compatibility.
    if (this.connection == null) {
      this.connection = createConnection();
    }

    return this.connection;
  }

  /**
   * 创建新的数据库连接，配置事务隔离级别
   * @return 新建的数据库连接
   */
  public Connection createConnection() {
    try {
      // 从DBConfiguration获取连接
      Connection newConnection = dbConf.getConnection();
      // 关闭自动提交
      newConnection.setAutoCommit(false);
      // 设置序列化事务隔离级别
      newConnection.setTransactionIsolation(
          Connection.TRANSACTION_SERIALIZABLE);

      return newConnection;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String getDBProductName() {
    return dbProductName;
  }

  /**
   * 根据数据库类型创建适配的DBRecordReader实例
   * @param split 输入分片
   * @param conf 作业配置
   * @return 对应数据库的RecordReader实例
   * @throws IOException 创建失败时抛出IO异常
   */
  protected RecordReader<LongWritable, T> createDBRecordReader(DBInputSplit split,
      Configuration conf) throws IOException {

    @SuppressWarnings("unchecked")
    Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
    try {
      // 根据数据库产品名称选择对应实现
      if (dbProductName.startsWith("ORACLE")) {
        // 使用Oracle专属读取器
        return new OracleDBRecordReader<T>(split, inputClass,
            conf, createConnection(), getDBConf(), conditions, fieldNames,
            tableName);
      } else if (dbProductName.startsWith("MYSQL")) {
        // 使用MySQL专属读取器
        return new MySQLDBRecordReader<T>(split, inputClass,
            conf, createConnection(), getDBConf(), conditions, fieldNames,
            tableName);
      } else {
        // 使用通用读取器
        return new DBRecordReader<T>(split, inputClass,
            conf, createConnection(), getDBConf(), conditions, fieldNames,
            tableName);
      }
    } catch (SQLException ex) {
      throw new IOException(ex.getMessage());
    }
  }

  /** {@inheritDoc} */
  public RecordReader<LongWritable, T> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {  
    // 创建对应数据库的RecordReader
    return createDBRecordReader((DBInputSplit) split, context.getConfiguration());
  }

  /** {@inheritDoc} */
  /**
   * 计算输入分片，根据总行数和Map任务数将数据切分为多个分片
   * @param job 作业上下文
   * @return 分片列表
   * @throws IOException 数据库查询异常时抛出IO异常
   */
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    ResultSet results = null;  
    Statement statement = null;
    try {
      // 创建Statement执行查询
      statement = connection.createStatement();

      // 执行总行数查询，获取符合条件的总行数
      results = statement.executeQuery(getCountQuery());
      results.next();

      long count = results.getLong(1);
      // 从配置获取Map任务数量，默认1个
      int chunks = job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
      // 计算每个分片的平均行数
      long chunkSize = (count / chunks);

      // 关闭结果集和语句对象
      results.close();
      statement.close();

      List<InputSplit> splits = new ArrayList<InputSplit>();

      // 按行数拆分分片，最后一个分片处理剩余行数
      for (int i = 0; i < chunks; i++) {
        DBInputSplit split;

        if ((i + 1) == chunks)
          split = new DBInputSplit(i * chunkSize, count);
        else
          split = new DBInputSplit(i * chunkSize, (i * chunkSize)
              + chunkSize);

        splits.add(split);
      }

      // 提交事务，释放资源
      connection.commit();
      return splits;
    } catch (SQLException e) {
      throw new IOException("Got SQLException", e);
    } finally {
      // 关闭结果集，处理异常不抛出
      try {
        if (results != null) { results.close(); }
      } catch (SQLException e1) {}
      // 关闭Statement，处理异常不抛出
      try {
        if (statement != null) { statement.close(); }
      } catch (SQLException e1) {}

      // 关闭数据库连接
      closeConnection();
    }
  }

  /** 
   * 生成统计总行数的SQL查询语句，子类可以覆盖实现自定义逻辑
   * @return 统计总行数的SQL字符串
   */
  protected String getCountQuery() {
    // 如果用户配置了自定义统计查询，直接返回
    if(dbConf.getInputCountQuery() != null) {
      return dbConf.getInputCountQuery();
    }
    
    StringBuilder query = new StringBuilder();
    query.append("SELECT COUNT(*) FROM " + tableName);

    // 拼接查询条件
    if (conditions != null && conditions.length() > 0)
      query.append(" WHERE " + conditions);
    return query.toString();
  }

  /**
   * 初始化作业的数据库输入配置，基于表名、字段、条件方式配置
   * 
   * @param job MapReduce作业对象
   * @param inputClass 实现DBWritable的输入数据类，用于封装行数据
   * @param tableName 要读取的数据库表名
   * @param conditions 查询条件语句，eg. '(updated > 20070101 AND length > 0)'
   * @param orderBy 排序字段名
   * @param fieldNames 要读取的字段名数组
   */
  public static void setInput(Job job, 
      Class<? extends DBWritable> inputClass,
      String tableName,String conditions, 
      String orderBy, String... fieldNames) {
    // 设置InputFormat为DBInputFormat
    job.setInputFormatClass(DBInputFormat.class);
    // 初始化DBConfiguration并设置各项参数
    DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
    dbConf.setInputClass(inputClass);
    dbConf.setInputTableName(tableName);
    dbConf.setInputFieldNames(fieldNames);
    dbConf.setInputConditions(conditions);
    dbConf.setInputOrderBy(orderBy);
  }
  
  /**
   * 初始化作业的数据库输入配置，基于自定义查询语句方式配置
   * 
   * @param job MapReduce作业对象
   * @param inputClass 实现DBWritable的输入数据类，用于封装行数据
   * @param inputQuery 自定义数据查询语句，示例: "SELECT f1, f2, f3 FROM Mytable ORDER BY f1"
   * @param inputCountQuery 自定义总行数统计查询语句，示例: "SELECT COUNT(f1) FROM Mytable"
   */
  public static void setInput(Job job,
      Class<? extends DBWritable> inputClass,
      String inputQuery, String inputCountQuery) {
    // 设置InputFormat为DBInputFormat
    job.setInputFormatClass(DBInputFormat.class);
    // 初始化DBConfiguration并设置自定义查询语句
    DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
    dbConf.setInputClass(inputClass);
    dbConf.setInputQuery(inputQuery);
    dbConf.setInputCountQuery(inputCountQuery);
  }

  /**
   * 安全关闭数据库连接，忽略关闭异常仅记录日志
   */
  protected void closeConnection() {
    try {
      if (null != this.connection) {
        this.connection.close();
        this.connection = null;
      }
    } catch (SQLException sqlE) {
      LOG.debug("Exception on close", sqlE);
    }
  }
}