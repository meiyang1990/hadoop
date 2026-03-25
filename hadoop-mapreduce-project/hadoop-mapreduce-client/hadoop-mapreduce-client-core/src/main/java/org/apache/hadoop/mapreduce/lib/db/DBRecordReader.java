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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 从关系型数据库SQL表中读取数据的RecordReader实现
 * 输出键为包含行号的LongWritable，输出值为实现DBWritable接口的自定义数据对象
 * 用于MapReduce作业从关系型数据库读取输入数据
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DBRecordReader<T extends DBWritable> extends
    RecordReader<LongWritable, T> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DBRecordReader.class);

  private ResultSet results = null;

  private Class<T> inputClass;

  private Configuration conf;

  private DBInputFormat.DBInputSplit split;

  private long pos = 0;
  
  private LongWritable key = null;
  
  private T value = null;

  private Connection connection;

  protected PreparedStatement statement;

  private DBConfiguration dbConf;

  private String conditions;

  private String [] fieldNames;

  private String tableName;

  /**
   * 构造DBRecordReader，为指定输入分片读取数据
   * @param split 要读取数据的输入分片
   * @param inputClass 输出值对象的类型
   * @param conf Hadoop配置对象
   * @param conn 数据库连接
   * @param dbConfig 数据库配置对象
   * @param cond 查询条件
   * @param fields 需要读取的字段名数组
   * @param table 要读取的表名
   * @throws SQLException 数据库异常
   */
  public DBRecordReader(DBInputFormat.DBInputSplit split, 
      Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConfig,
      String cond, String [] fields, String table)
      throws SQLException {
    this.inputClass = inputClass;
    this.split = split;
    this.conf = conf;
    this.connection = conn;
    this.dbConf = dbConfig;
    this.conditions = cond;
    this.fieldNames = fields;
    this.tableName = table;
  }

  /**
   * 执行指定的SQL查询语句，获取结果集
   * @param query 要执行的查询SQL
   * @return 查询结果集
   * @throws SQLException 数据库异常
   */
  protected ResultSet executeQuery(String query) throws SQLException {
    this.statement = connection.prepareStatement(query,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    return statement.executeQuery();
  }

  /**
   * 生成查询当前分片数据的SELECT语句，子类可覆盖实现自定义查询逻辑
   * @return 可执行的SQL查询字符串
   */
  protected String getSelectQuery() {
    StringBuilder query = new StringBuilder();

    // 默认路径：适配MySQL、HSQLDB等，依赖LIMIT/OFFSET实现分片
    if(dbConf.getInputQuery() == null) {
      query.append("SELECT ");
  
      // 拼接所有要查询的字段名
      for (int i = 0; i < fieldNames.length; i++) {
        query.append(fieldNames[i]);
        if (i != fieldNames.length -1) {
          query.append(", ");
        }
      }

      // 拼接表名，HSQLDB要求必须加表别名
      query.append(" FROM ").append(tableName);
      query.append(" AS ").append(tableName);
      // 拼接用户自定义查询条件
      if (conditions != null && conditions.length() > 0) {
        query.append(" WHERE (").append(conditions).append(")");
      }

      // 拼接排序语句
      String orderBy = dbConf.getInputOrderBy();
      if (orderBy != null && orderBy.length() > 0) {
        query.append(" ORDER BY ").append(orderBy);
      }
    } else {
      // 用户提供了完整自定义查询，直接使用
      query.append(dbConf.getInputQuery());
    }
        
    // 拼接分片的LIMIT和OFFSET，实现只查询当前分片对应的数据
    try {
      query.append(" LIMIT ").append(split.getLength());
      query.append(" OFFSET ").append(split.getStart());
    } catch (IOException ex) {
      // 不会抛出异常，忽略即可
    }		

    return query.toString();
  }

  /** {@inheritDoc} */
  /**
   * 关闭数据库资源，包括结果集、语句对象和连接
   * @throws IOException 关闭失败时抛出IO异常
   */
  public void close() throws IOException {
    try {
      if (null != results) {
        results.close();
      }
      if (null != statement) {
        statement.close();
      }
      if (null != connection) {
        connection.commit();
        connection.close();
      }
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * 初始化RecordReader，此处无额外初始化逻辑
   * @param split 输入分片
   * @param context 任务尝试上下文
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void initialize(InputSplit split, TaskAttemptContext context) 
      throws IOException, InterruptedException {
    //do nothing
  }

  /** {@inheritDoc} */
  public LongWritable getCurrentKey() {
    return key;  
  }

  /** {@inheritDoc} */
  public T getCurrentValue() {
    return value;
  }

  /**
   * @deprecated 已废弃，使用nextKeyValue()代替
   */
  @Deprecated
  public T createValue() {
    return ReflectionUtils.newInstance(inputClass, conf);
  }

  /**
   * @deprecated 已废弃
   */
  @Deprecated
  public long getPos() throws IOException {
    return pos;
  }

  /**
   * @deprecated 已废弃，使用{@link #nextKeyValue()}代替
   */
  @Deprecated
  public boolean next(LongWritable key, T value) throws IOException {
    this.key = key;
    this.value = value;
    return nextKeyValue();
  }

  /** {@inheritDoc} */
  public float getProgress() throws IOException {
    return pos / (float)split.getLength();
  }

  /** {@inheritDoc} */
  public boolean nextKeyValue() throws IOException {
    try {
      // 延迟初始化键对象
      if (key == null) {
        key = new LongWritable();
      }
      // 延迟初始化值对象
      if (value == null) {
        value = createValue();
      }
      // 第一次调用，执行查询获取结果集
      if (null == this.results) {
        this.results = executeQuery(getSelectQuery());
      }
      // 移动到下一行，没有更多数据则返回false
      if (!results.next())
        return false;

      // 设置行号键：基于分片起始位置计算全局行号
      key.set(pos + split.getStart());

      // 从结果集读取当前行数据到值对象
      value.readFields(results);

      // 已读取行数自增
      pos ++;
    } catch (SQLException e) {
      throw new IOException("SQLException in nextKeyValue", e);
    }
    return true;
  }

  protected DBInputFormat.DBInputSplit getSplit() {
    return split;
  }

  protected String [] getFieldNames() {
    return fieldNames;
  }

  protected String getTableName() {
    return tableName;
  }

  protected String getConditions() {
    return conditions;
  }

  protected DBConfiguration getDBConf() {
    return dbConf;
  }

  protected Connection getConnection() {
    return connection;
  }

  protected PreparedStatement getStatement() {
    return statement;
  }

  protected void setStatement(PreparedStatement stmt) {
    this.statement = stmt;
  }
}