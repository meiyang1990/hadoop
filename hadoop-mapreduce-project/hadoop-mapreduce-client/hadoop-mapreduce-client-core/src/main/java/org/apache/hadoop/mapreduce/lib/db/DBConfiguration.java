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
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;

/**
 * 文件级注释：MapReduce数据库读写模块的配置容器类，定义了所有JDBC数据库输入输出相关配置项
 * ，提供配置数据库连接、获取数据库连接以及读写输入输出表信息的工具方法，供DBInputFormat和DBOutputFormat使用。
 *
 * A container for configuration property names for jobs with DB input/output.
 *  
 * The job can be configured using the static methods in this class, 
 * {@link DBInputFormat}, and {@link DBOutputFormat}. 
 * Alternatively, the properties can be set in the configuration with proper
 * values. 
 *   
 * @see DBConfiguration#configureDB(Configuration, String, String, String, String)
 * @see DBInputFormat#setInput(Job, Class, String, String)
 * @see DBInputFormat#setInput(Job, Class, String, String, String, String...)
 * @see DBOutputFormat#setOutput(Job, String, String...)
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DBConfiguration {

  /** The JDBC Driver class name */
  public static final String DRIVER_CLASS_PROPERTY = 
    "mapreduce.jdbc.driver.class";
  
  /** JDBC Database access URL */
  public static final String URL_PROPERTY = "mapreduce.jdbc.url";

  /** User name to access the database */
  public static final String USERNAME_PROPERTY = "mapreduce.jdbc.username";
  
  /** Password to access the database */
  public static final String PASSWORD_PROPERTY = "mapreduce.jdbc.password";

  /** Input table name */
  public static final String INPUT_TABLE_NAME_PROPERTY = 
    "mapreduce.jdbc.input.table.name";

  /** Field names in the Input table */
  public static final String INPUT_FIELD_NAMES_PROPERTY = 
    "mapreduce.jdbc.input.field.names";

  /** WHERE clause in the input SELECT statement */
  public static final String INPUT_CONDITIONS_PROPERTY = 
    "mapreduce.jdbc.input.conditions";
  
  /** ORDER BY clause in the input SELECT statement */
  public static final String INPUT_ORDER_BY_PROPERTY = 
    "mapreduce.jdbc.input.orderby";
  
  /** Whole input query, exluding LIMIT...OFFSET */
  public static final String INPUT_QUERY = "mapreduce.jdbc.input.query";
  
  /** Input query to get the count of records */
  public static final String INPUT_COUNT_QUERY = 
    "mapreduce.jdbc.input.count.query";
  
  /** Input query to get the max and min values of the jdbc.input.query */
  public static final String INPUT_BOUNDING_QUERY =
      "mapred.jdbc.input.bounding.query";
  
  /** Class name implementing DBWritable which will hold input tuples */
  public static final String INPUT_CLASS_PROPERTY = 
    "mapreduce.jdbc.input.class";

  /** Output table name */
  public static final String OUTPUT_TABLE_NAME_PROPERTY = 
    "mapreduce.jdbc.output.table.name";

  /** Field names in the Output table */
  public static final String OUTPUT_FIELD_NAMES_PROPERTY = 
    "mapreduce.jdbc.output.field.names";  

  /** Number of fields in the Output table */
  public static final String OUTPUT_FIELD_COUNT_PROPERTY = 
    "mapreduce.jdbc.output.field.count";  
  
  /**
   * 在Configuration中配置数据库访问相关参数
   * @param conf 配置对象
   * @param driverClass JDBC驱动类名
   * @param dbUrl 数据库访问URL
   * @param userName 数据库访问用户名
   * @param passwd 数据库访问密码
   */
  public static void configureDB(Configuration conf, String driverClass, 
      String dbUrl, String userName, String passwd) {

    conf.set(DRIVER_CLASS_PROPERTY, driverClass);
    conf.set(URL_PROPERTY, dbUrl);
    if (userName != null) {
      conf.set(USERNAME_PROPERTY, userName);
    }
    if (passwd != null) {
      conf.set(PASSWORD_PROPERTY, passwd);
    }
  }

  /**
   * 在Configuration中配置数据库访问相关参数（不设置用户名密码）
   * @param job 配置对象
   * @param driverClass JDBC驱动类名
   * @param dbUrl 数据库访问URL
   */
  public static void configureDB(Configuration job, String driverClass,
      String dbUrl) {
    configureDB(job, driverClass, dbUrl, null, null);
  }

  private Configuration conf;

  /**
   * 构造DBConfiguration实例，包装给定的配置对象
   * @param job 配置对象
   */
  public DBConfiguration(Configuration job) {
    this.conf = job;
  }

  /**
   * 根据配置参数创建并返回数据库连接
   * @return 数据库连接对象
   * @throws ClassNotFoundException 找不到JDBC驱动类
   * @throws SQLException 数据库连接异常
   */
  public Connection getConnection() 
      throws ClassNotFoundException, SQLException {

    // 加载JDBC驱动类
    Class.forName(conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY));

    // 根据是否配置用户名密码选择不同的获取连接方式
    if(conf.get(DBConfiguration.USERNAME_PROPERTY) == null) {
      return DriverManager.getConnection(
               conf.get(DBConfiguration.URL_PROPERTY));
    } else {
      return DriverManager.getConnection(
          conf.get(DBConfiguration.URL_PROPERTY), 
          conf.get(DBConfiguration.USERNAME_PROPERTY), 
          conf.get(DBConfiguration.PASSWORD_PROPERTY));
    }
  }

  /**
   * 获取内部包装的配置对象
   * @return 配置对象
   */
  public Configuration getConf() {
    return conf;
  }
  
  /**
   * 获取输入表名
   * @return 输入表名
   */
  public String getInputTableName() {
    return conf.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY);
  }

  /**
   * 设置输入表名
   * @param tableName 输入表名
   */
  public void setInputTableName(String tableName) {
    conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, tableName);
  }

  /**
   * 获取输入字段名数组
   * @return 输入字段名数组
   */
  public String[] getInputFieldNames() {
    return conf.getStrings(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY);
  }

  /**
   * 设置输入字段名数组
   * @param fieldNames 输入字段名数组
   */
  public void setInputFieldNames(String... fieldNames) {
    conf.setStrings(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY, fieldNames);
  }

  /**
   * 获取查询条件语句（WHERE子句）
   * @return 查询条件语句
   */
  public String getInputConditions() {
    return conf.get(DBConfiguration.INPUT_CONDITIONS_PROPERTY);
  }

  /**
   * 设置查询条件语句（WHERE子句）
   * @param conditions 查询条件语句
   */
  public void setInputConditions(String conditions) {
    if (conditions != null && conditions.length() > 0)
      conf.set(DBConfiguration.INPUT_CONDITIONS_PROPERTY, conditions);
  }

  /**
   * 获取排序语句（ORDER BY子句）
   * @return 排序语句
   */
  public String getInputOrderBy() {
    return conf.get(DBConfiguration.INPUT_ORDER_BY_PROPERTY);
  }
  
  /**
   * 设置排序语句（ORDER BY子句）
   * @param orderby 排序语句
   */
  public void setInputOrderBy(String orderby) {
    if(orderby != null && orderby.length() >0) {
      conf.set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, orderby);
    }
  }
  
  /**
   * 获取自定义输入查询语句（不含LIMIT OFFSET）
   * @return 自定义输入查询语句
   */
  public String getInputQuery() {
    return conf.get(DBConfiguration.INPUT_QUERY);
  }
  
  /**
   * 设置自定义输入查询语句（不含LIMIT OFFSET）
   * @param query 自定义输入查询语句
   */
  public void setInputQuery(String query) {
    if(query != null && query.length() >0) {
      conf.set(DBConfiguration.INPUT_QUERY, query);
    }
  }
  
  /**
   * 获取记录总数查询语句
   * @return 记录总数查询语句
   */
  public String getInputCountQuery() {
    return conf.get(DBConfiguration.INPUT_COUNT_QUERY);
  }
  
  /**
   * 设置记录总数查询语句
   * @param query 记录总数查询语句
   */
  public void setInputCountQuery(String query) {
    if(query != null && query.length() > 0) {
      conf.set(DBConfiguration.INPUT_COUNT_QUERY, query);
    }
  }

  /**
   * 设置边界查询语句，用于获取分片的最大最小值
   * @param query 边界查询语句
   */
  public void setInputBoundingQuery(String query) {
    if (query != null && query.length() > 0) {
      conf.set(DBConfiguration.INPUT_BOUNDING_QUERY, query);
    }
  }

  /**
   * 获取边界查询语句
   * @return 边界查询语句
   */
  public String getInputBoundingQuery() {
    return conf.get(DBConfiguration.INPUT_BOUNDING_QUERY);
  }

  /**
   * 获取输入数据对应的DBWritable实现类
   * @return 输入数据类型类
   */
  public Class<?> getInputClass() {
    return conf.getClass(DBConfiguration.INPUT_CLASS_PROPERTY,
                         NullDBWritable.class);
  }

  /**
   * 设置输入数据对应的DBWritable实现类
   * @param inputClass 输入数据类型类
   */
  public void setInputClass(Class<? extends DBWritable> inputClass) {
    conf.setClass(DBConfiguration.INPUT_CLASS_PROPERTY, inputClass,
                  DBWritable.class);
  }

  /**
   * 获取输出表名
   * @return 输出表名
   */
  public String getOutputTableName() {
    return conf.get(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY);
  }

  /**
   * 设置输出表名
   * @param tableName 输出表名
   */
  public void setOutputTableName(String tableName) {
    conf.set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, tableName);
  }

  /**
   * 获取输出字段名数组
   * @return 输出字段名数组
   */
  public String[] getOutputFieldNames() {
    return conf.getStrings(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY);
  }

  /**
   * 设置输出字段名数组
   * @param fieldNames 输出字段名数组
   */
  public void setOutputFieldNames(String... fieldNames) {
    conf.setStrings(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, fieldNames);
  }

  /**
   * 设置输出字段数量
   * @param fieldCount 输出字段数量
   */
  public void setOutputFieldCount(int fieldCount) {
    conf.setInt(DBConfiguration.OUTPUT_FIELD_COUNT_PROPERTY, fieldCount);
  }
  
  /**
   * 获取输出字段数量
   * @return 输出字段数量，默认0
   */
  public int getOutputFieldCount() {
    return conf.getInt(OUTPUT_FIELD_COUNT_PROPERTY, 0);
  }
  
}