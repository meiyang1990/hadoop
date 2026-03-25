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

package org.apache.hadoop.mapred.lib.db;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.JobConf;

/**
 * 数据库输入输出配置类，为旧版MapReduce API提供数据库连接配置能力
 * 继承新版mapreduce包中的DBConfiguration，复用核心逻辑，保持旧API兼容性
 * 用于配置MapReduce作业读写关系型数据库所需的连接参数和表信息
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DBConfiguration extends 
    org.apache.hadoop.mapreduce.lib.db.DBConfiguration {
  /** The JDBC Driver class name */
  public static final String DRIVER_CLASS_PROPERTY = 
    org.apache.hadoop.mapreduce.lib.db.DBConfiguration.DRIVER_CLASS_PROPERTY;
  
  /** JDBC Database access URL */
  public static final String URL_PROPERTY = 
    org.apache.hadoop.mapreduce.lib.db.DBConfiguration.URL_PROPERTY;

  /** User name to access the database */
  public static final String USERNAME_PROPERTY = 
    org.apache.hadoop.mapreduce.lib.db.DBConfiguration.USERNAME_PROPERTY;
  
  /** Password to access the database */
  public static final String PASSWORD_PROPERTY = 
    org.apache.hadoop.mapreduce.lib.db.DBConfiguration.PASSWORD_PROPERTY;

  /** Input table name */
  public static final String INPUT_TABLE_NAME_PROPERTY = org.apache.hadoop.
    mapreduce.lib.db.DBConfiguration.INPUT_TABLE_NAME_PROPERTY;

  /** Field names in the Input table */
  public static final String INPUT_FIELD_NAMES_PROPERTY = org.apache.hadoop.
    mapreduce.lib.db.DBConfiguration.INPUT_FIELD_NAMES_PROPERTY;

  /** WHERE clause in the input SELECT statement */
  public static final String INPUT_CONDITIONS_PROPERTY = org.apache.hadoop.
    mapreduce.lib.db.DBConfiguration.INPUT_CONDITIONS_PROPERTY;
  
  /** ORDER BY clause in the input SELECT statement */
  public static final String INPUT_ORDER_BY_PROPERTY = org.apache.hadoop.
    mapreduce.lib.db.DBConfiguration.INPUT_ORDER_BY_PROPERTY;
  
  /** Whole input query, exluding LIMIT...OFFSET */
  public static final String INPUT_QUERY = 
    org.apache.hadoop.mapreduce.lib.db.DBConfiguration.INPUT_QUERY;
  
  /** Input query to get the count of records */
  public static final String INPUT_COUNT_QUERY = 
    org.apache.hadoop.mapreduce.lib.db.DBConfiguration.INPUT_COUNT_QUERY;
  
  /** Class name implementing DBWritable which will hold input tuples */
  public static final String INPUT_CLASS_PROPERTY = 
    org.apache.hadoop.mapreduce.lib.db.DBConfiguration.INPUT_CLASS_PROPERTY;

  /** Output table name */
  public static final String OUTPUT_TABLE_NAME_PROPERTY = org.apache.hadoop.
    mapreduce.lib.db.DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY;

  /** Field names in the Output table */
  public static final String OUTPUT_FIELD_NAMES_PROPERTY = org.apache.hadoop.
    mapreduce.lib.db.DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY;  

  /** Number of fields in the Output table */
  public static final String OUTPUT_FIELD_COUNT_PROPERTY = org.apache.hadoop.
    mapreduce.lib.db.DBConfiguration.OUTPUT_FIELD_COUNT_PROPERTY;

  
  /**
   * 在旧版JobConf中配置数据库访问相关参数
   * @param job 作业配置对象
   * @param driverClass JDBC驱动类名
   * @param dbUrl 数据库连接URL
   * @param userName 数据库访问用户名
   * @param passwd 数据库访问密码
   */
  public static void configureDB(JobConf job, String driverClass, String dbUrl
      , String userName, String passwd) {

    job.set(DRIVER_CLASS_PROPERTY, driverClass);
    job.set(URL_PROPERTY, dbUrl);
    if(userName != null)
      job.set(USERNAME_PROPERTY, userName);
    if(passwd != null)
      job.set(PASSWORD_PROPERTY, passwd);    
  }

  /**
   * 在旧版JobConf中配置数据库访问相关参数（不设置用户名密码）
   * @param job 作业配置对象
   * @param driverClass JDBC驱动类名
   * @param dbUrl 数据库连接URL
   */
  public static void configureDB(JobConf job, String driverClass, String dbUrl) {
    configureDB(job, driverClass, dbUrl, null, null);
  }

  /**
   * 构造DBConfiguration实例，委托父类处理JobConf初始化
   * @param job 旧版作业配置对象
   */
  DBConfiguration(JobConf job) {
    super(job);
  }
  
}