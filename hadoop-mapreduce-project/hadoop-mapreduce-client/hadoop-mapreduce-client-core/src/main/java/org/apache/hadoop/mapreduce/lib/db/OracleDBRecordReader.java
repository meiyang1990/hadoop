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
import java.sql.SQLException;
import java.lang.reflect.Method;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 专为Oracle数据库设计的RecordReader实现，从Oracle表中读取数据作为MapReduce输入
 * 适配Oracle特有的分页语法和时区处理逻辑
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OracleDBRecordReader<T extends DBWritable> extends DBRecordReader<T> {

  /** 配置项：Oracle会话时区，用于处理TIMESTAMP WITH LOCAL TIME ZONE类型字段 */
  public static final String SESSION_TIMEZONE_KEY = "oracle.sessionTimeZone";

  private static final Logger LOG =
      LoggerFactory.getLogger(OracleDBRecordReader.class);

  /**
   * 构造OracleDBRecordReader实例，完成初始化并设置会话时区
   * @param split 输入分片，对应本次要读取的数据范围
   * @param inputClass 输出数据类型，需实现DBWritable接口
   * @param conf Hadoop作业配置
   * @param conn 数据库连接
   * @param dbConfig 数据库输入配置
   * @param cond 查询条件
   * @param fields 要读取的字段列表
   * @param table 要读取的表名
   * @throws SQLException 数据库操作异常时抛出
   */
  public OracleDBRecordReader(DBInputFormat.DBInputSplit split, 
      Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConfig,
      String cond, String [] fields, String table) throws SQLException {
    super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
    setSessionTimeZone(conf, conn);
  }

  /**
   * 生成适配Oracle语法的分页查询SQL语句，使用ROWNUM机制实现分片读取
   * @return 完整的查询SQL字符串
   */
  protected String getSelectQuery() {
    StringBuilder query = new StringBuilder();
    DBConfiguration dbConf = getDBConf();
    String conditions = getConditions();
    String tableName = getTableName();
    String [] fieldNames = getFieldNames();

    // Oracle使用ROWNUM而非标准LIMIT/OFFSET实现分页，走专属分支
    if(dbConf.getInputQuery() == null) {
      query.append("SELECT ");
  
      for (int i = 0; i < fieldNames.length; i++) {
        query.append(fieldNames[i]);
        if (i != fieldNames.length -1) {
          query.append(", ");
        }
      }
  
      query.append(" FROM ").append(tableName);
      if (conditions != null && conditions.length() > 0)
        query.append(" WHERE ").append(conditions);
      String orderBy = dbConf.getInputOrderBy();
      if (orderBy != null && orderBy.length() > 0) {
        query.append(" ORDER BY ").append(orderBy);
      }
    } else {
      // 用户已预先定义好查询SQL，直接使用
      query.append(dbConf.getInputQuery());
    }
        
    try {
      DBInputFormat.DBInputSplit split = getSplit();
      // 分片长度大于0，说明需要分页读取，添加ROWNUM分页包装
      if (split.getLength() > 0){
        String querystring = query.toString();

        query = new StringBuilder();
        query.append("SELECT * FROM (SELECT a.*,ROWNUM dbif_rno FROM ( ");
        query.append(querystring);
        query.append(" ) a WHERE rownum <= ").append(split.getEnd());
        query.append(" ) WHERE dbif_rno > ").append(split.getStart());
      }
    } catch (IOException ex) {
      // 获取分片信息失败，不抛出异常，返回无分页的原始查询
    }		      

    return query.toString();
  }

  /**
   * 通过反射调用Oracle连接的setSessionTimeZone方法，设置会话时区
   * 用于正确处理Oracle的TIMESTAMP WITH LOCAL TIME ZONE类型字段
   * @param conf Hadoop配置，从中读取用户配置的时区值
   * @param conn Oracle数据库连接
   * @throws SQLException 反射调用失败或找不到方法时抛出
   */
  public static void setSessionTimeZone(Configuration conf,
      Connection conn) throws SQLException {
    // 通过反射调用OracleConnection的setSessionTimeZone方法，避免编译依赖Oracle驱动
    Method method;
    try {
      method = conn.getClass().getMethod(
              "setSessionTimeZone", new Class [] {String.class});
    } catch (Exception ex) {
      LOG.error("Could not find method setSessionTimeZone in " + conn.getClass().getName(), ex);
      // 包装异常为SQLException抛出
      throw new SQLException(ex);
    }

    // 从配置读取时区，默认使用GMT
    String clientTimeZone = conf.get(SESSION_TIMEZONE_KEY, "GMT");
    try {
      method.setAccessible(true);
      method.invoke(conn, clientTimeZone);
      LOG.info("Time zone has been set to " + clientTimeZone);
    } catch (Exception ex) {
      LOG.warn("Time zone " + clientTimeZone +
               " could not be set on Oracle database.");
      LOG.warn("Setting default time zone: GMT");
      try {
        // GMT时区一定存在，降级使用默认GMT
        method.invoke(conn, "GMT");
      } catch (Exception ex2) {
        LOG.error("Could not set time zone for oracle connection", ex2);
        // 仍然失败，抛出异常
        throw new SQLException(ex);
      }
    }
  }
}