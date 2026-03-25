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

/**
 * Oracle数据库特定的DataDrivenDBRecordReader实现类，用于从Oracle数据库表中分片读取数据
 * 继承通用DataDrivenDBRecordReader，添加Oracle特有的会话时区初始化逻辑
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OracleDataDrivenDBRecordReader<T extends DBWritable>
    extends DataDrivenDBRecordReader<T> {

  /**
   * 构造Oracle数据驱动的DB记录读取器
   * @param split 输入分片，表示当前Reader需要读取的数据范围
   * @param inputClass 输出数据类型，必须实现DBWritable接口
   * @param conf Hadoop作业配置
   * @param conn 数据库连接对象
   * @param dbConfig 数据库输入配置
   * @param cond 查询条件语句
   * @param fields 需要读取的字段数组
   * @param table 要读取的表名
   * @throws SQLException 数据库操作异常时抛出
   */
  public OracleDataDrivenDBRecordReader(DBInputFormat.DBInputSplit split,
      Class<T> inputClass, Configuration conf, Connection conn,
      DBConfiguration dbConfig, String cond, String [] fields,
      String table) throws SQLException {

    super(split, inputClass, conf, conn, dbConfig, cond, fields, table,
        "ORACLE");

    // 必须为Oracle连接初始化会话时区，保证时间类型数据处理一致性
    OracleDBRecordReader.setSessionTimeZone(conf, conn);
  }
}