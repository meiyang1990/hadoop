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
import java.sql.SQLException;
import java.sql.Types;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * Oracle数据库专用的数据驱动DBInputFormat实现，用于从Oracle数据库读取MapReduce输入数据。
 * 针对Oracle数据库的数据类型特性做了分片逻辑优化，特别是日期类型的分片处理。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OracleDataDrivenDBInputFormat<T extends DBWritable>
    extends DataDrivenDBInputFormat<T> implements Configurable {

  /**
   * 根据数据类型获取对应的分片器，针对Oracle日期类型提供专用实现。
   * @param sqlDataType 分片字段的SQL数据类型
   * @return 适配对应数据类型的分片器实例
   */
  @Override
  protected DBSplitter getSplitter(int sqlDataType) {
    switch (sqlDataType) {
    case Types.DATE:
    case Types.TIME:
    case Types.TIMESTAMP:
      return new OracleDateSplitter();

    default:
      return super.getSplitter(sqlDataType);
    }
  }

  /**
   * 创建Oracle专用的数据库记录读取器，使用Oracle特定的实现读取数据。
   * @param split 数据库分片信息
   * @param conf 作业配置对象
   * @return Oracle专用的记录读取器实例
   * @throws IOException 创建连接或读取配置失败时抛出
   */
  @Override
  protected RecordReader<LongWritable, T> createDBRecordReader(DBInputSplit split,
      Configuration conf) throws IOException {

    DBConfiguration dbConf = getDBConf();
    @SuppressWarnings("unchecked")
    Class<T> inputClass = (Class<T>) (dbConf.getInputClass());

    try {
      // 使用Oracle专用的记录读取器实现
      return new OracleDataDrivenDBRecordReader<T>(split, inputClass,
          conf, createConnection(), dbConf, dbConf.getInputConditions(),
          dbConf.getInputFieldNames(), dbConf.getInputTableName());
    } catch (SQLException ex) {
      throw new IOException(ex.getMessage());
    }
  }
}