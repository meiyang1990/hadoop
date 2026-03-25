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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.Progressable;

/**
 * 文件说明：MapReduce旧API框架下的数据库输出格式实现类，将MapReduce计算结果写入关系型数据库
 * 核心职责：为旧版MapReduce API提供将计算结果批量写入关系型数据库的能力，继承新版DBOutputFormat实现适配
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DBOutputFormat<K  extends DBWritable, V> 
    extends org.apache.hadoop.mapreduce.lib.db.DBOutputFormat<K, V>
    implements OutputFormat<K, V> {

  /**
   * 将MapReduce输出写入SQL表的记录写入器实现，适配旧版MapReduce API
   */
  protected class DBRecordWriter extends 
      org.apache.hadoop.mapreduce.lib.db.DBOutputFormat<K, V>.DBRecordWriter
      implements RecordWriter<K, V> {

    protected DBRecordWriter(Connection connection, 
      PreparedStatement statement) throws SQLException {
      super(connection, statement);
    }

    /** {@inheritDoc} */
    public void close(Reporter reporter) throws IOException {
      super.close(null);
    }
  }

  /**
   * 检查输出规格是否合法，此处无额外检查逻辑
   */
  public void checkOutputSpecs(FileSystem filesystem, JobConf job)
  throws IOException {
  }


  /**
   * 获取用于写入数据库的记录写入器实例
   * @return 适配旧API的数据库记录写入器
   * @throws IOException 获取写入器失败时抛出
   */
  public RecordWriter<K, V> getRecordWriter(FileSystem filesystem,
      JobConf job, String name, Progressable progress) throws IOException {
    // 从JobConf获取任务尝试ID，创建新版API任务尝试上下文
    org.apache.hadoop.mapreduce.RecordWriter<K, V> w = super.getRecordWriter(
      new TaskAttemptContextImpl(job, 
            TaskAttemptID.forName(job.get(MRJobConfig.TASK_ATTEMPT_ID))));
    // 将新版写入器强制转型
    org.apache.hadoop.mapreduce.lib.db.DBOutputFormat.DBRecordWriter writer = 
     (org.apache.hadoop.mapreduce.lib.db.DBOutputFormat.DBRecordWriter) w;
    try {
      // 复用新版连接和预处理语句，构造旧API写入器实例返回
      return new DBRecordWriter(writer.getConnection(), writer.getStatement());
    } catch(SQLException se) {
      throw new IOException(se);
    }
  }

  /**
   * 初始化Job输出配置，指定写入的数据库表和字段名
   * @param job MapReduce作业配置对象
   * @param tableName 目标表名称，数据将插入到该表中
   * @param fieldNames 目标表中需要插入的字段名称数组
   */
  public static void setOutput(JobConf job, String tableName, String... fieldNames) {
    if(fieldNames.length > 0 && fieldNames[0] != null) {
      DBConfiguration dbConf = setOutput(job, tableName);
      dbConf.setOutputFieldNames(fieldNames);
    } else {
      if(fieldNames.length > 0)
        setOutput(job, tableName, fieldNames.length);
      else 
        throw new IllegalArgumentException("Field names must be greater than 0");
    }
  }
  
  /**
   * 初始化Job输出配置，仅指定写入字段数量，不指定字段名（用于生成占位符）
   * @param job MapReduce作业配置对象
   * @param tableName 目标表名称，数据将插入到该表中
   * @param fieldCount 目标表中需要插入的字段数量
   */
  public static void setOutput(JobConf job, String tableName, int fieldCount) {
    DBConfiguration dbConf = setOutput(job, tableName);
    dbConf.setOutputFieldCount(fieldCount);
  }
  
  /**
   * 通用输出配置初始化，设置输出格式、禁用reduce推测执行，设置目标表名
   * @param job MapReduce作业配置对象
   * @param tableName 目标表名称
   * @return 初始化后的数据库配置对象
   */
  private static DBConfiguration setOutput(JobConf job, String tableName) {
    job.setOutputFormat(DBOutputFormat.class);
    job.setReduceSpeculativeExecution(false);

    DBConfiguration dbConf = new DBConfiguration(job);
    
    dbConf.setOutputTableName(tableName);
    return dbConf;
  }
  
}