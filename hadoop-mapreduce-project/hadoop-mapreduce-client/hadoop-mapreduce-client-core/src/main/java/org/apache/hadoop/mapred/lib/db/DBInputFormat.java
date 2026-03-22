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
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;

/**
 * 从关系型数据库读取数据的MapReduce旧API输入格式实现
 * 支持将数据库表数据分片后并行读取，作为Map任务的输入
 * 继承新版本API实现，适配旧版MapReduce接口
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@SuppressWarnings("deprecation")
public class DBInputFormat<T  extends DBWritable>
    extends org.apache.hadoop.mapreduce.lib.db.DBInputFormat<T> 
    implements InputFormat<LongWritable, T>, JobConfigurable {
  /**
   * 从SQL表读取记录的RecordReader实现
   * 输出key为记录编号（LongWritable），value为实现DBWritable的记录对象
   */
  protected class DBRecordReader extends
      org.apache.hadoop.mapreduce.lib.db.DBRecordReader<T>
      implements RecordReader<LongWritable, T> {
    /**
     * 兼容旧版MapReduce 1.x的构造方法
     *
     * @param split 要读取数据的输入分片
     * @throws SQLException 数据库访问异常
     */
    protected DBRecordReader(DBInputSplit split, Class<T> inputClass,
        JobConf job) throws SQLException {
      super(split, inputClass, job, connection, dbConf, conditions, fieldNames, tableName);
    }

    /**
     * 完整构造方法
     * @param split 要读取数据的输入分片
     * @throws SQLException 数据库访问异常
     */
    protected DBRecordReader(DBInputSplit split, Class<T> inputClass, 
        JobConf job, Connection conn, DBConfiguration dbConfig, String cond,
        String [] fields, String table) throws SQLException {
      super(split, inputClass, job, conn, dbConfig, cond, fields, table);
    }

    /** {@inheritDoc} */
    public LongWritable createKey() {
      return new LongWritable();  
    }

    /** {@inheritDoc} */
    public T createValue() {
      return super.createValue();
    }

    public long getPos() throws IOException {
      return super.getPos();
    }

    /** {@inheritDoc} */
    public boolean next(LongWritable key, T value) throws IOException {
      return super.next(key, value);
    }
  }

  /**
   * 适配新旧API的RecordReader包装类
   * 将新版本API的DBRecordReader转换为旧版MapReduce接口要求的实现
   */
  private static class DBRecordReaderWrapper<T extends DBWritable>
      implements RecordReader<LongWritable, T> {

    private org.apache.hadoop.mapreduce.lib.db.DBRecordReader<T> rr;
    
    /**
     * 构造包装器，包装新版本API的RecordReader
     * @param inner 新版本API的DBRecordReader实例
     */
    public DBRecordReaderWrapper(
        org.apache.hadoop.mapreduce.lib.db.DBRecordReader<T> inner) {
      this.rr = inner;
    }

    public void close() throws IOException {
      rr.close();
    }

    public LongWritable createKey() {
      return new LongWritable();
    }

    public T createValue() {
      return rr.createValue();
    }

    public float getProgress() throws IOException {
      return rr.getProgress();
    }
    
    public long getPos() throws IOException {
      return rr.getPos();
    }

    public boolean next(LongWritable key, T value) throws IOException {
      return rr.next(key, value);
    }
  }

  /**
   * 空实现的DBWritable类，用于占位场景
   */
  public static class NullDBWritable extends 
      org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable 
      implements DBWritable, Writable {
  }
  
  /**
   * 对应数据库查询结果范围的输入分片，包含一组连续行
   */
  protected static class DBInputSplit extends 
      org.apache.hadoop.mapreduce.lib.db.DBInputFormat.DBInputSplit 
      implements InputSplit {
    /**
     * 默认构造方法
     */
    public DBInputSplit() {
    }

    /**
     * 构造指定行范围的分片
     * @param start 第一个待读取行的索引
     * @param end 最后一个待读取行的索引
     */
    public DBInputSplit(long start, long end) {
      super(start, end);
    }
  }

  /** {@inheritDoc} */
  public void configure(JobConf job) {
    super.setConf(job);
  }

  /** {@inheritDoc} */
  public RecordReader<LongWritable, T> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {

    // 使用包装类适配新旧API差异
    return new DBRecordReaderWrapper<T>(
        (org.apache.hadoop.mapreduce.lib.db.DBRecordReader<T>) 
        createDBRecordReader(
          (org.apache.hadoop.mapreduce.lib.db.DBInputFormat.DBInputSplit) split, job));
  }

  /** {@inheritDoc} */
  public InputSplit[] getSplits(JobConf job, int chunks) throws IOException {
    // 调用新版本API获取分片列表
    List<org.apache.hadoop.mapreduce.InputSplit> newSplits = 
      super.getSplits(Job.getInstance(job));
    // 转换为旧版API分片数组返回
    InputSplit[] ret = new InputSplit[newSplits.size()];
    int i = 0;
    for (org.apache.hadoop.mapreduce.InputSplit s : newSplits) {
      org.apache.hadoop.mapreduce.lib.db.DBInputFormat.DBInputSplit split = 
    	(org.apache.hadoop.mapreduce.lib.db.DBInputFormat.DBInputSplit)s;
      ret[i++] = new DBInputSplit(split.getStart(), split.getEnd());
    }
    return ret;
  }

  /**
   * 初始化作业数据库输入配置，通过指定表、条件、排序和字段读取数据
   * 
   * @param job 作业配置对象
   * @param inputClass 实现DBWritable的记录类型，用于保存数据库行数据
   * @param tableName 待读取的数据库表名
   * @param conditions 查询条件，对应SQL的WHERE子句，例如 '(updated > 20070101 AND length > 0)'
   * @param orderBy 排序字段，对应SQL的ORDER BY子句
   * @param fieldNames 待读取的表字段名列表
   */
  public static void setInput(JobConf job, Class<? extends DBWritable> inputClass,
      String tableName,String conditions, String orderBy, String... fieldNames) {
    job.setInputFormat(DBInputFormat.class);

    DBConfiguration dbConf = new DBConfiguration(job);
    dbConf.setInputClass(inputClass);
    dbConf.setInputTableName(tableName);
    dbConf.setInputFieldNames(fieldNames);
    dbConf.setInputConditions(conditions);
    dbConf.setInputOrderBy(orderBy);
  }
  
  /**
   * 初始化作业数据库输入配置，通过自定义SQL查询读取数据
   * 
   * @param job 作业配置对象
   * @param inputClass 实现DBWritable的记录类型，用于保存数据库行数据
   * @param inputQuery 数据查询SQL，例如 "SELECT f1, f2, f3 FROM Mytable ORDER BY f1"
   * @param inputCountQuery 统计总行数的SQL，用于分片计算，例如 "SELECT COUNT(f1) FROM Mytable"
   */
  public static void setInput(JobConf job, Class<? extends DBWritable> inputClass,
      String inputQuery, String inputCountQuery) {
    job.setInputFormat(DBInputFormat.class);
    
    DBConfiguration dbConf = new DBConfiguration(job);
    dbConf.setInputClass(inputClass);
    dbConf.setInputQuery(inputQuery);
    dbConf.setInputCountQuery(inputCountQuery);
    
  }
}