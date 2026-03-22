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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * 文件说明：数据驱动型数据库输入格式，用于从SQL表读取数据作为MapReduce输入
 * 核心功能：不同于DBInputFormat使用LIMIT/OFFSET划分分片，该类通过生成WHERE条件
 *          将数据划分为近似等规模的数据分片，支持并行读取数据库数据
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DataDrivenDBInputFormat<T extends DBWritable>
    extends DBInputFormat<T> implements Configurable {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataDrivenDBInputFormat.class);

  /** 用户自定义查询中占位符，将被替换为分片范围条件，实现并行分片读取 */
  public static final String SUBSTITUTE_TOKEN = "$CONDITIONS";

  /**
   * 类说明：数据驱动型数据库分片，代表一组数据行范围的输入分片
   */
  @InterfaceStability.Evolving
  public static class DataDrivenDBInputSplit extends DBInputFormat.DBInputSplit {

    private String lowerBoundClause;
    private String upperBoundClause;

    /**
     * 默认构造函数
     */
    public DataDrivenDBInputSplit() {
    }

    /**
     * 构造函数，根据上下边界条件创建分片
     * @param lower WHERE子句中的下限条件字符串
     * @param upper WHERE子句中的上限条件字符串
     */
    public DataDrivenDBInputSplit(final String lower, final String upper) {
      this.lowerBoundClause = lower;
      this.upperBoundClause = upper;
    }


    /**
     * 获取当前分片总行数，无法提前确定所以返回0
     * @return 0 表示未知总行数
     */
    public long getLength() throws IOException {
      return 0; // unfortunately, we don't know this.
    }

    /** {@inheritDoc} */
    public void readFields(DataInput input) throws IOException {
      this.lowerBoundClause = Text.readString(input);
      this.upperBoundClause = Text.readString(input);
    }

    /** {@inheritDoc} */
    public void write(DataOutput output) throws IOException {
      Text.writeString(output, this.lowerBoundClause);
      Text.writeString(output, this.upperBoundClause);
    }

    public String getLowerClause() {
      return lowerBoundClause;
    }

    public String getUpperClause() {
      return upperBoundClause;
    }
  }

  /**
   * 根据SQL数据类型获取对应的数据分片器实现
   * @param sqlDataType JDBC SQL数据类型常量
   * @return 对应类型的分片器实例，不支持的类型返回null
   */
  protected DBSplitter getSplitter(int sqlDataType) {
    switch (sqlDataType) {
    case Types.NUMERIC:
    case Types.DECIMAL:
      return new BigDecimalSplitter();

    case Types.BIT:
    case Types.BOOLEAN:
      return new BooleanSplitter();

    case Types.INTEGER:
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.BIGINT:
      return new IntegerSplitter();

    case Types.REAL:
    case Types.FLOAT:
    case Types.DOUBLE:
      return new FloatSplitter();

    case Types.CHAR:
    case Types.VARCHAR:
    case Types.LONGVARCHAR:
      return new TextSplitter();

    case Types.DATE:
    case Types.TIME:
    case Types.TIMESTAMP:
      return new DateSplitter();

    default:
      // TODO: Support BINARY, VARBINARY, LONGVARBINARY, DISTINCT, CLOB, BLOB, ARRAY
      // STRUCT, REF, DATALINK, and JAVA_OBJECT.
      return null;
    }
  }

  /** {@inheritDoc} */
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    // 从配置获取目标map任务数量
    int targetNumTasks = job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
    if (1 == targetNumTasks) {
      // 只需要一个分片，无需查询边界，直接返回全表分片，对于无索引大表更高效
      List<InputSplit> singletonSplit = new ArrayList<InputSplit>();
      singletonSplit.add(new DataDrivenDBInputSplit("1=1", "1=1"));
      return singletonSplit;
    }

    ResultSet results = null;
    Statement statement = null;
    try {
      // 创建数据库语句对象
      statement = connection.createStatement();

      // 执行查询获取拆分列的最大最小值
      results = statement.executeQuery(getBoundingValsQuery());
      results.next();

      // 根据拆分列的数据类型获取对应分片器
      int sqlDataType = results.getMetaData().getColumnType(1);
      DBSplitter splitter = getSplitter(sqlDataType);
      if (null == splitter) {
        throw new IOException("Unknown SQL data type: " + sqlDataType);
      }

      // 调用分片器生成分片列表返回
      return splitter.split(job.getConfiguration(), results, getDBConf().getInputOrderBy());
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    } finally {
      // 关闭结果集，忽略异常仅记录日志
      try {
        if (null != results) {
          results.close();
        }
      } catch (SQLException se) {
        LOG.debug("SQLException closing resultset: " + se.toString());
      }
      // 关闭语句对象，忽略异常仅记录日志
      try {
        if (null != statement) {
          statement.close();
        }
      } catch (SQLException se) {
        LOG.debug("SQLException closing statement: " + se.toString());
      }
      // 提交事务并关闭连接，忽略异常仅记录日志
      try {
        connection.commit();
        closeConnection();
      } catch (SQLException se) {
        LOG.debug("SQLException committing split transaction: " + se.toString());
      }
    }
  }

  /**
   * 生成查询拆分列最小最大值的SQL语句
   * @return 包含拆分列MIN和MAX结果的SQL查询语句
   */
  protected String getBoundingValsQuery() {
    // 如果用户自定义了边界查询，直接返回用户定义
    String userQuery = getDBConf().getInputBoundingQuery();
    if (null != userQuery) {
      return userQuery;
    }

    // 根据表名自动生成边界查询语句
    StringBuilder query = new StringBuilder();

    String splitCol = getDBConf().getInputOrderBy();
    query.append("SELECT MIN(").append(splitCol).append("), ");
    query.append("MAX(").append(splitCol).append(") FROM ");
    query.append(getDBConf().getInputTableName());
    String conditions = getDBConf().getInputConditions();
    if (null != conditions) {
      query.append(" WHERE ( " + conditions + " )");
    }

    return query.toString();
  }

  /**
   * 设置用户自定义边界查询，用于分片计算拆分列的最大最小值
   * 用户自定义查询必须包含占位符$CONDITIONS，分片条件会替换该占位符
   * @param conf 作业配置对象
   * @param query 用户定义的边界查询语句
   */
  public static void setBoundingQuery(Configuration conf, String query) {
    if (null != query) {
      // 如果查询中没有占位符，输出警告提示用户
      if (query.indexOf(SUBSTITUTE_TOKEN) == -1) {
        LOG.warn("Could not find " + SUBSTITUTE_TOKEN + " token in query: " + query
            + "; splits may not partition data.");
      }
    }

    conf.set(DBConfiguration.INPUT_BOUNDING_QUERY, query);
  }

  /**
   * 创建对应数据库类型的数据记录读取器
   * @param split 输入分片对象
   * @param conf 作业配置对象
   * @return 适用于当前数据库的记录读取器实例
   * @throws IOException 创建失败抛出异常
   */
  protected RecordReader<LongWritable, T> createDBRecordReader(DBInputSplit split,
      Configuration conf) throws IOException {

    DBConfiguration dbConf = getDBConf();
    @SuppressWarnings("unchecked")
    Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
    String dbProductName = getDBProductName();

    LOG.debug("Creating db record reader for db product: " + dbProductName);

    try {
      // 根据数据库产品类型选择对应读取器实现
      if (dbProductName.startsWith("MYSQL")) {
        // 使用MySQL特定读取器
        return new MySQLDataDrivenDBRecordReader<T>(split, inputClass,
            conf, createConnection(), dbConf, dbConf.getInputConditions(),
            dbConf.getInputFieldNames(), dbConf.getInputTableName());
      } else {
        // 使用通用读取器
        return new DataDrivenDBRecordReader<T>(split, inputClass,
            conf, createConnection(), dbConf, dbConf.getInputConditions(),
            dbConf.getInputFieldNames(), dbConf.getInputTableName(),
            dbProductName);
      }
    } catch (SQLException ex) {
      throw new IOException(ex.getMessage());
    }
  }

  // Configuration methods override superclass to ensure that the proper
  // DataDrivenDBInputFormat gets used.

  /**
   * 配置作业输入，基于表名进行分片，指定拆分列
   * 此处拆分列对应于DBInputFormat中的排序列，本质用于数据划分而非排序
   * @param job 作业对象
   * @param inputClass DBWritable实现类，用于反序列化数据行
   * @param tableName 数据库表名
   * @param conditions 数据过滤条件
   * @param splitBy 用于划分分片的列名
   * @param fieldNames 需要读取的字段名列表
   */
  public static void setInput(Job job, 
      Class<? extends DBWritable> inputClass,
      String tableName,String conditions, 
      String splitBy, String... fieldNames) {
    DBInputFormat.setInput(job, inputClass, tableName, conditions, splitBy, fieldNames);
    job.setInputFormatClass(DataDrivenDBInputFormat.class);
  }

  /**
   * 配置作业输入，使用用户自定义查询和自定义边界查询
   * @param job 作业对象
   * @param inputClass DBWritable实现类，用于反序列化数据行
   * @param inputQuery 用户自定义数据查询
   * @param inputBoundingQuery 获取拆分列最大最小值的边界查询
   */
  public static void setInput(Job job,
      Class<? extends DBWritable> inputClass,
      String inputQuery, String inputBoundingQuery) {
    DBInputFormat.setInput(job, inputClass, inputQuery, "");
    job.getConfiguration().set(DBConfiguration.INPUT_BOUNDING_QUERY, inputBoundingQuery);
    job.setInputFormatClass(DataDrivenDBInputFormat.class);
  }
}