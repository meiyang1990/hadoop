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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * 时间线服务HBase存储层所有表读写操作的抽象基类，提供通用表操作能力，线程安全可并发使用。
 *
 * @param <T> 表实例类型，用于类型安全约束。
 */
public abstract class BaseTableRW<T extends BaseTable<T>> {

  /**
   * 配置项名称，用于指定本HBase表的名称。
   */
  private final String tableNameConfName;

  /**
   * 默认表名，若配置未指定则使用该默认值。
   */
  private final String defaultTableName;

  /**
   * 构造函数，初始化表名称配置项和默认表名。
   *
   * @param tableNameConfName 表名称配置项名称。
   * @param defaultTableName 未配置时使用的默认表名。
   */
  protected BaseTableRW(String tableNameConfName, String defaultTableName) {
    this.tableNameConfName = tableNameConfName;
    this.defaultTableName = defaultTableName;
  }

  /**
   * 获取当前表的类型安全批量写入器。
   *
   * @param hbaseConf HBase配置，用于读取表名称。
   * @param conn HBase连接，用于创建写入器。
   * @return 当前表的类型安全BufferedMutator。
   * @throws IOException 创建写入器过程中发生异常时抛出。
   */
  public TypedBufferedMutator<T> getTableMutator(Configuration hbaseConf,
      Connection conn) throws IOException {

    // 获取本HBase表的TableName对象
    TableName tableName = this.getTableName(hbaseConf);

    // 创建基础BufferedMutator
    BufferedMutator bufferedMutator = conn.getBufferedMutator(tableName);

    // 包装为类型安全的BufferedMutator
    TypedBufferedMutator<T> table =
        new TypedBufferedMutator<T>(bufferedMutator);

    return table;
  }

  /**
   * 根据扫描条件获取当前表的结果扫描器。
   *
   * @param hbaseConf HBase配置，用于读取表名称。
   * @param conn HBase连接，用于获取表对象。
   * @param scan 扫描条件，指定需要读取的数据范围。
   * @return 扫描结果扫描器。
   * @throws IOException 获取扫描器过程中发生异常时抛出。
   */
  public ResultScanner getResultScanner(Configuration hbaseConf,
      Connection conn, Scan scan) throws IOException {
    Table table = conn.getTable(getTableName(hbaseConf));
    return table.getScanner(scan);
  }

  /**
   * 根据Get查询获取单行结果。
   *
   * @param hbaseConf HBase配置，用于读取表名称。
   * @param conn HBase连接，用于获取表对象。
   * @param get Get查询条件，指定需要获取的单行数据。
   * @return Get查询返回的结果。
   * @throws IOException 获取结果过程中发生异常时抛出。
   */
  public Result getResult(Configuration hbaseConf, Connection conn, Get get)
      throws IOException {
    Table table = conn.getTable(getTableName(hbaseConf));
    return table.get(get);
  }

  /**
   * 拼接Schema前缀后，构造TableName对象。
   *
   * @param conf HBase配置，用于读取Schema前缀配置。
   * @param tableName 表名称。
   * @return 拼接Schema前缀后的完整TableName对象。
   */
  public static TableName getTableName(Configuration conf, String tableName) {
    String tableSchemaPrefix =  conf.get(
        YarnConfiguration.TIMELINE_SERVICE_HBASE_SCHEMA_PREFIX_NAME,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_HBASE_SCHEMA_PREFIX);
    return TableName.valueOf(tableSchemaPrefix + tableName);
  }

  /**
   * 获取当前表的TableName对象，从配置读取表名，不存在则使用默认值。
   *
   * @param conf HBase配置，用于读取表名称。
   * @return 当前表的TableName对象。
   */
  public TableName getTableName(Configuration conf) {
    String tableName = conf.get(tableNameConfName, defaultTableName);
    return getTableName(conf, tableName);
  }

  /**
   * 从配置读取表名，不存在则使用默认值，拼接Schema前缀后构造TableName对象。
   *
   * @param conf HBase配置，用于读取表名称和Schema前缀。
   * @param tableNameInConf 配置中的表名配置项。
   * @param defaultTableName 默认表名。
   * @return 拼接Schema前缀后的完整TableName对象。
   */
  public static TableName getTableName(Configuration conf,
      String tableNameInConf, String defaultTableName) {
    String tableName = conf.get(tableNameInConf, defaultTableName);
    return getTableName(conf, tableName);
  }

  /**
   * 在HBase中创建当前表，每个HBase实例只需要调用一次。
   *
   * @param admin HBase管理员客户端，用于执行表创建操作。
   * @param hbaseConf HBase配置。
   * @throws IOException 创建表过程中发生异常时抛出。
   */
  public abstract void createTable(Admin admin, Configuration hbaseConf)
      throws IOException;

}