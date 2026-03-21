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
package org.apache.hadoop.yarn.server.timelineservice.storage.application;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineHBaseSchemaConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 应用信息表读写工具类，负责应用表的创建和schema定义，为YARN时间线服务存储应用元数据提供底层支持。
 * 应用表用于存储YARN应用的基本信息、配置信息和指标数据。
 */
public class ApplicationTableRW extends BaseTableRW<ApplicationTable> {
  /** 配置项前缀. */
  private static final String PREFIX =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "application";

  /** 配置项名称：应用表的表名. */
  public static final String TABLE_NAME_CONF_NAME = PREFIX + ".table.name";

  /**
   * 配置项名称：应用表中指标列族的TTL（生存时间）。
   */
  private static final String METRICS_TTL_CONF_NAME = PREFIX
      + ".table.metrics.ttl";

  /**
   * 配置项名称：应用表中指标列族的最大版本数。
   */
  private static final String METRICS_MAX_VERSIONS =
      PREFIX + ".table.metrics.max-versions";

  /** 应用表默认名称. */
  private static final String DEFAULT_TABLE_NAME =
      "timelineservice.application";

  /** 指标默认TTL：30天，换算为秒. */
  private static final int DEFAULT_METRICS_TTL = 2592000;

  /** 指标默认最大版本数. */
  private static final int DEFAULT_METRICS_MAX_VERSIONS = 10000;

  private static final Logger LOG =
      LoggerFactory.getLogger(ApplicationTableRW.class);

  /**
   * 构造函数，指定表名配置项和默认表名。
   */
  public ApplicationTableRW() {
    super(TABLE_NAME_CONF_NAME, DEFAULT_TABLE_NAME);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.BaseTableRW#
   * createTable(org.apache.hadoop.hbase.client.Admin,
   * org.apache.hadoop.conf.Configuration)
   */
  /**
   * 根据配置创建应用信息HBase表，包含列族定义和预分区设置。
   * @param admin HBase管理员客户端
   * @param hbaseConf HBase配置
   * @throws IOException 表已存在或创建失败时抛出异常
   */
  public void createTable(Admin admin, Configuration hbaseConf)
      throws IOException {

    // 从配置获取表名
    TableName table = getTableName(hbaseConf);
    // 表已存在则抛出异常，不覆盖已有表
    if (admin.tableExists(table)) {
      // do not disable / delete existing table
      // similar to the approach taken by map-reduce jobs when
      // output directory exists
      throw new IOException("Table " + table.getNameAsString()
          + " already exists.");
    }

    // 创建表描述符
    HTableDescriptor applicationTableDescp = new HTableDescriptor(table);
    // 创建信息列族，存储应用基本信息
    HColumnDescriptor infoCF =
        new HColumnDescriptor(ApplicationColumnFamily.INFO.getBytes());
    // 设置布隆过滤器类型：行+列
    infoCF.setBloomFilterType(BloomType.ROWCOL);
    // 添加列族到表
    applicationTableDescp.addFamily(infoCF);

    // 创建配置列族，存储应用配置信息
    HColumnDescriptor configCF =
        new HColumnDescriptor(ApplicationColumnFamily.CONFIGS.getBytes());
    // 设置布隆过滤器类型：行+列
    configCF.setBloomFilterType(BloomType.ROWCOL);
    // 开启块缓存，提升查询性能
    configCF.setBlockCacheEnabled(true);
    // 添加列族到表
    applicationTableDescp.addFamily(configCF);

    // 创建指标列族，存储应用指标数据
    HColumnDescriptor metricsCF =
        new HColumnDescriptor(ApplicationColumnFamily.METRICS.getBytes());
    // 添加列族到表
    applicationTableDescp.addFamily(metricsCF);
    // 开启块缓存，提升指标查询性能
    metricsCF.setBlockCacheEnabled(true);
    // always keep 1 version (the latest)
    // 设置最小保留版本数为1
    metricsCF.setMinVersions(1);
    // 从配置读取最大版本数，使用默认值作为兜底
    metricsCF.setMaxVersions(
        hbaseConf.getInt(METRICS_MAX_VERSIONS, DEFAULT_METRICS_MAX_VERSIONS));
    // 从配置读取TTL，使用默认值作为兜底
    metricsCF.setTimeToLive(hbaseConf.getInt(METRICS_TTL_CONF_NAME,
        DEFAULT_METRICS_TTL));
    // 设置按用户名前缀分割的分区策略，实现数据按用户隔离划分
    applicationTableDescp.setRegionSplitPolicyClassName(
        "org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy");
    // 设置前缀长度，用于分区策略分割键
    applicationTableDescp.setValue("KeyPrefixRegionSplitPolicy.prefix_length",
        TimelineHBaseSchemaConstants.USERNAME_SPLIT_KEY_PREFIX_LENGTH);
    // 按用户名前缀预分区创建表，提升写入性能
    admin.createTable(applicationTableDescp,
        TimelineHBaseSchemaConstants.getUsernameSplits());
    LOG.info("Status of table creation for " + table.getNameAsString() + "="
        + admin.tableExists(table));
  }

  /**
   * 设置指标列族的TTL，写入配置对象。
   * @param metricsTTL 指标TTL值（秒）
   * @param hbaseConf 要修改的配置对象
   */
  public void setMetricsTTL(int metricsTTL, Configuration hbaseConf) {
    hbaseConf.setInt(METRICS_TTL_CONF_NAME, metricsTTL);
  }

}