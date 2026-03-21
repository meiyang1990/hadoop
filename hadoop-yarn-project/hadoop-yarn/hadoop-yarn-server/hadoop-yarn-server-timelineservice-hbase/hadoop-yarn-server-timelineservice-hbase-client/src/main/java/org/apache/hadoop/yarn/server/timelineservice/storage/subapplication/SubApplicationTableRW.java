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
package org.apache.hadoop.yarn.server.timelineservice.storage.subapplication;

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
 * 子应用表的读写操作实现类，负责该表的创建与配置管理。
 * 子应用表用于存储YARN Timeline服务中子应用的元数据与指标信息。
 */
public class SubApplicationTableRW extends BaseTableRW<SubApplicationTable> {
  /** 配置前缀。 */
  private static final String PREFIX =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "subapplication";

  /** 配置项：子应用表名称。 */
  public static final String TABLE_NAME_CONF_NAME = PREFIX + ".table.name";

  /**
   * 配置项：子应用表中指标列族的TTL（生存时间）。
   */
  private static final String METRICS_TTL_CONF_NAME = PREFIX
      + ".table.metrics.ttl";

  /**
   * 配置项：子应用表中指标列族的最大版本数。
   */
  private static final String METRICS_MAX_VERSIONS =
      PREFIX + ".table.metrics.max-versions";

  /** 默认值：子应用表名称。 */
  public static final String DEFAULT_TABLE_NAME =
      "timelineservice.subapplication";

  /** 默认值：指标TTL，单位秒，默认30天。 */
  private static final int DEFAULT_METRICS_TTL = 2592000;

  /** 默认值：指标最大版本数。 */
  private static final int DEFAULT_METRICS_MAX_VERSIONS = 10000;

  private static final Logger LOG = LoggerFactory.getLogger(
      SubApplicationTableRW.class);

  /**
   * 构造函数，初始化子应用表读写工具。
   */
  public SubApplicationTableRW() {
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
   * 在HBase中创建子应用表，根据配置初始化表结构和列族。
   * @param admin HBase管理员客户端
   * @param hbaseConf HBase配置
   * @throws IOException 创建表失败时抛出异常
   */
  public void createTable(Admin admin, Configuration hbaseConf)
      throws IOException {

    // 获取表名
    TableName table = getTableName(hbaseConf);
    // 表已存在则抛出异常避免覆盖
    if (admin.tableExists(table)) {
      throw new IOException("Table " + table.getNameAsString()
          + " already exists.");
    }

    // 创建表描述符
    HTableDescriptor subAppTableDescp = new HTableDescriptor(table);
    // 创建INFO信息列族
    HColumnDescriptor infoCF =
        new HColumnDescriptor(SubApplicationColumnFamily.INFO.getBytes());
    // 设置布隆过滤器类型为行+列
    infoCF.setBloomFilterType(BloomType.ROWCOL);
    // 添加列族到表
    subAppTableDescp.addFamily(infoCF);

    // 创建CONFIGS配置列族
    HColumnDescriptor configCF =
        new HColumnDescriptor(SubApplicationColumnFamily.CONFIGS.getBytes());
    configCF.setBloomFilterType(BloomType.ROWCOL);
    // 开启块缓存
    configCF.setBlockCacheEnabled(true);
    subAppTableDescp.addFamily(configCF);

    // 创建METRICS指标列族
    HColumnDescriptor metricsCF =
        new HColumnDescriptor(SubApplicationColumnFamily.METRICS.getBytes());
    subAppTableDescp.addFamily(metricsCF);
    metricsCF.setBlockCacheEnabled(true);
    // 最小保留1个版本
    metricsCF.setMinVersions(1);
    // 从配置读取最大版本数，使用默认值兜底
    metricsCF.setMaxVersions(
        hbaseConf.getInt(METRICS_MAX_VERSIONS, DEFAULT_METRICS_MAX_VERSIONS));
    // 从配置读取TTL，使用默认值兜底
    metricsCF.setTimeToLive(hbaseConf.getInt(METRICS_TTL_CONF_NAME,
        DEFAULT_METRICS_TTL));
    // 设置基于前缀的分区分裂策略，按用户名前缀分割区域
    subAppTableDescp.setRegionSplitPolicyClassName(
        "org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy");
    // 设置前缀长度
    subAppTableDescp.setValue("KeyPrefixRegionSplitPolicy.prefix_length",
        TimelineHBaseSchemaConstants.USERNAME_SPLIT_KEY_PREFIX_LENGTH);
    // 创建表，按用户名预分区域
    admin.createTable(subAppTableDescp,
        TimelineHBaseSchemaConstants.getUsernameSplits());
    LOG.info("Status of table creation for " + table.getNameAsString() + "="
        + admin.tableExists(table));
  }

  /**
   * 设置指标TTL到配置中。
   * @param metricsTTL 指标生存时间（秒）
   * @param hbaseConf 要修改的配置对象
   */
  public void setMetricsTTL(int metricsTTL, Configuration hbaseConf) {
    hbaseConf.setInt(METRICS_TTL_CONF_NAME, metricsTTL);
  }

}