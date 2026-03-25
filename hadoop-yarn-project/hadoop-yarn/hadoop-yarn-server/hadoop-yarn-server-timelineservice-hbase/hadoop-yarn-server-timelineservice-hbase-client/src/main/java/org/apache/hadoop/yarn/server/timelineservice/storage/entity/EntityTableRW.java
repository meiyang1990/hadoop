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
package org.apache.hadoop.yarn.server.timelineservice.storage.entity;

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
 * 实体表读写工具类，负责HBase中实体表的创建、配置管理，用于存储时间线服务的实体数据。
 * 继承自BaseTableRW提供基础表读写能力，针对实体表做专属配置。
 */
public class EntityTableRW extends BaseTableRW<EntityTable> {
  /** 配置键前缀。 */
  private static final String PREFIX =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "entity";

  /** 实体表表名配置项名称。 */
  public static final String TABLE_NAME_CONF_NAME = PREFIX + ".table.name";

  /**
   * 实体表中指标列族的TTL配置项名称。
   */
  private static final String METRICS_TTL_CONF_NAME = PREFIX
      + ".table.metrics.ttl";

  /**
   * 实体表中指标列族的最大版本数配置项名称。
   */
  private static final String METRICS_MAX_VERSIONS =
      PREFIX + ".table.metrics.max-versions";

  /** 实体表默认表名。 */
  public static final String DEFAULT_TABLE_NAME = "timelineservice.entity";

  /** 指标默认TTL为30天。 */
  private static final int DEFAULT_METRICS_TTL = 2592000;

  /** 指标默认最大版本数。 */
  private static final int DEFAULT_METRICS_MAX_VERSIONS = 10000;

  private static final Logger LOG =
      LoggerFactory.getLogger(EntityTableRW.class);

  /**
   * 构造方法，传入表名配置和默认表名初始化父类。
   */
  public EntityTableRW() {
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
   * 根据配置创建实体HBase表。
   * @param admin HBase管理员客户端
   * @param hbaseConf HBase配置
   * @throws IOException 如果表已存在或创建失败抛出异常
   */
  public void createTable(Admin admin, Configuration hbaseConf)
      throws IOException {

    // 从配置获取表名
    TableName table = getTableName(hbaseConf);
    // 如果表已存在，抛出异常避免覆盖
    if (admin.tableExists(table)) {
      // do not disable / delete existing table
      // similar to the approach taken by map-reduce jobs when
      // output directory exists
      throw new IOException("Table " + table.getNameAsString()
          + " already exists.");
    }

    // 创建表描述符
    HTableDescriptor entityTableDescp = new HTableDescriptor(table);
    // 创建info列族，设置行列级布隆过滤器
    HColumnDescriptor infoCF =
        new HColumnDescriptor(EntityColumnFamily.INFO.getBytes());
    infoCF.setBloomFilterType(BloomType.ROWCOL);
    entityTableDescp.addFamily(infoCF);

    // 创建configs列族，开启块缓存，设置行列级布隆过滤器
    HColumnDescriptor configCF =
        new HColumnDescriptor(EntityColumnFamily.CONFIGS.getBytes());
    configCF.setBloomFilterType(BloomType.ROWCOL);
    configCF.setBlockCacheEnabled(true);
    entityTableDescp.addFamily(configCF);

    // 创建metrics列族，开启块缓存
    HColumnDescriptor metricsCF =
        new HColumnDescriptor(EntityColumnFamily.METRICS.getBytes());
    entityTableDescp.addFamily(metricsCF);
    metricsCF.setBlockCacheEnabled(true);
    // always keep 1 version (the latest)
    metricsCF.setMinVersions(1);
    // 从配置读取设置最大版本数
    metricsCF.setMaxVersions(
        hbaseConf.getInt(METRICS_MAX_VERSIONS, DEFAULT_METRICS_MAX_VERSIONS));
    // 从配置读取设置TTL
    metricsCF.setTimeToLive(hbaseConf.getInt(METRICS_TTL_CONF_NAME,
        DEFAULT_METRICS_TTL));
    // 设置按用户名前缀分割的分区策略，按用户划分region提高查询性能
    entityTableDescp.setRegionSplitPolicyClassName(
        "org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy");
    entityTableDescp.setValue("KeyPrefixRegionSplitPolicy.prefix_length",
        TimelineHBaseSchemaConstants.USERNAME_SPLIT_KEY_PREFIX_LENGTH);
    // 按照用户名预分区创建表
    admin.createTable(entityTableDescp,
        TimelineHBaseSchemaConstants.getUsernameSplits());
    LOG.info("Status of table creation for " + table.getNameAsString() + "="
        + admin.tableExists(table));
  }

  /**
   * 设置指标列族TTL到配置中。
   * @param metricsTTL 指标TTL值
   * @param hbaseConf 要修改的配置对象
   */
  public void setMetricsTTL(int metricsTTL, Configuration hbaseConf) {
    hbaseConf.setInt(METRICS_TTL_CONF_NAME, metricsTTL);
  }

}