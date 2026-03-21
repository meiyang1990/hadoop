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
package org.apache.hadoop.yarn.server.timelineservice.storage.flow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTableRW;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 流量活动HBase表的读写操作实现类，负责该表的创建及基础读写能力封装。
 */
public class FlowActivityTableRW extends BaseTableRW<FlowActivityTable> {
  /** 流量活动表名称前缀。 */
  private static final String PREFIX =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "flowactivity";

  /** 配置项名称，用于指定流量活动表的表名。 */
  public static final String TABLE_NAME_CONF_NAME = PREFIX + ".table.name";

  /** 流量活动表的默认表名。 */
  public static final String DEFAULT_TABLE_NAME =
      "timelineservice.flowactivity";

  private static final Logger LOG =
      LoggerFactory.getLogger(FlowActivityTableRW.class);

  /** 指标版本保留的默认最大数量，保留所有版本。 */
  public static final int DEFAULT_METRICS_MAX_VERSIONS = Integer.MAX_VALUE;

  /**
   * 构造函数，指定表名配置项和默认表名。
   */
  public FlowActivityTableRW() {
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
   * 创建流量活动HBase表，完成表结构和列族定义。
   * @param admin HBase管理员客户端
   * @param hbaseConf HBase配置
   * @throws IOException 创建表失败或表已存在时抛出异常
   */
  public void createTable(Admin admin, Configuration hbaseConf)
      throws IOException {

    // 从配置中获取表名
    TableName table = getTableName(hbaseConf);
    // 检查表是否已存在，存在则抛出异常避免覆盖
    if (admin.tableExists(table)) {
      // do not disable / delete existing table
      // similar to the approach taken by map-reduce jobs when
      // output directory exists
      throw new IOException("Table " + table.getNameAsString()
          + " already exists.");
    }

    // 初始化表描述符
    HTableDescriptor flowActivityTableDescp = new HTableDescriptor(table);
    // 创建INFO列族
    HColumnDescriptor infoCF =
        new HColumnDescriptor(FlowActivityColumnFamily.INFO.getBytes());
    // 设置布隆过滤器类型为ROWCOL，优化随机查询性能
    infoCF.setBloomFilterType(BloomType.ROWCOL);
    // 将INFO列族添加到表描述符
    flowActivityTableDescp.addFamily(infoCF);
    // 设置最小版本保留数为1
    infoCF.setMinVersions(1);
    // 设置最大版本保留数为Integer.MAX_VALUE，保留所有版本
    infoCF.setMaxVersions(DEFAULT_METRICS_MAX_VERSIONS);

    // TODO: figure the split policy before running in production
    // 执行表创建
    admin.createTable(flowActivityTableDescp);
    LOG.info("Status of table creation for " + table.getNameAsString() + "="
        + admin.tableExists(table));
  }
}