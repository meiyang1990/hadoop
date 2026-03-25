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
package org.apache.hadoop.yarn.server.timelineservice.storage.domain;


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
 * 时间线服务域表读写操作实现类，负责域表的创建、读写，基于HBase存储时间线服务的域元数据
 */
public class DomainTableRW extends BaseTableRW<DomainTable> {
  /** domain prefix. */
  private static final String PREFIX =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "domain";

  /** config param name that specifies the domain table name. */
  public static final String TABLE_NAME_CONF_NAME = PREFIX + ".table.name";

  /** default value for domain table name. */
  public static final String DEFAULT_TABLE_NAME = "timelineservice.domain";

  private static final Logger LOG =
      LoggerFactory.getLogger(DomainTableRW.class);

  /**
   * 构造函数，传入表名配置项和默认表名
   */
  public DomainTableRW() {
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
   * 在HBase中创建域表，完成表结构初始化和预分区
   * @param admin HBase管理员客户端
   * @param hbaseConf HBase配置
   * @throws IOException 创建失败时抛出异常
   */
  public void createTable(Admin admin, Configuration hbaseConf)
      throws IOException {

    // 从配置中获取表名
    TableName table = getTableName(hbaseConf);
    // 表已存在时抛出异常，避免覆盖已有数据
    if (admin.tableExists(table)) {
      // do not disable / delete existing table
      // similar to the approach taken by map-reduce jobs when
      // output directory exists
      throw new IOException("Table " + table.getNameAsString()
          + " already exists.");
    }

    // 创建表描述符
    HTableDescriptor domainTableDescp = new HTableDescriptor(table);
    // 创建INFO列族描述符
    HColumnDescriptor mappCF =
        new HColumnDescriptor(DomainColumnFamily.INFO.getBytes());
    // 设置布隆过滤器类型为ROWCOL，优化随机查询性能
    mappCF.setBloomFilterType(BloomType.ROWCOL);
    // 将INFO列族添加到表描述符
    domainTableDescp.addFamily(mappCF);

    // 设置按前缀拆分的分区策略，按用户名前缀划分Region
    domainTableDescp
        .setRegionSplitPolicyClassName(
            "org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy");
    // 设置前缀长度，对应用户名前缀长度
    domainTableDescp.setValue("KeyPrefixRegionSplitPolicy.prefix_length",
        TimelineHBaseSchemaConstants.USERNAME_SPLIT_KEY_PREFIX_LENGTH);
    // 按照用户名前缀预创建分区，创建HBase表
    admin.createTable(domainTableDescp,
        TimelineHBaseSchemaConstants.getUsernameSplits());
    // 记录表创建结果日志
    LOG.info("Status of table creation for " + table.getNameAsString() + "="
        + admin.tableExists(table));
  }
}