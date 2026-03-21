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
package org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow;


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

import java.io.IOException;

/**
 * AppToFlow表读写操作实现，存储应用ID到流信息的映射关系，用于时间线服务。
 * 支持表创建、读写App-To-Flow映射关系。
 */
public class AppToFlowTableRW extends BaseTableRW<AppToFlowTable> {
  /** 配置键前缀。 */
  private static final String PREFIX =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "app-flow";

  /** 配置项名称：AppToFlow表名。 */
  public static final String TABLE_NAME_CONF_NAME = PREFIX + ".table.name";

  /** AppToFlow表默认名称。 */
  private static final String DEFAULT_TABLE_NAME = "timelineservice.app_flow";

  private static final Logger LOG =
      LoggerFactory.getLogger(AppToFlowTableRW.class);

  /**
   * 构造方法，初始化AppToFlow表读写器。
   */
  public AppToFlowTableRW() {
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
   * 创建AppToFlow HBase表，如果表已存在则抛出异常。
   * @param admin HBase管理员客户端
   * @param hbaseConf HBase配置
   * @throws IOException 创建表失败或表已存在时抛出IO异常
   */
  public void createTable(Admin admin, Configuration hbaseConf)
      throws IOException {

    // 从配置获取表名
    TableName table = getTableName(hbaseConf);
    // 检查表是否已存在
    if (admin.tableExists(table)) {
      // 不覆盖现有表，同MapReduce输出目录存在时的处理逻辑
      throw new IOException("Table " + table.getNameAsString()
          + " already exists.");
    }

    // 新建表描述符
    HTableDescriptor appToFlowTableDescp = new HTableDescriptor(table);
    // 创建映射列族
    HColumnDescriptor mappCF =
        new HColumnDescriptor(AppToFlowColumnFamily.MAPPING.getBytes());
    // 设置布隆过滤器类型为ROWCOL，优化随机查询性能
    mappCF.setBloomFilterType(BloomType.ROWCOL);
    // 添加列族到表
    appToFlowTableDescp.addFamily(mappCF);

    // 创建表
    admin.createTable(appToFlowTableDescp);
    // 记录创建结果
    LOG.info("Status of table creation for " + table.getNameAsString() + "="
        + admin.tableExists(table));
  }
}