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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;

/**
 * 时间线服务FlowRun表的读写操作实现，负责FlowRun表存储流运行实例元数据。
 * 继承BaseTableRW提供通用表操作能力，提供表创建逻辑。
 */
public class FlowRunTableRW extends BaseTableRW<FlowRunTable> {
  /** 配置键前缀. */
  private static final String PREFIX =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "flowrun";

  /** 配置参数名：指定FlowRun表名称. */
  public static final String TABLE_NAME_CONF_NAME = PREFIX + ".table.name";

  /** 默认FlowRun表名称默认值. */
  public static final String DEFAULT_TABLE_NAME = "timelineservice.flowrun";

  private static final Logger LOG =
      LoggerFactory.getLogger(FlowRunTableRW.class);

  /** 指标列最大版本数默认值. */
  public static final int DEFAULT_METRICS_MAX_VERSIONS = Integer.MAX_VALUE;

  /**
   * 构造函数，传入表名称配置键和默认表名初始化父类。
   */
  public FlowRunTableRW() {
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
   * 在HBase中创建FlowRun表，包含列族和协处理器配置。
   * @param admin HBase管理员客户端
   * @param hbaseConf HBase配置
   * @throws IOException 如果表已存在或创建失败抛出异常
   */
  public void createTable(Admin admin, Configuration hbaseConf)
      throws IOException {

    // 从配置获取表名
    TableName table = getTableName(hbaseConf);
    // 表已存在则抛出异常，避免覆盖已有数据
    if (admin.tableExists(table)) {
      // do not disable / delete existing table
      // similar to the approach taken by map-reduce jobs when
      // output directory exists
      throw new IOException("Table " + table.getNameAsString()
          + " already exists.");
    }

    // 创建表描述符
    HTableDescriptor flowRunTableDescp = new HTableDescriptor(table);
    // 创建INFO列族描述符
    HColumnDescriptor infoCF =
        new HColumnDescriptor(FlowRunColumnFamily.INFO.getBytes());
    // 设置布隆过滤器为ROWCOL，提升随机查询性能
    infoCF.setBloomFilterType(BloomType.ROWCOL);
    // 将INFO列族添加到表描述符
    flowRunTableDescp.addFamily(infoCF);
    // 设置最小版本数为1，保留至少1个版本数据
    infoCF.setMinVersions(1);
    // 设置最大版本数，保留所有版本历史
    infoCF.setMaxVersions(DEFAULT_METRICS_MAX_VERSIONS);

    // TODO: figure the split policy
    // 从配置获取协处理器Jar包在HDFS上的路径
    String coprocessorJarPathStr = hbaseConf.get(
        YarnConfiguration.FLOW_RUN_COPROCESSOR_JAR_HDFS_LOCATION,
        YarnConfiguration.DEFAULT_HDFS_LOCATION_FLOW_RUN_COPROCESSOR_JAR);

    // 转换为Path对象
    Path coprocessorJarPath = new Path(coprocessorJarPathStr);
    LOG.info("CoprocessorJarPath=" + coprocessorJarPath.toString());
    // 添加FlowRun协处理器，用于处理聚合逻辑
    flowRunTableDescp.addCoprocessor(
        "org.apache.hadoop.yarn.server.timelineservice.storage." +
            "flow.FlowRunCoprocessor", coprocessorJarPath,
        Coprocessor.PRIORITY_USER, null);
    // 调用HBase API创建表
    admin.createTable(flowRunTableDescp);
    LOG.info("Status of table creation for " + table.getNameAsString() + "="
        + admin.tableExists(table));
  }
}