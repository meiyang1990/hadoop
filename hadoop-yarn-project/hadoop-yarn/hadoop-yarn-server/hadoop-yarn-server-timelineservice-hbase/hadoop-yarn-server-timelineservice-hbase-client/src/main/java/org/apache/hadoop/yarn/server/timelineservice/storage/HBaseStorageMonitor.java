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
package org.apache.hadoop.yarn.server.timelineservice.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.reader.TimelineEntityReader;
import org.apache.hadoop.yarn.server.timelineservice.storage.reader.TimelineEntityReaderFactory;

/**
 * 基于HBase实现的时间线存储健康监控器，用于检查HBase存储服务的可用性
 */
public class HBaseStorageMonitor extends TimelineStorageMonitor {

  /** 监控查询过滤器：限制只返回1条实体，最小化查询开销 */
  protected static final TimelineEntityFilters MONITOR_FILTERS =
      new TimelineEntityFilters.Builder().entityLimit(1L).build();
  /** 监控查询参数：不获取任何具体数据，仅验证连接可用性 */
  protected static final TimelineDataToRetrieve DATA_TO_RETRIEVE =
      new TimelineDataToRetrieve(null, null, null, null, null, null);

  private Configuration monitorHBaseConf;
  private Connection monitorConn;
  private TimelineEntityReader reader;

  /**
   * 构造HBase存储监控器，完成初始化
   * @param conf Yarn配置对象
   * @throws Exception 初始化失败时抛出异常
   */
  public HBaseStorageMonitor(Configuration conf) throws Exception {
    super(conf, Storage.HBase);
    this.initialize(conf);
  }

  /**
   * 初始化HBase连接和监控查询上下文
   * @param conf Yarn配置对象
   * @throws Exception 初始化失败时抛出异常
   */
  private void initialize(Configuration conf) throws  Exception {
    // 从配置中获取时间线服务HBase专用配置
    monitorHBaseConf = HBaseTimelineStorageUtils.
        getTimelineServiceHBaseConf(conf);
    // 降低HBase客户端重试次数，快速失败
    monitorHBaseConf.setInt("hbase.client.retries.number", 3);
    // 设置重试等待时间
    monitorHBaseConf.setLong("hbase.client.pause", 1000);
    // 获取健康检查间隔配置
    long monitorInterval = conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_READER_STORAGE_MONITOR_INTERVAL_MS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_STORAGE_MONITOR_INTERVAL_MS
    );
    // 将RPC超时设置为监控间隔，避免超时早于检查周期
    monitorHBaseConf.setLong("hbase.rpc.timeout", monitorInterval);
    // 将扫描超时设置为监控间隔，避免超时早于检查周期
    monitorHBaseConf.setLong("hbase.client.scanner.timeout.period",
        monitorInterval);
    // 降低Zookeeper重试次数，快速失败
    monitorHBaseConf.setInt("zookeeper.recovery.retry", 1);
    // 创建HBase连接
    monitorConn = ConnectionFactory.createConnection(monitorHBaseConf);

    // 获取集群ID
    String clusterId = conf.get(YarnConfiguration.RM_CLUSTER_ID,
        YarnConfiguration.DEFAULT_RM_CLUSTER_ID);
    // 构造监控查询上下文，查询YARN流活动实体
    TimelineReaderContext monitorContext =
        new TimelineReaderContext(clusterId, null, null, null, null,
        TimelineEntityType.YARN_FLOW_ACTIVITY.toString(), null, null);
    // 创建多实体读取器用于健康检查
    reader = TimelineEntityReaderFactory.createMultipleEntitiesReader(
        monitorContext, MONITOR_FILTERS, DATA_TO_RETRIEVE);
  }

  @Override
  /**
   * 执行健康检查，通过读取实体验证HBase服务可用性
   * @throws Exception 健康检查失败时抛出异常
   */
  public void healthCheck() throws Exception {
    // 执行查询，若成功则说明HBase存储正常
    reader.readEntities(monitorHBaseConf, monitorConn);
  }

  @Override
  /**
   * 启动监控器
   */
  public void start() {
    super.start();
  }

  @Override
  /**
   * 停止监控器，关闭HBase连接释放资源
   * @throws Exception 关闭连接失败时抛出异常
   */
  public void stop() throws Exception {
    super.stop();
    monitorConn.close();
  }
}