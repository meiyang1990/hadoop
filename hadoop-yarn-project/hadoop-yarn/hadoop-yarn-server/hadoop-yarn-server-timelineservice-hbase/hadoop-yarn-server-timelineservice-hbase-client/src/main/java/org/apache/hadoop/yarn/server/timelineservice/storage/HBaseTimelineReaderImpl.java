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


import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timeline.TimelineHealth;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.reader.EntityTypeReader;
import org.apache.hadoop.yarn.server.timelineservice.storage.reader.TimelineEntityReader;
import org.apache.hadoop.yarn.server.timelineservice.storage.reader.TimelineEntityReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于HBase存储实现的时间线数据读取器，提供从HBase中读取应用时间线数据的能力。
 * 实现{@link TimelineReader}接口，是YARN时间线服务HBase存储层的核心读取组件。
 */
public class HBaseTimelineReaderImpl
    extends AbstractService implements TimelineReader {

  private static final Logger LOG = LoggerFactory
      .getLogger(HBaseTimelineReaderImpl.class);

  private Configuration hbaseConf = null;
  private Connection conn;
  private TimelineStorageMonitor storageMonitor;

  /**
   * 构造函数，初始化服务基类。
   */
  public HBaseTimelineReaderImpl() {
    super(HBaseTimelineReaderImpl.class.getName());
  }

  @Override
  /**
   * 服务初始化方法，创建HBase连接和存储监控器。
   */
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    // 从全局配置提取时间线服务专属HBase配置
    hbaseConf = HBaseTimelineStorageUtils.getTimelineServiceHBaseConf(conf);
    // 创建HBase连接对象
    conn = ConnectionFactory.createConnection(hbaseConf);
    // 初始化HBase存储健康监控器
    storageMonitor = new HBaseStorageMonitor(conf);
  }

  @Override
  /**
   * 服务启动方法，启动存储健康监控。
   */
  protected void serviceStart() throws Exception {
    super.serviceStart();
    storageMonitor.start();
  }

  @Override
  /**
   * 服务停止方法，关闭HBase连接并停止监控。
   */
  protected void serviceStop() throws Exception {
    if (conn != null) {
      LOG.info("closing the hbase Connection");
      // 关闭HBase连接释放资源
      conn.close();
    }
    // 停止存储健康监控
    storageMonitor.stop();
    super.serviceStop();
  }

  @Override
  /**
   * 根据上下文获取单个时间线实体。
   * @param context 读取上下文，包含查询实体的基本信息
   * @param dataToRetrieve 指定需要返回哪些数据字段
   * @return 匹配的时间线实体
   * @throws IOException 存储异常或存储不可用
   */
  public TimelineEntity getEntity(TimelineReaderContext context,
      TimelineDataToRetrieve dataToRetrieve) throws IOException {
    // 检查HBase存储是否可用
    storageMonitor.checkStorageIsUp();
    // 创建单个实体读取器
    TimelineEntityReader reader =
        TimelineEntityReaderFactory.createSingleEntityReader(context,
            dataToRetrieve);
    // 执行读取并返回结果
    return reader.readEntity(hbaseConf, conn);
  }

  @Override
  /**
   * 根据过滤条件获取多个匹配的时间线实体。
   * @param context 读取上下文
   * @param filters 实体过滤条件
   * @param dataToRetrieve 指定需要返回哪些数据字段
   * @return 匹配的时间线实体集合
   * @throws IOException 存储异常或存储不可用
   */
  public Set<TimelineEntity> getEntities(TimelineReaderContext context,
      TimelineEntityFilters filters, TimelineDataToRetrieve dataToRetrieve)
      throws IOException {
    // 检查HBase存储是否可用
    storageMonitor.checkStorageIsUp();
    // 创建多实体读取器
    TimelineEntityReader reader =
        TimelineEntityReaderFactory.createMultipleEntitiesReader(context,
            filters, dataToRetrieve);
    // 执行读取并返回结果
    return reader.readEntities(hbaseConf, conn);
  }

  @Override
  /**
   * 获取当前上下文存在的所有实体类型。
   * @param context 读取上下文
   * @return 实体类型名称集合
   * @throws IOException 存储异常或存储不可用
   */
  public Set<String> getEntityTypes(TimelineReaderContext context)
      throws IOException {
    // 检查HBase存储是否可用
    storageMonitor.checkStorageIsUp();
    // 创建实体类型读取器
    EntityTypeReader reader = new EntityTypeReader(context);
    // 执行读取并返回结果
    return reader.readEntityTypes(hbaseConf, conn);
  }

  @Override
  /**
   * 获取时间线存储的健康状态。
   * @return 健康状态对象，包含运行状态和错误信息
   */
  public TimelineHealth getHealthStatus() {
    try {
      // 检查存储可用性
      storageMonitor.checkStorageIsUp();
      // 健康检查通过，返回运行中状态
      return new TimelineHealth(TimelineHealth.TimelineHealthStatus.RUNNING,
          "");
    } catch (IOException e){
      // 健康检查失败，返回连接失败状态
      return new TimelineHealth(
          TimelineHealth.TimelineHealthStatus.CONNECTION_FAILURE,
          "HBase connection is down");
    }
  }

  /**
   * 获取存储监控器实例，用于测试等场景。
   * @return 时间线存储监控对象
   */
  protected TimelineStorageMonitor getTimelineStorageMonitor() {
    return storageMonitor;
  }

}