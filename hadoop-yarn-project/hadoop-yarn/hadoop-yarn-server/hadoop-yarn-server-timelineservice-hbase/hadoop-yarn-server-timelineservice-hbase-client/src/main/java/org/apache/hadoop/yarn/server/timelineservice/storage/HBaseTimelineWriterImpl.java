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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timeline.TimelineHealth;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.SubApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnRWHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.EventColumnName;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.StringKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TypedBufferedMutator;
import org.apache.hadoop.yarn.server.timelineservice.storage.domain.DomainColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.domain.DomainRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.domain.DomainTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.domain.DomainTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationCompactionDimension;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationOperation;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationTableRW;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于HBase实现的时间线数据存储后端，负责将时间线实体信息写入多个HBase表
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HBaseTimelineWriterImpl extends AbstractService implements
    TimelineWriter {

  private static final Logger LOG = LoggerFactory
      .getLogger(HBaseTimelineWriterImpl.class);

  private Connection conn;
  private TimelineStorageMonitor storageMonitor;
  private TypedBufferedMutator<EntityTable> entityTable;
  private TypedBufferedMutator<AppToFlowTable> appToFlowTable;
  private TypedBufferedMutator<ApplicationTable> applicationTable;
  private TypedBufferedMutator<FlowActivityTable> flowActivityTable;
  private TypedBufferedMutator<FlowRunTable> flowRunTable;
  private TypedBufferedMutator<SubApplicationTable> subApplicationTable;
  private TypedBufferedMutator<DomainTable> domainTable;

  /**
   * 用于字符串键与存储格式之间的转换
   */
  private final KeyConverter<String> stringKeyConverter =
      new StringKeyConverter();

  /**
   * 用于Long类型键与存储格式之间的转换
   */
  private final KeyConverter<Long> longKeyConverter = new LongKeyConverter();

  /**
   * 枚举类型标识需要写入的目标表类型
   */
  private enum Tables {
    APPLICATION_TABLE, ENTITY_TABLE, SUBAPPLICATION_TABLE
  };

  public HBaseTimelineWriterImpl() {
    super(HBaseTimelineWriterImpl.class.getName());
  }

  /**
   * 初始化HBase连接，为写入实体表做准备
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    // 从配置中提取HBase相关配置
    Configuration hbaseConf =
        HBaseTimelineStorageUtils.getTimelineServiceHBaseConf(conf);
    // 创建HBase连接
    conn = ConnectionFactory.createConnection(hbaseConf);
    // 为各个表获取批量写入器
    entityTable = new EntityTableRW().getTableMutator(hbaseConf, conn);
    appToFlowTable = new AppToFlowTableRW().getTableMutator(hbaseConf, conn);
    applicationTable =
        new ApplicationTableRW().getTableMutator(hbaseConf, conn);
    flowRunTable = new FlowRunTableRW().getTableMutator(hbaseConf, conn);
    flowActivityTable =
        new FlowActivityTableRW().getTableMutator(hbaseConf, conn);
    subApplicationTable =
        new SubApplicationTableRW().getTableMutator(hbaseConf, conn);
    domainTable = new DomainTableRW().getTableMutator(hbaseConf, conn);

    // 获取当前用户UGI，安全模式使用登录用户，否则使用当前用户
    UserGroupInformation ugi = UserGroupInformation.isSecurityEnabled() ?
        UserGroupInformation.getLoginUser() :
        UserGroupInformation.getCurrentUser();
    // 初始化存储监控器，用于检查HBase可用性
    storageMonitor = new HBaseStorageMonitor(conf);
    LOG.info("Initialized HBaseTimelineWriterImpl UGI to " + ugi);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    // 启动存储监控器
    storageMonitor.start();
  }

  /**
   * 将TimelineEntities中的全部时间线数据写入HBase存储
   */
  @Override
  public TimelineWriteResponse write(TimelineCollectorContext context,
      TimelineEntities data, UserGroupInformation callerUgi)
      throws IOException {
    // 检查存储服务是否可用
    storageMonitor.checkStorageIsUp();
    TimelineWriteResponse putStatus = new TimelineWriteResponse();

    // 从上下文提取基础信息
    String clusterId = context.getClusterId();
    String userId = context.getUserId();
    String flowName = context.getFlowName();
    String flowVersion = context.getFlowVersion();
    long flowRunId = context.getFlowRunId();
    String appId = context.getAppId();
    // 提取提交用户用户名
    String subApplicationUser = callerUgi.getShortUserName();

    // 防御性检查避免构建行键时出现空指针
    if ((flowName == null) || (appId == null) || (clusterId == null)
        || (userId == null)) {
      LOG.warn("Found null for one of: flowName=" + flowName + " appId=" + appId
          + " userId=" + userId + " clusterId=" + clusterId
          + " . Not proceeding with writing to hbase");
      return putStatus;
    }

    // 遍历所有实体逐个写入
    for (TimelineEntity te : data.getEntities()) {

      // 跳过空实体
      if (te == null) {
        continue;
      }

      // 判断实体是否为应用实体，应用实体写入应用表
      boolean isApplication = ApplicationEntity.isApplicationEntity(te);
      byte[] rowKey;
      if (isApplication) {
        // 构建应用行键并写入应用表
        ApplicationRowKey applicationRowKey =
            new ApplicationRowKey(clusterId, userId, flowName, flowRunId,
                appId);
        rowKey = applicationRowKey.getRowKey();
        store(rowKey, te, flowVersion, Tables.APPLICATION_TABLE);
      } else {
        // 构建普通实体行键并写入实体表
        EntityRowKey entityRowKey =
            new EntityRowKey(clusterId, userId, flowName, flowRunId, appId,
                te.getType(), te.getIdPrefix(), te.getId());
        rowKey = entityRowKey.getRowKey();
        store(rowKey, te, flowVersion, Tables.ENTITY_TABLE);
      }

      // 若为子应用实体，额外写入子应用表
      if (!isApplication && SubApplicationEntity.isSubApplicationEntity(te)) {
        SubApplicationRowKey subApplicationRowKey =
            new SubApplicationRowKey(subApplicationUser, clusterId,
                te.getType(), te.getIdPrefix(), te.getId(), userId);
        rowKey = subApplicationRowKey.getRowKey();
        store(rowKey, te, flowVersion, Tables.SUBAPPLICATION_TABLE);
      }

      // 应用实体额外处理：写入流相关表
      if (isApplication) {
        // 获取应用创建事件
        TimelineEvent event =
            ApplicationEntity.getApplicationEvent(te,
                ApplicationMetricsConstants.CREATED_EVENT_TYPE);
        FlowRunRowKey flowRunRowKey =
            new FlowRunRowKey(clusterId, userId, flowName, flowRunId);
        if (event != null) {
          // 处理应用创建逻辑，写入应用到流映射和流活动表
          onApplicationCreated(flowRunRowKey, clusterId, appId, userId,
              flowVersion, te, event.getTimestamp());
        }
        // 存储应用运行期间的指标到流运行表
        storeFlowMetricsAppRunning(flowRunRowKey, appId, te);
        // 获取应用完成事件
        event = ApplicationEntity.getApplicationEvent(te,
            ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
        if (event != null) {
          // 处理应用完成逻辑，写入完成时间和最终指标
          onApplicationFinished(flowRunRowKey, flowVersion, appId, te,
              event.getTimestamp());
        }
      }
    }
    return putStatus;
  }

  @Override
  public TimelineWriteResponse write(TimelineCollectorContext context,
      TimelineDomain domain)
      throws IOException {
    // 检查存储服务是否可用
    storageMonitor.checkStorageIsUp();
    TimelineWriteResponse putStatus = new TimelineWriteResponse();

    String clusterId = context.getClusterId();
    String domainId = domain.getId();

    // 防御性检查避免构建行键时出现空指针
    if (clusterId == null) {
      LOG.warn(
          "Found null for clusterId. Not proceeding with writing to hbase");
      return putStatus;
    }

    // 构建域行键
    DomainRowKey domainRowKey = new DomainRowKey(clusterId, domainId);
    byte[] rowKey = domainRowKey.getRowKey();

    // 将域各个属性写入域表
    ColumnRWHelper.store(rowKey, domainTable, DomainColumn.CREATED_TIME, null,
        domain.getCreatedTime());
    ColumnRWHelper.store(rowKey, domainTable, DomainColumn.DESCRIPTION, null,
        domain.getDescription());
    ColumnRWHelper
        .store(rowKey, domainTable, DomainColumn.MODIFICATION_TIME, null,
            domain.getModifiedTime());
    ColumnRWHelper.store(rowKey, domainTable, DomainColumn.OWNER, null,
        domain.getOwner());
    ColumnRWHelper.store(rowKey, domainTable, DomainColumn.READERS, null,
        domain.getReaders());
    ColumnRWHelper.store(rowKey, domainTable, DomainColumn.WRITERS, null,
        domain.getWriters());
    return putStatus;
  }

  /**
   * 应用创建时，处理相关表的写入：应用-流映射、流运行表、流活动表
   */
  private void onApplicationCreated(FlowRunRowKey flowRunRowKey,
      String clusterId, String appId, String userId, String flowVersion,
      TimelineEntity te, long appCreatedTimeStamp)
      throws IOException {

    String flowName = flowRunRowKey.getFlowName();
    Long flowRunId = flowRunRowKey.getFlowRunId();

    // 写入应用到流映射表
    AppToFlowRowKey appToFlowRowKey = new AppToFlowRowKey(appId);
    byte[] rowKey = appToFlowRowKey.getRowKey();
    ColumnRWHelper.store(rowKey, appToFlowTable,
        AppToFlowColumnPrefix.FLOW_NAME, clusterId, null, flowName);
    ColumnRWHelper.store(rowKey, appToFlowTable,
        AppToFlowColumnPrefix.FLOW_RUN_ID, clusterId, null, flowRunId);
    ColumnRWHelper.store(rowKey, appToFlowTable, AppToFlowColumnPrefix.USER_ID,
        clusterId, null, userId);

    // 写入流运行表，记录应用创建信息
    storeAppCreatedInFlowRunTable(flowRunRowKey, appId, te);

    // 写入流活动表，记录本次流运行的活动信息
    byte[] flowActivityRowKeyBytes =
        new FlowActivityRowKey(flowRunRowKey.getClusterId(),
            appCreatedTimeStamp, flowRunRowKey.getUserId(), flowName)
            .getRowKey();
    byte[] qualifier = longKeyConverter.encode(flowRunRowKey.getFlowRunId());
    ColumnRWHelper.store(flowActivityRowKeyBytes, flowActivityTable,
        FlowActivityColumnPrefix.RUN_ID, qualifier, null, flowVersion,
        AggregationCompactionDimension.APPLICATION_ID.getAttribute(appId));
  }

  /*
   * 更新流运行表，写入应用创建信息
   */
  private void storeAppCreatedInFlowRunTable(FlowRunRowKey flowRunRowKey,
      String appId, TimelineEntity te) throws IOException {
    byte[] rowKey = flowRunRowKey.getRowKey();
    ColumnRWHelper.store(rowKey, flowRunTable, FlowRunColumn.MIN_START_TIME,
        null, te.getCreatedTime(),
        AggregationCompactionDimension.APPLICATION_ID.getAttribute(appId));
  }


  /*
   * 应用完成时，更新流运行表和流活动表
   */
  private void onApplicationFinished(FlowRunRowKey flowRunRowKey,
      String flowVersion, String appId, TimelineEntity te,
      long appFinishedTimeStamp) throws IOException {
    // 更新流运行表，写入完成信息
    storeAppFinishedInFlowRunTable(flowRunRowKey, appId, te,
        appFinishedTimeStamp);

    // 在流活动表中标记应用完成
    byte[] rowKey =
        new FlowActivityRowKey(flowRun