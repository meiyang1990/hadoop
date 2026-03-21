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

package org.apache.hadoop.yarn.server.timelineservice.documentstore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.*;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.api.records.timeline.TimelineHealth;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineAggregationTrack;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.CollectionType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEntityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowactivity.FlowActivityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowrun.FlowRunDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.TimelineCollectionWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * 文档存储类型时间线写入器实现，用于存储时间线实体信息。
 * 根据配置选择不同的文档存储后端（如MongoDB等）写入数据，支持按类型分集合存储。
 */
public class DocumentStoreTimelineWriterImpl extends AbstractService
    implements TimelineWriter {

  private static final Logger LOG = LoggerFactory
      .getLogger(DocumentStoreTimelineWriterImpl.class);
  private static final String DOC_ID_DELIMITER = "!";

  private DocumentStoreVendor storeType;
  private TimelineCollectionWriter<TimelineEntityDocument> appCollWriter;
  private TimelineCollectionWriter<TimelineEntityDocument>
      entityCollWriter;
  private TimelineCollectionWriter<FlowActivityDocument> flowActivityCollWriter;
  private TimelineCollectionWriter<FlowRunDocument> flowRunCollWriter;


  public DocumentStoreTimelineWriterImpl() {
    super(DocumentStoreTimelineWriterImpl.class.getName());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    // 从配置中获取文档存储厂商类型
    storeType = DocumentStoreUtils.getStoreVendor(conf);
    LOG.info("Initializing Document Store Writer for : " + storeType);
    super.serviceInit(conf);

    // 初始化应用集合写入器
    this.appCollWriter = new TimelineCollectionWriter<>(
        CollectionType.APPLICATION, conf);
    // 初始化普通实体集合写入器
    this.entityCollWriter = new TimelineCollectionWriter<>(
        CollectionType.ENTITY, conf);
    // 初始化流活动集合写入器
    this.flowActivityCollWriter = new TimelineCollectionWriter<>(
        CollectionType.FLOW_ACTIVITY, conf);
    // 初始化流运行集合写入器
    this.flowRunCollWriter = new TimelineCollectionWriter<>(
        CollectionType.FLOW_RUN, conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    // 关闭各集合写入器释放资源
    appCollWriter.close();
    entityCollWriter.close();
    flowActivityCollWriter.close();
    flowRunCollWriter.close();
  }

  @Override
  public TimelineWriteResponse write(TimelineCollectorContext
      context, TimelineEntities data, UserGroupInformation callerUgi) {
    LOG.debug("Writing Timeline Entity for appID : {}", context.getAppId());
    TimelineWriteResponse putStatus = new TimelineWriteResponse();
    // 获取提交用户的短用户名
    String subApplicationUser = callerUgi.getShortUserName();

    // 空值检查，避免生成文档ID时出现NPE
    if (DocumentStoreUtils.isNullOrEmpty(context.getFlowName(),
        context.getAppId(), context.getClusterId(), context.getUserId())) {
      LOG.warn("Found NULL for one of: flowName={} appId={} " +
          "userId={} clusterId={} . Not proceeding on writing to store : " +
          storeType);
      return putStatus;
    }

    // 遍历所有待写入实体
    for (TimelineEntity timelineEntity : data.getEntities()) {
      // 跳过空实体
      if(timelineEntity == null) {
        continue;
      }

      TimelineEntityDocument entityDocument;
      // 应用实体单独存储到应用集合
      if (ApplicationEntity.isApplicationEntity(timelineEntity)) {
        // 创建应用实体文档对象
        entityDocument = createTimelineEntityDoc(context, subApplicationUser,
            timelineEntity, true);
        // 创建流运行文档，用于聚合指标
        FlowRunDocument flowRunDoc = createFlowRunDoc(context,
            timelineEntity.getMetrics());
        // 根据应用创建/完成事件生成流活动文档
        FlowActivityDocument flowActivityDoc = getFlowActivityDoc(context,
            timelineEntity, flowRunDoc, entityDocument);
        // 写入应用文档到对应集合
        writeApplicationDoc(entityDocument);
        // 写入流运行文档到对应集合
        writeFlowRunDoc(flowRunDoc);
        // 如果生成了流活动文档则写入
        if(flowActivityDoc != null) {
          storeFlowActivityDoc(flowActivityDoc);
        }
      } else {
        // 非应用实体创建文档对象
        entityDocument = createTimelineEntityDoc(context, subApplicationUser,
            timelineEntity, false);
        // 如果提交用户和上下文用户不同，追加用户信息到上下文
        appendSubAppUserIfExists(context, subApplicationUser);
        // 设置实体创建时间
        entityDocument.setCreatedTime(fetchEntityCreationTime(timelineEntity));
        // 写入普通实体文档到实体集合
        writeEntityDoc(entityDocument);
      }
    }
    return putStatus;
  }

  @Override
  public TimelineWriteResponse write(TimelineCollectorContext context,
      TimelineDomain domain) throws IOException {
    return null;
  }

  @Override
  public TimelineHealth getHealthStatus() {
    // 默认返回运行中健康状态
    return new TimelineHealth(TimelineHealth.TimelineHealthStatus.RUNNING, "");
  }

  /**
   * 如果提交用户与上下文用户不同，将提交用户追加到上下文用户ID中。
   */
  private void appendSubAppUserIfExists(TimelineCollectorContext context,
      String subApplicationUser) {
    String userId = context.getUserId();
    if (!userId.equals(subApplicationUser) &&
        !userId.contains(subApplicationUser)) {
      userId = userId.concat(DOC_ID_DELIMITER).concat(subApplicationUser);
      context.setUserId(userId);
    }
  }

  /**
   * 创建时间线实体文档对象，根据是否为应用实体生成对应文档ID。
   */
  private TimelineEntityDocument createTimelineEntityDoc(
      TimelineCollectorContext context, String subApplicationUser,
      TimelineEntity timelineEntity, boolean isAppEntity) {
    TimelineEntityDocument entityDocument =
        new TimelineEntityDocument(timelineEntity);
    entityDocument.setContext(context);
    entityDocument.setFlowVersion(context.getFlowVersion());
    entityDocument.setSubApplicationUser(subApplicationUser);
    if (isAppEntity) {
      entityDocument.setId(DocumentStoreUtils.constructTimelineEntityDocId(
          context, timelineEntity.getType()));
    } else {
      entityDocument.setId(DocumentStoreUtils.constructTimelineEntityDocId(
          context, timelineEntity.getType(), timelineEntity.getId()));
    }
    return entityDocument;
  }

  /**
   * 创建流运行文档对象，生成对应文档ID。
   */
  private FlowRunDocument createFlowRunDoc(TimelineCollectorContext context,
      Set<TimelineMetric> metrics) {
    FlowRunDocument flowRunDoc = new FlowRunDocument(context, metrics);
    flowRunDoc.setFlowVersion(context.getFlowVersion());
    flowRunDoc.setId(DocumentStoreUtils.constructFlowRunDocId(context));
    return flowRunDoc;
  }

  /**
   * 根据实体类型从对应事件中提取实体创建时间。
   */
  private long fetchEntityCreationTime(TimelineEntity timelineEntity) {
    TimelineEvent event;
    // 根据实体类型选择对应的创建事件类型
    switch (TimelineEntityType.valueOf(timelineEntity.getType())) {
    case YARN_CONTAINER:
      // 容器从CREATED事件获取创建时间
      event = DocumentStoreUtils.fetchEvent(
          timelineEntity, ContainerMetricsConstants.CREATED_EVENT_TYPE);
      if (event != null) {
        return event.getTimestamp();
      }
      break;
    case YARN_APPLICATION_ATTEMPT:
      // 应用尝试从REGISTERED事件获取创建时间
      event = DocumentStoreUtils.fetchEvent(
          timelineEntity, AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE);
      if (event != null) {
        return event.getTimestamp();
      }
      break;
    default:
      // 其他类型不处理
    }
    // 没有找到对应事件则返回实体本身携带的创建时间
    if (timelineEntity.getCreatedTime() == null) {
      return 0;
    }
    return timelineEntity.getCreatedTime();
  }

  /**
   * 从应用实体的创建/完成事件中生成流活动文档。
   */
  private FlowActivityDocument getFlowActivityDoc(
      TimelineCollectorContext context,
      TimelineEntity timelineEntity, FlowRunDocument flowRunDoc,
      TimelineEntityDocument entityDocument) {
    FlowActivityDocument flowActivityDoc = null;
    // 检查是否有应用创建事件
    TimelineEvent event = DocumentStoreUtils.fetchEvent(
        timelineEntity, ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    if (event != null) {
      // 设置应用创建时间
      entityDocument.setCreatedTime(event.getTimestamp());
      // 设置流运行最小开始时间
      flowRunDoc.setMinStartTime(event.getTimestamp());
      // 创建流活动文档
      flowActivityDoc = createFlowActivityDoc(context, context.getFlowName(),
          context.getFlowVersion(), context.getFlowRunId(), event);
    }

    // 检查是否有应用完成事件
    event = DocumentStoreUtils.fetchEvent(timelineEntity,
        ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
    if (event != null) {
      // 设置流运行最大结束时间
      flowRunDoc.setMaxEndTime(event.getTimestamp());

      // 如果同时存在创建和完成事件且之前未创建流活动文档，则创建
      if (flowActivityDoc == null) {
        flowActivityDoc = createFlowActivityDoc(context, context.getFlowName(),
            context.getFlowVersion(), context.getFlowRunId(), event);
      }
    }
    return flowActivityDoc;
  }

  /**
   * 创建流活动文档对象，生成文档ID并设置时间戳按天分区。
   */
  private FlowActivityDocument createFlowActivityDoc(
      TimelineCollectorContext context, String flowName, String flowVersion,
      long flowRunId, TimelineEvent event) {
    FlowActivityDocument flowActivityDoc = new FlowActivityDocument(flowName,
        flowVersion, flowRunId);
    // 获取当日零点时间戳用于按天分区存储
    flowActivityDoc.setDayTimestamp(DocumentStoreUtils.getTopOfTheDayTimestamp(
        event.getTimestamp()));
    flowActivityDoc.setFlowName(flowName);
    flowActivityDoc.setUser(context.getUserId());
    flowActivityDoc.setId(DocumentStoreUtils.constructFlowActivityDocId(
        context, event.getTimestamp()));
    return flowActivityDoc;
  }

  /**
   * 将流运行文档写入对应集合。
   */
  private void writeFlowRunDoc(FlowRunDocument flowRunDoc) {
    flowRunCollWriter.writeDocument(flowRunDoc);
  }

  /**
   * 将流活动文档写入对应集合。
   */
  private void storeFlowActivityDoc(FlowActivityDocument flowActivityDoc) {
    flowActivityCollWriter.writeDocument(flowActivityDoc);
  }

  /**
   * 将普通实体文档写入对应集合。
   */
  private void writeEntityDoc(TimelineEntityDocument entityDocument) {
    entityCollWriter.writeDocument(entityDocument);
  }

  /**
   * 将应用实体文档写入对应集合。
   */
  private void writeApplicationDoc(TimelineEntityDocument entityDocument) {
    appCollWriter.writeDocument(entityDocument);
  }

  public TimelineWriteResponse aggregate(TimelineEntity data,
      TimelineAggregationTrack track) {
    return null;
  }

  @Override
  public void flush() {
  }
}