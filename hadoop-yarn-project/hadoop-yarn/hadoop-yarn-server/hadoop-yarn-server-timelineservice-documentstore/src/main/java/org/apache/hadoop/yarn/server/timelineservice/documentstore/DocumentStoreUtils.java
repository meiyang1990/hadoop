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

import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelinePrefixFilter;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEntityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEventSubDoc;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineMetricSubDoc;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowactivity.FlowActivityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowrun.FlowRunDocument;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * 文档存储服务工具类，为文档存储厂商提供读写文档所需的通用工具方法。
 */
public final class DocumentStoreUtils {

  private DocumentStoreUtils(){}

  /** 一天包含的毫秒数 */
  private static final long MILLIS_ONE_DAY = 86400000L;

  private static final String TIMELINE_STORE_TYPE =
      YarnConfiguration.TIMELINE_SERVICE_PREFIX + "document-store-type";
  static final String TIMELINE_SERVICE_COSMOSDB_ENDPOINT =
      "yarn.timeline-service.document-store.cosmos-db.endpoint";
  static final String TIMELINE_SERVICE_COSMOSDB_MASTER_KEY =
      "yarn.timeline-service.document-store.cosmos-db.masterkey";
  static final String TIMELINE_SERVICE_DOCUMENTSTORE_DATABASE_NAME =
      "yarn.timeline-service.document-store.db-name";
  private static final String
      DEFAULT_TIMELINE_SERVICE_DOCUMENTSTORE_DATABASE_NAME = "timeline_service";

  /**
   * 验证yarn-site.xml中CosmosDB配置是否完整正确。
   * @param conf Yarn配置对象
   * @throws YarnException 如果缺少必填配置项则抛出异常
   */
  public static void validateCosmosDBConf(Configuration conf)
      throws YarnException {
    if (conf == null) {
      throw new NullPointerException("Configuration cannot be null");
    }
    if (isNullOrEmpty(conf.get(TIMELINE_SERVICE_COSMOSDB_ENDPOINT),
        conf.get(TIMELINE_SERVICE_COSMOSDB_MASTER_KEY))) {
      throw new YarnException("One or more CosmosDB configuration property is" +
          " missing in yarn-site.xml");
    }
  }

  /**
   * 获取配置中指定的文档存储厂商类型，默认使用CosmosDB。
   * @param conf Yarn配置对象
   * @return 返回配置的文档存储厂商，如果未配置则返回默认的COSMOS_DB
   */
  public static DocumentStoreVendor getStoreVendor(Configuration conf) {
    return DocumentStoreVendor.getStoreType(conf.get(TIMELINE_STORE_TYPE,
        DocumentStoreVendor.COSMOS_DB.name()));
  }

  /**
   * 从时间线实体中按事件类型查找指定事件。
   * @param timelineEntity 待查找的时间线实体
   * @param eventType 需要查找的事件类型
   * @return 找到则返回对应TimelineEvent，否则返回null
   */
  public static TimelineEvent fetchEvent(TimelineEntity timelineEntity,
      String eventType) {
    for (TimelineEvent event : timelineEntity.getEvents()) {
      if (event.getId().equals(eventType)) {
        return event;
      }
    }
    return null;
  }

  /**
   * 检查给定字符串数组中是否存在null或空字符串。
   * @param values 需要检查的字符串数组
   * @return 任意字符串为null或空则返回true，全部非空则返回false
   */
  public static boolean isNullOrEmpty(String...values) {
    if (values == null || values.length == 0) {
      return true;
    }

    for (String value : values) {
      if (value == null || value.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /**
   * 创建CosmosDB异步文档客户端实例。
   * @param conf Yarn配置对象，用于获取CosmosDB端点和密钥
   * @return CosmosDB异步文档客户端实例
   */
  public static AsyncDocumentClient createCosmosDBAsyncClient(
      Configuration conf){
    return new AsyncDocumentClient.Builder()
      .withServiceEndpoint(DocumentStoreUtils.getCosmosDBEndpoint(conf))
      .withMasterKeyOrResourceToken(
          DocumentStoreUtils.getCosmosDBMasterKey(conf))
      .withConnectionPolicy(ConnectionPolicy.GetDefault())
      .withConsistencyLevel(ConsistencyLevel.Session)
      .build();
  }

  /**
   * 计算给定时间戳对应日期的起始时间戳（即当天0点整）。
   * @param timeStamp 输入时间戳
   * @return 对应日期起始时间戳（0点整）
   */
  public static long getTopOfTheDayTimestamp(long timeStamp) {
    return timeStamp - (timeStamp % MILLIS_ONE_DAY);
  }

  /**
   * 构建时间线实体文档的复合文档ID。
   * @param collectorContext 时间线写入器收集上下文
   * @param type 实体类型
   * @return 使用!分隔的复合ID
   */
  public static String constructTimelineEntityDocId(TimelineCollectorContext
      collectorContext, String type) {
    return String.format("%s!%s!%s!%d!%s!%s",
        collectorContext.getClusterId(), collectorContext.getUserId(),
        collectorContext.getFlowName(), collectorContext.getFlowRunId(),
        collectorContext.getAppId(), type);
  }

  /**
   * 构建带实体ID的时间线实体文档的复合文档ID。
   * @param collectorContext 时间线写入器收集上下文
   * @param type 实体类型
   * @param id 实体ID
   * @return 使用!分隔的复合ID
   */
  public static String constructTimelineEntityDocId(TimelineCollectorContext
      collectorContext, String type, String id) {
    return String.format("%s!%s!%s!%d!%s!%s!%s",
        collectorContext.getClusterId(), collectorContext.getUserId(),
        collectorContext.getFlowName(), collectorContext.getFlowRunId(),
        collectorContext.getAppId(), type, id);
  }

  /**
   * 构建流运行文档的复合文档ID。
   * @param collectorContext 时间线写入器收集上下文
   * @return 使用!分隔的复合ID
   */
  public static String constructFlowRunDocId(TimelineCollectorContext
      collectorContext) {
    return String.format("%s!%s!%s!%s", collectorContext.getClusterId(),
        collectorContext.getUserId(), collectorContext.getFlowName(),
        collectorContext.getFlowRunId());
  }

  /**
   * 构建流活动文档的复合文档ID。
   * @param collectorContext 时间线写入器收集上下文
   * @param eventTimestamp 时间线实体事件时间戳
   * @return 使用!分隔的复合ID
   */
  public static String constructFlowActivityDocId(TimelineCollectorContext
      collectorContext, long eventTimestamp) {
    return String.format("%s!%s!%s!%s", collectorContext.getClusterId(),
        getTopOfTheDayTimestamp(eventTimestamp),
        collectorContext.getUserId(), collectorContext.getFlowName());
  }

  private static String getCosmosDBEndpoint(Configuration conf) {
    return conf.get(TIMELINE_SERVICE_COSMOSDB_ENDPOINT);
  }

  private static String getCosmosDBMasterKey(Configuration conf) {
    return conf.get(TIMELINE_SERVICE_COSMOSDB_MASTER_KEY);
  }

  /**
   * 获取文档存储数据库名称，使用配置值，未配置则生成默认名称。
   * @param conf Yarn配置对象
   * @return 数据库名称
   */
  public static String getCosmosDBDatabaseName(Configuration conf) {
    return conf.get(TIMELINE_SERVICE_DOCUMENTSTORE_DATABASE_NAME,
        getDefaultTimelineServiceDBName(conf));
  }

  private static String getDefaultTimelineServiceDBName(
      Configuration conf) {
    return getClusterId(conf) + "_" +
        DEFAULT_TIMELINE_SERVICE_DOCUMENTSTORE_DATABASE_NAME;
  }

  private static String getClusterId(Configuration conf) {
    return conf.get(YarnConfiguration.RM_CLUSTER_ID,
        YarnConfiguration.DEFAULT_RM_CLUSTER_ID);
  }

  private static boolean isTimeInRange(long time, long timeBegin,
      long timeEnd) {
    return (time >= timeBegin) && (time <= timeEnd);
  }

  /**
   * 检查过滤器是否与给定时间线实体不匹配。
   * @param filters 需要应用的过滤器集合
   * @param timelineEntity 待检查的时间线实体
   * @return 任意过滤器不匹配则返回true，全部匹配则返回false
   * @throws IOException 如果遇到不支持的过滤器类型抛出异常
   */
  static boolean isFilterNotMatching(TimelineEntityFilters filters,
      TimelineEntity timelineEntity) throws IOException {
    // 检查创建时间是否在范围内
    if (timelineEntity.getCreatedTime() != null && !isTimeInRange(timelineEntity
        .getCreatedTime(), filters.getCreatedTimeBegin(),
        filters.getCreatedTimeEnd())) {
      return true;
    }

    // 检查relatesTo过滤条件
    if (filters.getRelatesTo() != null &&
        !filters.getRelatesTo().getFilterList().isEmpty() &&
        !TimelineStorageUtils.matchRelatesTo(timelineEntity,
            filters.getRelatesTo())) {
      return true;
    }

    // 检查isRelatedTo过滤条件
    if (filters.getIsRelatedTo() != null &&
        !filters.getIsRelatedTo().getFilterList().isEmpty() &&
        !TimelineStorageUtils.matchIsRelatedTo(timelineEntity,
            filters.getIsRelatedTo())) {
      return true;
    }

    // 检查info过滤条件
    if (filters.getInfoFilters() != null &&
        !filters.getInfoFilters().getFilterList().isEmpty() &&
        !TimelineStorageUtils.matchInfoFilters(timelineEntity,
            filters.getInfoFilters())) {
      return true;
    }

    // 检查config过滤条件
    if (filters.getConfigFilters() != null &&
        !filters.getConfigFilters().getFilterList().isEmpty() &&
        !TimelineStorageUtils.matchConfigFilters(timelineEntity,
            filters.getConfigFilters())) {
      return true;
    }

    // 检查metric过滤条件
    if (filters.getMetricFilters() != null &&
        !filters.getMetricFilters().getFilterList().isEmpty() &&
        !TimelineStorageUtils.matchMetricFilters(timelineEntity,
            filters.getMetricFilters())) {
      return true;
    }

    // 检查event过滤条件
    return filters.getEventFilters() != null &&
        !filters.getEventFilters().getFilterList().isEmpty() &&
        !TimelineStorageUtils.matchEventFilters(timelineEntity,
            filters.getEventFilters());
  }

  /**
   * 根据查询要求创建最终返回给调用方的时间线实体。
   * @param timelineEntityDocument 存储在文档库中的完整时间线实体文档
   * @param dataToRetrieve 指定需要检索的字段和过滤器
   * @return 过滤后的时间线实体
   */
  public static TimelineEntity createEntityToBeReturned(
      TimelineEntityDocument timelineEntityDocument,
      TimelineDataToRetrieve dataToRetrieve) {
    // 根据实体类型创建对应子类实例
    TimelineEntity entityToBeReturned = createTimelineEntity(
        timelineEntityDocument.getType(),
        timelineEntityDocument.fetchTimelineEntity());

    // 设置基础公共属性
    entityToBeReturned.setIdentifier(new TimelineEntity.Identifier(
        timelineEntityDocument.getType(), timelineEntityDocument.getId()));
    entityToBeReturned.setCreatedTime(
        timelineEntityDocument.getCreatedTime());
    entityToBeReturned.setInfo(timelineEntityDocument.getInfo());

    // 根据指定字段填充需要返回的内容
    if (dataToRetrieve.getFieldsToRetrieve() != null) {
      fillFields(entityToBeReturned, timelineEntityDocument,
          dataToRetrieve);
    }
    return entityToBeReturned;
  }

  /**
   * 根据配置和指标过滤器创建最终返回给调用方的时间线实体。
   * @param timelineEntityDocument 存储在文档库中的完整时间线实体文档
   * @param confsToRetrieve 配置过滤器
   * @param metricsToRetrieve 指标过滤器
   * @return 过滤后的时间线实体
   */
  public static TimelineEntity createEntityToBeReturned(
      TimelineEntityDocument timelineEntityDocument,
      TimelineFilterList confsToRetrieve,
      TimelineFilterList metricsToRetrieve) {
    TimelineEntity timelineEntity = timelineEntityDocument
        .fetchTimelineEntity();
    // 应用配置过滤器
    if (confsToRetrieve != null) {
      timelineEntity.setConfigs(DocumentStoreUtils.applyConfigFilter(
          confsToRetrieve, timelineEntity.getConfigs()));
    }
    // 应用指标过滤器
    if (metricsToRetrieve != null) {
      timelineEntity.setMetrics(DocumentStoreUtils.transformMetrics(
          metricsToRetrieve, timelineEntityDocument.getMetrics()));
    }
    return timelineEntity;
  }

  /**
   * 根据实体类型创建对应时间线实体子类实例。
   * @param type 实体类型
   * @param timelineEntity 原始时间线实体
   * @return 对应子类的新实例
   */
  private static TimelineEntity createTimelineEntity(String type,
      TimelineEntity timelineEntity) {
    switch (TimelineEntityType.valueOf(type)) {
    case YARN_APPLICATION:
      return new ApplicationEntity();
    case YARN_FLOW_RUN:
      return new FlowRunEntity();
    case YARN_FLOW_ACTIVITY:
      FlowActivityEntity flowActivityEntity =
          (FlowActivityEntity) timelineEntity;
      FlowActivityEntity newFlowActivity = new FlowActivityEntity();
      newFlowActivity.addFlowRuns(flowActivityEntity.getFlowRuns());
      return newFlowActivity;
    default:
      return new TimelineEntity();
    }
  }

  /**
   * 根据需要检索的字段填充返回实体对应内容。
   * @param finalEntity 目标返回实体
   * @param entityDoc 完整实体文档
   * @param dataToRetrieve 需要检索的字段信息
   */
  private static void fillFields(TimelineEntity finalEntity,
      TimelineEntityDocument entityDoc,
      TimelineDataToRetrieve dataToRetrieve) {
    EnumSet<TimelineReader.Field> fieldsToRetrieve =
        dataToRetrieve.getFieldsToRetrieve();
    // 如果需要所有字段，替换为全字段集合
    if (fieldsToRetrieve.contains(TimelineReader.Field.ALL)) {
      fieldsToRetrieve = EnumSet.allOf(TimelineReader.Field.class);
    }
    // 遍历需要的字段逐个填充
    for (