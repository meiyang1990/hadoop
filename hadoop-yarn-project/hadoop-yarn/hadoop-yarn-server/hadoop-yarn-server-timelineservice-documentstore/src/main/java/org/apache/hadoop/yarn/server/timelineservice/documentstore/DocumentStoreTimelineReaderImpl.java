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
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timeline.TimelineHealth;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.reader.TimelineCollectionReader;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEntityDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 基于文档存储的时间线数据读取器实现，负责从后端文档存储读取时间线实体信息。
 * 根据配置的{@link DocumentStoreVendor}，从对应后端存储读取文档数据。
 */
public class DocumentStoreTimelineReaderImpl
    extends AbstractService implements TimelineReader {

  private static final Logger LOG = LoggerFactory
      .getLogger(DocumentStoreTimelineReaderImpl.class);

  // 时间线集合读取器，负责具体文档读取逻辑
  private TimelineCollectionReader collectionReader;

  /**
   * 构造函数，初始化服务名称。
   */
  public DocumentStoreTimelineReaderImpl() {
    super(DocumentStoreTimelineReaderImpl.class.getName());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    // 从配置中获取文档存储类型
    DocumentStoreVendor storeType = DocumentStoreUtils.getStoreVendor(conf);
    LOG.info("Initializing Document Store Reader for : " + storeType);
    // 初始化集合读取器
    collectionReader = new TimelineCollectionReader(conf);
    super.serviceInit(conf);
  }

  @Override
  public void serviceStop() throws Exception {
    super.serviceStop();
    LOG.info("Stopping Document Timeline Store reader...");
    // 关闭集合读取器，释放资源
    collectionReader.close();
  }

  /**
   * 根据上下文和检索要求获取单个时间线实体。
   * @param context 读取器上下文，包含实体标识信息
   * @param dataToRetrieve 需要检索的数据项配置
   * @return 符合要求的时间线实体
   * @throws IOException 读取失败时抛出异常
   */
  public TimelineEntity getEntity(TimelineReaderContext context,
      TimelineDataToRetrieve dataToRetrieve) throws IOException {
    TimelineEntityDocument timelineEntityDoc;
    // 根据实体类型分流处理
    switch (TimelineEntityType.valueOf(context.getEntityType())) {
    case YARN_FLOW_ACTIVITY:
    case YARN_FLOW_RUN:
      // 读取指定上下文对应的实体文档
      timelineEntityDoc =
          collectionReader.readDocument(context);
      // 按要求提取指定配置和指标，返回结果实体
      return DocumentStoreUtils.createEntityToBeReturned(
          timelineEntityDoc, dataToRetrieve.getConfsToRetrieve(),
          dataToRetrieve.getMetricsToRetrieve());
    default:
      // 其他类型实体统一读取
      timelineEntityDoc =
          collectionReader.readDocument(context);
    }
    // 按检索要求提取对应字段，返回结果实体
    return DocumentStoreUtils.createEntityToBeReturned(
        timelineEntityDoc, dataToRetrieve);
  }

  /**
   * 根据上下文、过滤条件获取一批时间线实体。
   * @param context 读取器上下文
   * @param filters 实体过滤条件
   * @param dataToRetrieve 需要检索的数据项配置
   * @return 符合条件的时间线实体集合
   * @throws IOException 读取失败时抛出异常
   */
  public Set<TimelineEntity> getEntities(TimelineReaderContext context,
      TimelineEntityFilters filters, TimelineDataToRetrieve dataToRetrieve)
      throws IOException {
    // 从文档存储读取限制数量的实体文档
    List<TimelineEntityDocument> entityDocs =
        collectionReader.readDocuments(context, filters.getLimit());

    // 应用过滤条件，返回最终结果
    return applyFilters(filters, dataToRetrieve, entityDocs);
  }

  /**
   * 获取当前上下文中所有实体类型集合。
   * @param context 读取器上下文
   * @return 实体类型名称集合
   */
  public Set<String> getEntityTypes(TimelineReaderContext context) {
    return collectionReader.fetchEntityTypes(context);
  }

  @Override
  public TimelineHealth getHealthStatus() {
    // 检查读取器是否已初始化，返回对应健康状态
    if (collectionReader != null) {
      return new TimelineHealth(TimelineHealth.TimelineHealthStatus.RUNNING,
          "");
    } else {
      return new TimelineHealth(
          TimelineHealth.TimelineHealthStatus.CONNECTION_FAILURE,
          "Timeline store reader not initialized.");
    }
  }

  /**
   * 对读取到的实体文档应用所有过滤条件，转换为结果实体集合。
   * @param filters 实体过滤条件
   * @param dataToRetrieve 需要检索的数据项配置
   * @param entityDocs 从存储读取到的原始实体文档列表
   * @return 过滤后符合要求的时间线实体集合
   * @throws IOException 处理失败时抛出异常
   */
  private Set<TimelineEntity> applyFilters(TimelineEntityFilters filters,
      TimelineDataToRetrieve dataToRetrieve,
      List<TimelineEntityDocument> entityDocs) throws IOException {
    Set<TimelineEntity> timelineEntities = new HashSet<>();
    // 遍历所有原始实体文档
    for (TimelineEntityDocument entityDoc : entityDocs) {
      // 从文档中提取时间线实体对象
      final TimelineEntity timelineEntity = entityDoc.fetchTimelineEntity();

      // 如果不匹配过滤条件，跳过该实体
      if (DocumentStoreUtils.isFilterNotMatching(filters, timelineEntity)) {
        continue;
      }

      // 按检索要求提取字段，添加到结果集合
      TimelineEntity entityToBeReturned = DocumentStoreUtils
          .createEntityToBeReturned(entityDoc, dataToRetrieve);
      timelineEntities.add(entityToBeReturned);
    }
    return timelineEntities;
  }
}