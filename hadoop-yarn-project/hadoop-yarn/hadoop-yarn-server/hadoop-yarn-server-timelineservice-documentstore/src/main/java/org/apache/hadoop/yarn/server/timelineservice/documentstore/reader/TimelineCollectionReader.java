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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.DocumentStoreUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.CollectionType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEntityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowactivity.FlowActivityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowactivity.FlowActivitySubDoc;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowrun.FlowRunDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreFactory;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * 文档存储的通用集合读取器，负责从指定文档存储后端读取不同类型的时间线集合文档。
 * 支持通用实体、流运行、流活动三种类型集合的读取。
 */
public  class TimelineCollectionReader {

  private static final Logger LOG = LoggerFactory
      .getLogger(TimelineCollectionReader.class);

  // 通用时间线实体文档读取器
  private final DocumentStoreReader<TimelineEntityDocument>
      genericEntityDocReader;
  // 流运行文档读取器
  private final DocumentStoreReader<FlowRunDocument>
      flowRunDocReader;
  // 流活动文档读取器
  private final DocumentStoreReader<FlowActivityDocument>
      flowActivityDocReader;

  /**
   * 构造函数，初始化各类型文档读取器。
   * @param conf Hadoop配置对象
   * @throws YarnException 初始化读取器失败时抛出异常
   */
  public TimelineCollectionReader(
      Configuration conf) throws YarnException {
    LOG.info("Initializing TimelineCollectionReader...");
    genericEntityDocReader = DocumentStoreFactory
        .createDocumentStoreReader(conf);
    flowRunDocReader = DocumentStoreFactory
        .createDocumentStoreReader(conf);
    flowActivityDocReader = DocumentStoreFactory
        .createDocumentStoreReader(conf);
  }

  /**
   * 从文档存储后端读取单个指定类型的时间线文档。
   * @param context 时间线读取上下文，包含查询条件信息
   * @return 读取到的时间线实体文档
   * @throws IOException 读取过程发生IO错误时抛出
   */
  public TimelineEntityDocument readDocument(
      TimelineReaderContext context) throws IOException {
    LOG.debug("Fetching document for entity type {}", context.getEntityType());
    // 根据实体类型选择对应的读取逻辑
    switch (TimelineEntityType.valueOf(context.getEntityType())) {
    case YARN_APPLICATION:
      // YARN应用实体从应用集合读取
      return genericEntityDocReader.readDocument(
          CollectionType.APPLICATION.getCollectionName(), context,
           TimelineEntityDocument.class);
    case YARN_FLOW_RUN:
      // YARN流运行实体从流运行集合读取，转换为时间线实体文档
      FlowRunDocument flowRunDoc = flowRunDocReader.readDocument(
          CollectionType.FLOW_RUN.getCollectionName(), context,
          FlowRunDocument.class);
      FlowRunEntity flowRun = createFlowRunEntity(flowRunDoc);
      return new TimelineEntityDocument(flowRun);
    case YARN_FLOW_ACTIVITY:
      // YARN流活动实体从流活动集合读取，转换为时间线实体文档
      FlowActivityDocument flowActivityDoc = flowActivityDocReader
          .readDocument(CollectionType.FLOW_RUN.getCollectionName(),
              context, FlowActivityDocument.class);
      FlowActivityEntity flowActivity = createFlowActivityEntity(context,
          flowActivityDoc);
      return  new TimelineEntityDocument(flowActivity);
    default:
      // 其他实体默认从通用实体集合读取
      return genericEntityDocReader.readDocument(
          CollectionType.ENTITY.getCollectionName(), context,
          TimelineEntityDocument.class);
    }
  }

  /**
   * 从文档存储后端批量读取指定类型的时间线文档。
   * @param context 时间线读取上下文，包含查询条件信息
   * @param documentsSize 批量读取的最大文档数量限制
   * @return 读取到的时间线实体文档列表
   * @throws IOException 读取过程发生IO错误时抛出
   */
  public List<TimelineEntityDocument> readDocuments(
      TimelineReaderContext context, long documentsSize) throws IOException {
    List<TimelineEntityDocument> entityDocs = new ArrayList<>();
    LOG.debug("Fetching documents for entity type {}", context.getEntityType());
    // 根据实体类型选择对应的批量读取逻辑
    switch (TimelineEntityType.valueOf(context.getEntityType())) {
    case YARN_APPLICATION:
      // YARN应用实体从应用集合批量读取
      return genericEntityDocReader.readDocumentList(
          CollectionType.APPLICATION.getCollectionName(), context,
           TimelineEntityDocument.class, documentsSize);
    case YARN_FLOW_RUN:
      // YARN流运行实体从流运行集合批量读取，逐个转换为时间线实体文档
      List<FlowRunDocument> flowRunDocs = flowRunDocReader.readDocumentList(
          CollectionType.FLOW_RUN.getCollectionName(), context,
               FlowRunDocument.class, documentsSize);
      for (FlowRunDocument flowRunDoc : flowRunDocs) {
        entityDocs.add(new TimelineEntityDocument(createFlowRunEntity(
            flowRunDoc)));
      }
      return entityDocs;
    case YARN_FLOW_ACTIVITY:
      // YARN流活动实体从流活动集合批量读取，逐个转换为时间线实体文档
      List<FlowActivityDocument> flowActivityDocs = flowActivityDocReader
          .readDocumentList(CollectionType.FLOW_ACTIVITY.getCollectionName(),
              context, FlowActivityDocument.class, documentsSize);
      for(FlowActivityDocument flowActivityDoc : flowActivityDocs) {
        entityDocs.add(new TimelineEntityDocument(
            createFlowActivityEntity(context, flowActivityDoc)));
      }
      return entityDocs;
    default:
      // 其他实体默认从通用实体集合批量读取
      return genericEntityDocReader.readDocumentList(
          CollectionType.ENTITY.getCollectionName(), context,
          TimelineEntityDocument.class, documentsSize);
    }
  }

  /**
   * 获取指定应用下所有实体类型列表。
   * @param context 时间线读取上下文，包含应用ID信息
   * @return 实体类型集合
   */
  public Set<String> fetchEntityTypes(
      TimelineReaderContext context) {
    LOG.debug("Fetching all entity-types for appId : {}", context.getAppId());
    return genericEntityDocReader.fetchEntityTypes(
        CollectionType.ENTITY.getCollectionName(), context);
  }

  /**
   * 从流活动文档构造流活动实体对象，转换存储模型为业务模型。
   * @param context 时间线读取上下文
   * @param flowActivityDoc 从文档存储读取的流活动文档
   * @return 构造完成的流活动实体
   */
  private FlowActivityEntity createFlowActivityEntity(
      TimelineReaderContext context, FlowActivityDocument flowActivityDoc) {
    FlowActivityEntity flowActivity = new FlowActivityEntity(
        context.getClusterId(), flowActivityDoc.getDayTimestamp(),
        flowActivityDoc.getUser(), flowActivityDoc.getFlowName());
    flowActivity.setId(flowActivityDoc.getId());
    // 遍历文档中关联的所有流运行，添加到流活动实体中
    for (FlowActivitySubDoc activity : flowActivityDoc
        .getFlowActivities()) {
      FlowRunEntity flowRunEntity = new FlowRunEntity();
      flowRunEntity.setUser(flowActivityDoc.getUser());
      flowRunEntity.setName(activity.getFlowName());
      flowRunEntity.setRunId(activity.getFlowRunId());
      flowRunEntity.setVersion(activity.getFlowVersion());
      flowRunEntity.setId(flowRunEntity.getId());
      flowActivity.addFlowRun(flowRunEntity);
    }
    flowActivity.getInfo().put(TimelineReaderUtils.FROMID_KEY,
        flowActivityDoc.getId());
    flowActivity.setCreatedTime(flowActivityDoc.getDayTimestamp());
    return flowActivity;
  }

  /**
   * 从流运行文档构造流运行实体对象，转换存储模型为业务模型。
   * @param flowRunDoc 从文档存储读取的流运行文档
   * @return 构造完成的流运行实体
   */
  private FlowRunEntity createFlowRunEntity(FlowRunDocument flowRunDoc) {
    FlowRunEntity flowRun = new FlowRunEntity();
    flowRun.setRunId(flowRunDoc.getFlowRunId());
    flowRun.setUser(flowRunDoc.getUsername());
    flowRun.setName(flowRunDoc.getFlowName());

    // 设置流运行开始时间
    if (flowRunDoc.getMinStartTime() > 0) {
      flowRun.setStartTime(flowRunDoc.getMinStartTime());
    }

    // 设置流运行结束时间（如果存在）
    if (flowRunDoc.getMaxEndTime() > 0) {
      flowRun.setMaxEndTime(flowRunDoc.getMaxEndTime());
    }

    // 设置流版本（如果存在）
    if (!DocumentStoreUtils.isNullOrEmpty(flowRunDoc.getFlowVersion())) {
      flowRun.setVersion(flowRunDoc.getFlowVersion());
    }
    flowRun.setMetrics(flowRunDoc.fetchTimelineMetrics());
    flowRun.setId(flowRunDoc.getId());
    flowRun.getInfo().put(TimelineReaderUtils.FROMID_KEY, flowRunDoc.getId());
    return flowRun;
  }

  /**
   * 关闭所有读取器，释放资源。
   * @throws Exception 关闭过程发生异常时抛出
   */
  public void close() throws Exception {
    genericEntityDocReader.close();
    flowRunDocReader.close();
    flowActivityDocReader.close();
  }
}