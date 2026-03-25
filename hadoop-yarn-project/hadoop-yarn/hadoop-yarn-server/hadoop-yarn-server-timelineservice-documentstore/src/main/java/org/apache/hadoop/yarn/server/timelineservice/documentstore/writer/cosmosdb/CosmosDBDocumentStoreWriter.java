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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.cosmosdb;


import org.apache.hadoop.classification.VisibleForTesting;
import com.microsoft.azure.cosmosdb.AccessCondition;
import com.microsoft.azure.cosmosdb.AccessConditionType;
import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.SqlParameter;
import com.microsoft.azure.cosmosdb.SqlParameterCollection;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.server.timelineservice.metrics.PerNodeAggTimelineCollectorMetrics;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.DocumentStoreUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.CollectionType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEntityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowactivity.FlowActivityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowrun.FlowRunDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.DocumentStoreWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Azure Cosmos DB 实现的文档存储写入器，负责将时间线数据写入Cosmos DB文档库。
 * 实现了{@link DocumentStoreWriter}接口，是时间线服务对Cosmos DB的写入层。
 */
public class CosmosDBDocumentStoreWriter<TimelineDoc extends TimelineDocument>
    implements DocumentStoreWriter<TimelineDoc> {

  private static final Logger LOG = LoggerFactory
      .getLogger(CosmosDBDocumentStoreWriter.class);

  private final String databaseName;
  private static final PerNodeAggTimelineCollectorMetrics METRICS =
      PerNodeAggTimelineCollectorMetrics.getInstance();

  private static AsyncDocumentClient client;
  // 创建与集合类型数量相等大小的线程池
  private ExecutorService executorService =
      Executors.newFixedThreadPool(CollectionType.values().length);
  private Scheduler schedulerForBlockingWork =
      Schedulers.from(executorService);

  private static final String DATABASE_LINK = "/dbs/%s";
  private static final String COLLECTION_LINK = DATABASE_LINK + "/colls/%s";
  private static final String DOCUMENT_LINK = COLLECTION_LINK + "/docs/%s";
  private static final String ID = "@id";
  private static final String QUERY_COLLECTION_IF_EXISTS = "SELECT * FROM r " +
      "where r.id = " + ID;

  /**
   * 构造函数，从配置初始化Cosmos DB写入器。
   * @param conf Hadoop配置对象
   */
  public CosmosDBDocumentStoreWriter(Configuration conf) {
    LOG.info("Initializing Cosmos DB DocumentStoreWriter...");
    databaseName = DocumentStoreUtils.getCosmosDBDatabaseName(conf);
    initCosmosDBClient(conf);
  }

  /**
   * 单例模式初始化Cosmos DB异步客户端，添加JVM关闭钩子。
   * @param conf Hadoop配置对象
   */
  private synchronized void initCosmosDBClient(Configuration conf) {
    // 保证Cosmos DB异步客户端单例
    if (client == null) {
      LOG.info("Creating Cosmos DB Writer Async Client...");
      client = DocumentStoreUtils.createCosmosDBAsyncClient(conf);
      addShutdownHook();
    }
  }

  @Override
  public void createDatabase() {
    // 尝试读取已存在的数据库
    Observable<ResourceResponse<Database>> databaseReadObs =
        client.readDatabase(String.format(DATABASE_LINK, databaseName), null);

    Observable<ResourceResponse<Database>> databaseExistenceObs =
        databaseReadObs
            .doOnNext(databaseResourceResponse ->
                LOG.info("Database {} already exists.", databaseName))
            .onErrorResumeNext(throwable -> {
              // 读取失败则判断是否是404（数据库不存在）
              if (throwable instanceof DocumentClientException) {
                DocumentClientException de =
                    (DocumentClientException) throwable;
                if (de.getStatusCode() == 404) {
                  // 数据库不存在，创建新数据库
                  LOG.info("Creating new Database : {}", databaseName);

                  Database dbDefinition = new Database();
                  dbDefinition.setId(databaseName);

                  return client.createDatabase(dbDefinition, null);
                }
              }
              // 非404错误，向上抛出异常
              LOG.error("Reading database : {} if it exists failed.",
                  databaseName, throwable);
              return Observable.error(throwable);
            });
    // 等待操作完成
    databaseExistenceObs.toCompletable().await();
  }

  @Override
  public void createCollection(final String collectionName) {
    LOG.info("Creating Timeline Collection : {} for Database : {}",
        collectionName, databaseName);
    // 查询集合是否已存在
    client.queryCollections(String.format(DATABASE_LINK, databaseName),
        new SqlQuerySpec(QUERY_COLLECTION_IF_EXISTS,
            new SqlParameterCollection(
                new SqlParameter(ID, collectionName))), null)
        .single() // 结果应为单页
        .flatMap((Func1<FeedResponse<DocumentCollection>, Observable<?>>)
            page -> {
            if (page.getResults().isEmpty()) {
              // 集合不存在，创建新集合
              DocumentCollection collection = new DocumentCollection();
              collection.setId(collectionName);
              LOG.info("Creating collection {}", collectionName);
              return client.createCollection(
                  String.format(DATABASE_LINK, databaseName),
                  collection, null);
            } else {
              // 集合已存在，无需操作
              LOG.info("Collection {} already exists.", collectionName);
              return Observable.empty();
            }
          })
        .doOnError(throwable -> LOG.error("Unable to create collection : {}",
            collectionName, throwable))
        .toCompletable().await();
  }

  @Override
  public void writeDocument(final TimelineDoc timelineDoc,
      final CollectionType collectionType) {
    LOG.debug("Upserting document under collection : {} with  entity type : " +
        "{} under Database {}", databaseName, timelineDoc.getType(),
        collectionType.getCollectionName());
    boolean succeeded = false;
    long startTime = Time.monotonicNow();
    try {
      upsertDocument(collectionType, timelineDoc);
      succeeded = true;
    } catch (Exception e) {
      LOG.error("Unable to perform upsert for Document Id : {} under " +
          "Collection : {} under Database {}", timelineDoc.getId(),
          collectionType.getCollectionName(), databaseName, e);
    } finally {
      // 统计写入延迟和成功率指标
      long latency = Time.monotonicNow() - startTime;
      METRICS.addPutEntitiesLatency(latency, succeeded);
    }
  }

  /**
   * 更新或插入文档到Cosmos DB，处理冲突重试。
   * @param collectionType 集合类型
   * @param timelineDoc 待写入的时间线文档
   */
  @SuppressWarnings("unchecked")
  private void upsertDocument(final  CollectionType collectionType,
      final TimelineDoc timelineDoc) {
    final String collectionLink = String.format(COLLECTION_LINK, databaseName,
        collectionType.getCollectionName());
    RequestOptions requestOptions  = new RequestOptions();
    AccessCondition accessCondition = new AccessCondition();
    StringBuilder eTagStrBuilder = new StringBuilder();

    // 基于已有文档合并更新，获取最新ETag
    final TimelineDoc updatedTimelineDoc = applyUpdatesOnPrevDoc(collectionType,
        timelineDoc, eTagStrBuilder);

    // 设置IfMatch条件，保证并发更新一致性
    accessCondition.setCondition(eTagStrBuilder.toString());
    accessCondition.setType(AccessConditionType.IfMatch);
    requestOptions.setAccessCondition(accessCondition);

    // 异步执行upsert，阻塞等待结果
    ResourceResponse<Document> resourceResponse =
        client.upsertDocument(collectionLink, updatedTimelineDoc,
            requestOptions, true)
            .subscribeOn(schedulerForBlockingWork)
            .doOnError(throwable ->
                LOG.error("Error while upserting Collection : {} " +
                    "with Doc Id : {} under Database : {}",
                collectionType.getCollectionName(),
                updatedTimelineDoc.getId(), databaseName, throwable))
            .toBlocking()
            .single();

    // 冲突时重试
    if (resourceResponse.getStatusCode() == 409) {
      LOG.warn("There was a conflict while upserting, hence retrying...",
          resourceResponse);
      upsertDocument(collectionType, updatedTimelineDoc);
    } else if (resourceResponse.getStatusCode() >= 200 && resourceResponse
        .getStatusCode() < 300) {
      // 写入成功日志
      LOG.debug("Successfully wrote doc with id : {} and type : {} under " +
          "Database : {}", timelineDoc.getId(), timelineDoc.getType(),
          databaseName);
    }
  }

  /**
   * 读取已有文档合并更新，提取最新ETag。
   * @param collectionType 集合类型
   * @param timelineDoc 输入文档
   * @param eTagStrBuilder 输出参数，存储最新ETag
   * @return 合并更新后的文档
   */
  @VisibleForTesting
  @SuppressWarnings("unchecked")
  TimelineDoc applyUpdatesOnPrevDoc(CollectionType collectionType,
      TimelineDoc timelineDoc, StringBuilder eTagStrBuilder) {
    TimelineDoc prevDocument = fetchLatestDoc(collectionType,
        timelineDoc.getId(), eTagStrBuilder);
    if (prevDocument != null) {
      // 将新文档内容合并到已有文档
      prevDocument.merge(timelineDoc);
      timelineDoc = prevDocument;
    }
    return timelineDoc;
  }

  /**
   * 从Cosmos DB读取指定ID的最新文档，提取ETag。
   * @param collectionType 集合类型
   * @param documentId 文档ID
   * @param eTagStrBuilder 输出参数，存储文档ETag
   * @return 读取到的文档，不存在则返回null
   */
  @VisibleForTesting
  @SuppressWarnings("unchecked")
  TimelineDoc fetchLatestDoc(final CollectionType collectionType,
      final String documentId, StringBuilder eTagStrBuilder) {
    final String documentLink = String.format(DOCUMENT_LINK, databaseName,
        collectionType.getCollectionName(), documentId);
    try {
      // 读取文档
      Document latestDocument = client.readDocument(documentLink, new
          RequestOptions()).toBlocking().single().getResource();
      TimelineDoc timelineDoc;
      // 根据集合类型反序列化为对应文档类
      switch (collectionType) {
      case FLOW_RUN:
        timelineDoc = (TimelineDoc) latestDocument.toObject(
            FlowRunDocument.class);
        break;
      case FLOW_ACTIVITY:
        timelineDoc = (TimelineDoc) latestDocument.toObject(FlowActivityDocument
            .class);
        break;
      default:
        timelineDoc = (TimelineDoc) latestDocument.toObject(
            TimelineEntityDocument.class);
      }
      // 保存ETag用于乐观锁
      eTagStrBuilder.append(latestDocument.getETag());
      return timelineDoc;
    } catch (Exception e) {
      // 文档不存在视为正常情况，返回null
      LOG.debug("No previous Document found with id : {} for Collection" +
          " : {} under Database : {}", documentId, collectionType
          .getCollectionName(), databaseName);
      return null;
    }
  }

  @Override
  public synchronized void close() {
    if (client != null) {
      LOG.info("Closing Cosmos DB Writer Async Client...");
      client.close();
      client = null;
    }
  }

  /**
   * 添加JVM关闭钩子，退出时关闭线程池。
   */
  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new SubjectInheritingThread(() -> {
      if (executorService != null) {
        executorService.shutdown();
      }
    }));
  }
}