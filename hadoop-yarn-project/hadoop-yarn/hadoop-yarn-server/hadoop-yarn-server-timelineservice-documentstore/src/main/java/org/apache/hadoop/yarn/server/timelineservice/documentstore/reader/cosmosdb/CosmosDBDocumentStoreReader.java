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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.reader.cosmosdb;

import org.apache.hadoop.classification.VisibleForTesting;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.DocumentStoreUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.NoDocumentFoundException;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.reader.DocumentStoreReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Azure CosmosDB 文档存储实现的时间线数据读取器，实现 DocumentStoreReader 接口。
 * 负责从Azure CosmosDB查询读取时间线服务实体数据。
 */
public class CosmosDBDocumentStoreReader<TimelineDoc extends TimelineDocument>
    implements DocumentStoreReader<TimelineDoc> {

  private static final Logger LOG = LoggerFactory
      .getLogger(CosmosDBDocumentStoreReader.class);
  private static final int DEFAULT_DOCUMENTS_SIZE = 1;

  private static AsyncDocumentClient client;
  private final String databaseName;
  private final static String COLLECTION_LINK = "/dbs/%s/colls/%s";
  private final static String SELECT_TOP_FROM_COLLECTION = "SELECT TOP %d * " +
      "FROM %s c";
  private final static String SELECT_ALL_FROM_COLLECTION =
      "SELECT  * FROM %s c";
  private final static String SELECT_DISTINCT_TYPES_FROM_COLLECTION =
      "SELECT  distinct c.type FROM %s c";
  private static final String ENTITY_TYPE_COLUMN = "type";
  private final static String WHERE_CLAUSE = " WHERE ";
  private final static String AND_OPERATOR = " AND ";
  private final static String CONTAINS_FUNC_FOR_ID = " CONTAINS(c.id, \"%s\") ";
  private final static String CONTAINS_FUNC_FOR_TYPE = " CONTAINS(c.type, " +
      "\"%s\") ";
  private final static String ORDER_BY_CLAUSE = " ORDER BY c.createdTime";

  // 创建线程池，线程数为JVM可用处理器数的一半，用于异步查询
  private static ExecutorService executorService = Executors.newFixedThreadPool(
      Runtime.getRuntime().availableProcessors() / 2);
  private static Scheduler schedulerForBlockingWork =
      Schedulers.from(executorService);

  /**
   * 构造函数，从配置中初始化CosmosDB读取器。
   * @param conf Hadoop配置对象
   */
  public CosmosDBDocumentStoreReader(Configuration conf) {
    LOG.info("Initializing Cosmos DB DocumentStoreReader...");
    databaseName = DocumentStoreUtils.getCosmosDBDatabaseName(conf);
    initCosmosDBClient(conf);
  }

  /**
   * 同步初始化CosmosDB异步客户端，保证单例模式。
   * @param conf Hadoop配置对象
   */
  private synchronized void initCosmosDBClient(Configuration conf) {
    // 保证CosmosDB异步客户端单例
    if (client == null) {
      LOG.info("Creating Cosmos DB Reader Async Client...");
      client = DocumentStoreUtils.createCosmosDBAsyncClient(conf);
      addShutdownHook();
    }
  }

  @Override
  public List<TimelineDoc> readDocumentList(String collectionName,
      TimelineReaderContext context, final Class<TimelineDoc> timelineDocClass,
      long size) throws NoDocumentFoundException {
    final List<TimelineDoc> result = queryDocuments(collectionName,
        context, timelineDocClass, size);
    if (result.size() > 0) {
      return result;
    }
    throw new NoDocumentFoundException("No documents were found while " +
        "querying Collection : " + collectionName);
  }

  @Override
  public Set<String> fetchEntityTypes(String collectionName,
      TimelineReaderContext context) {
    // 构建查询去重实体类型的SQL
    StringBuilder queryStrBuilder = new StringBuilder();
    queryStrBuilder.append(
        String.format(SELECT_DISTINCT_TYPES_FROM_COLLECTION, collectionName));
    // 添加查询条件
    String sqlQuery = addPredicates(context, collectionName, queryStrBuilder);

    LOG.debug("Querying Collection : {} , with query {}", collectionName,
        sqlQuery);

    // 执行查询并提取实体类型结果
    return Sets.newHashSet(client.queryDocuments(
        String.format(COLLECTION_LINK, databaseName, collectionName),
        sqlQuery, new FeedOptions())
        .map(FeedResponse::getResults)
        .concatMap(Observable::from)
        .map(document -> String.valueOf(document.get(ENTITY_TYPE_COLUMN)))
        .toList()
        .subscribeOn(schedulerForBlockingWork)
        .toBlocking()
        .single());
  }

  @Override
  public TimelineDoc readDocument(String collectionName, TimelineReaderContext
      context, final Class<TimelineDoc> timelineDocClass)
      throws  NoDocumentFoundException {
    final List<TimelineDoc> result = queryDocuments(collectionName,
        context, timelineDocClass, DEFAULT_DOCUMENTS_SIZE);
    if(result.size() > 0) {
      return result.get(0);
    }
    throw new NoDocumentFoundException("No documents were found while " +
        "querying Collection : " + collectionName);
  }

  /**
   * 根据查询上下文构建SQL并执行查询，返回文档列表。
   * @param collectionName CosmosDB集合名称
   * @param context 时间线读取上下文
   * @param docClass 时间线文档类
   * @param maxDocumentsSize 最大返回文档数
   * @return 查询到的时间线文档列表
   */
  private List<TimelineDoc> queryDocuments(String collectionName,
      TimelineReaderContext context, final Class<TimelineDoc> docClass,
      final long maxDocumentsSize) {
    // 构建带查询条件的SQL语句
    final String sqlQuery = buildQueryWithPredicates(context, collectionName,
        maxDocumentsSize);
    LOG.debug("Querying Collection : {} , with query {}", collectionName,
        sqlQuery);

    // 执行异步查询并转换结果为目标文档类型
    return client.queryDocuments(String.format(COLLECTION_LINK,
        databaseName, collectionName), sqlQuery, new FeedOptions())
        .map(FeedResponse::getResults)
        .concatMap(Observable::from)
        .map(document -> {
          // 将CosmosDB文档转换为时间线文档对象
          TimelineDoc resultDoc = document.toObject(docClass);
          // 如果文档未设置创建时间，则从文档时间戳补全创建时间
          if (resultDoc.getCreatedTime() == 0 &&
              document.getTimestamp() != null) {
            resultDoc.setCreatedTime(document.getTimestamp().getTime());
          }
          return resultDoc;
        })
        .toList()
        .subscribeOn(schedulerForBlockingWork)
        .toBlocking()
        .single();
  }

  /**
   * 根据参数构建带查询条件的完整SQL语句。
   * @param context 时间线读取上下文
   * @param collectionName 集合名称
   * @param size 最大返回文档数，-1表示返回全部
   * @return 完整SQL语句字符串
   */
  private String buildQueryWithPredicates(TimelineReaderContext context,
      String collectionName, long size) {
    StringBuilder queryStrBuilder = new StringBuilder();
    // 根据是否限制大小选择查询全部或查询指定条数
    if (size == -1) {
      queryStrBuilder.append(String.format(SELECT_ALL_FROM_COLLECTION,
          collectionName));
    } else {
      queryStrBuilder.append(String.format(SELECT_TOP_FROM_COLLECTION, size,
          collectionName));
    }

    // 添加查询条件
    return addPredicates(context, collectionName, queryStrBuilder);
  }

  @VisibleForTesting
  /**
   * 根据上下文向SQL中添加WHERE查询条件。
   * @param context 时间线读取上下文，包含各类查询过滤条件
   * @param collectionName 集合名称
   * @param queryStrBuilder SQL构建器
   * @return 添加完条件的完整SQL语句
   */
  String addPredicates(TimelineReaderContext context,
      String collectionName, StringBuilder queryStrBuilder) {
    boolean hasPredicate = false;

    // 先添加WHERE子句开头
    queryStrBuilder.append(WHERE_CLAUSE);

    // 根据上下文非空字段依次添加匹配条件
    if (!DocumentStoreUtils.isNullOrEmpty(context.getClusterId())) {
      hasPredicate = true;
      queryStrBuilder.append(String.format(CONTAINS_FUNC_FOR_ID,
          context.getClusterId()));
    }
    if (!DocumentStoreUtils.isNullOrEmpty(context.getUserId())) {
      hasPredicate = true;
      queryStrBuilder.append(AND_OPERATOR)
          .append(String.format(CONTAINS_FUNC_FOR_ID, context.getUserId()));
    }
    if (!DocumentStoreUtils.isNullOrEmpty(context.getFlowName())) {
      hasPredicate = true;
      queryStrBuilder.append(AND_OPERATOR)
          .append(String.format(CONTAINS_FUNC_FOR_ID, context.getFlowName()));
    }
    if (!DocumentStoreUtils.isNullOrEmpty(context.getAppId())) {
      hasPredicate = true;
      queryStrBuilder.append(AND_OPERATOR)
          .append(String.format(CONTAINS_FUNC_FOR_ID, context.getAppId()));
    }
    if (!DocumentStoreUtils.isNullOrEmpty(context.getEntityId())) {
      hasPredicate = true;
      queryStrBuilder.append(AND_OPERATOR)
          .append(String.format(CONTAINS_FUNC_FOR_ID, context.getEntityId()));
    }
    if (context.getFlowRunId() != null) {
      hasPredicate = true;
      queryStrBuilder.append(AND_OPERATOR)
          .append(String.format(CONTAINS_FUNC_FOR_ID, context.getFlowRunId()));
    }
    if (!DocumentStoreUtils.isNullOrEmpty(context.getEntityType())){
      hasPredicate = true;
      queryStrBuilder.append(AND_OPERATOR)
          .append(String.format(CONTAINS_FUNC_FOR_TYPE,
              context.getEntityType()));
    }

    // 存在有效查询条件，则添加排序并返回结果
    if (hasPredicate) {
      queryStrBuilder.append(ORDER_BY_CLAUSE);
      LOG.debug("CosmosDB Sql Query with predicates : {}", queryStrBuilder);
      return queryStrBuilder.toString();
    }
    // 没有任何有效查询条件则抛出异常
    throw new IllegalArgumentException("The TimelineReaderContext does not " +
        "have enough information to query documents for Collection : " +
        collectionName);
  }

  @Override
  /**
   * 关闭CosmosDB客户端，释放资源。
   */
  public synchronized void close() {
    if (client != null) {
      LOG.info("Closing Cosmos DB Reader Async Client...");
      client.close();
      client = null;
    }
  }

  /**
   * 添加JVM关闭钩子，在进程退出时关闭线程池释放资源。
   */
  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new SubjectInheritingThread(() -> {
      if (executorService != null) {
        executorService.shutdown();
      }
    }));
  }
}