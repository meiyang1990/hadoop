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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.CollectionType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreFactory;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;
import org.apache.hadoop.yarn.server.timelineservice.metrics.PerNodeAggTimelineCollectorMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 通用时间线数据集合写入器，支持向不同的文档存储后端写入多种类型的时间线文档
 * 基于缓冲区攒批提高写入性能，通过定期刷写和缓冲区满异步刷写保障数据及时性
 * @param <Document> 泛型，限定为TimelineDocument或其子类
 */
public class TimelineCollectionWriter<Document extends TimelineDocument> {

  private static final Logger LOG = LoggerFactory
      .getLogger(TimelineCollectionWriter.class);

  private final static String DOCUMENT_BUFFER_SIZE_CONF =
      "yarn.timeline-service.document-buffer.size";
  private static final int DEFAULT_BUFFER_SIZE = 1024;
  private static final int AWAIT_TIMEOUT_SECS = 5;
  private static final PerNodeAggTimelineCollectorMetrics METRICS =
      PerNodeAggTimelineCollectorMetrics.getInstance();

  // 当前写入的集合类型
  private final CollectionType collectionType;
  // 底层文档存储写入器
  private final DocumentStoreWriter<Document> documentStoreWriter;
  // 文档内存缓冲区，按文档ID缓存待写入文档
  private final Map<String, Document> documentsBuffer;
  // 缓冲区最大容量
  private final int maxBufferSize;
  // 定期刷写缓冲区的定时执行器
  private final ScheduledExecutorService scheduledDocumentsFlusher;
  // 缓冲区满时异步刷写的执行器
  private final ExecutorService documentsBufferFullFlusher;

  /**
   * 构造时间线集合写入器，初始化缓冲区和后台刷写线程
   * @param collectionType 要写入的集合类型
   * @param conf Yarn配置对象
   * @throws YarnException 初始化文档存储写入器失败时抛出异常
   */
  public TimelineCollectionWriter(CollectionType collectionType,
      Configuration conf) throws YarnException {
    LOG.info("Initializing TimelineCollectionWriter for collection type : {}",
        collectionType);
    // 从配置读取刷写间隔，使用默认值兜底
    int flushIntervalSecs = conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_WRITER_FLUSH_INTERVAL_SECONDS,
        YarnConfiguration
            .DEFAULT_TIMELINE_SERVICE_WRITER_FLUSH_INTERVAL_SECONDS);
    // 从配置读取缓冲区大小，使用默认值兜底
    maxBufferSize = conf.getInt(DOCUMENT_BUFFER_SIZE_CONF, DEFAULT_BUFFER_SIZE);
    // 初始化缓冲区
    documentsBuffer = new HashMap<>(maxBufferSize);
    this.collectionType = collectionType;
    // 通过工厂创建对应存储类型的写入器
    documentStoreWriter = DocumentStoreFactory.createDocumentStoreWriter(conf);
    // 创建单线程定时刷写执行器
    scheduledDocumentsFlusher = Executors.newSingleThreadScheduledExecutor();
    // 启动固定间隔定时刷写任务
    scheduledDocumentsFlusher.scheduleAtFixedRate(this::flush,
        flushIntervalSecs, flushIntervalSecs, TimeUnit.SECONDS);
    // 创建单线程异步刷写执行器，用于缓冲区满时异步刷写
    documentsBufferFullFlusher = Executors.newSingleThreadExecutor();
  }

  /**
   * 写入单个时间线文档，先写入缓冲区，满足刷写条件时触发异步刷写
   * @param timelineDocument 待写入的时间线文档
   */
  @SuppressWarnings("unchecked")
  public void writeDocument(Document timelineDocument) {
    /*
     * The DocumentBuffer is used to buffer the most frequently used
     * documents for performing upserts on them, whenever either due to
     * buffer gets fulled or the scheduledDocumentsFlusher
     * invokes flush() periodically, all the buffered documents would be written
     * to DocumentStore in a background thread.
     */
    // 记录方法开始时间，用于统计延迟
    long startTime = Time.monotonicNow();

    synchronized(documentsBuffer) {
      // 缓冲区已满，复制缓冲区后触发异步刷写
      if (documentsBuffer.size() == maxBufferSize) {
        final Map<String, Document> flushedBuffer = copyToFlushBuffer();
        // 后台线程异步刷写缓冲区所有文档
        documentsBufferFullFlusher.execute(() -> flush(flushedBuffer));
      }
      // 获取当前文档ID已缓存的文档
      Document prevDocument = documentsBuffer.get(timelineDocument.getId());
      // 文档已存在，合并新数据到已有文档
      if (prevDocument != null) {
        prevDocument.merge(timelineDocument);
      } else { // 文档不存在，使用新文档
        prevDocument = timelineDocument;
      }
      // 更新缓冲区
      documentsBuffer.put(prevDocument.getId(), prevDocument);
    }
    // 记录异步写入延迟指标
    METRICS.addAsyncPutEntitiesLatency(Time.monotonicNow() - startTime,
        true);
  }

  /**
   * 将当前缓冲区内容复制到新刷写缓冲区，并清空原缓冲区
   * @return 待刷写的缓冲区内容
   */
  private Map<String, Document> copyToFlushBuffer() {
    Map<String, Document> flushBuffer = new HashMap<>();
    synchronized(documentsBuffer) {
      if (documentsBuffer.size() > 0) {
        // 将所有文档复制到新缓冲区
        flushBuffer.putAll(documentsBuffer);
        // 清空原缓冲区
        documentsBuffer.clear();
      }
    }
    return flushBuffer;
  }

  /**
   * 将刷写缓冲区中的所有文档写入底层文档存储
   * @param flushBuffer 待刷写的缓冲区
   */
  private void flush(Map<String, Document> flushBuffer) {
    for (Document document : flushBuffer.values()) {
      documentStoreWriter.writeDocument(document, collectionType);
    }
  }

  /**
   * 刷写当前缓冲区所有文档到存储
   */
  public void flush() {
    flush(copyToFlushBuffer());
  }

  /**
   * 关闭写入器，停止后台线程并刷写剩余数据
   * @throws Exception 关闭底层写入器时可能抛出异常
   */
  public void close() throws Exception {
    // 停止定时刷写和异步刷写线程接收新任务
    scheduledDocumentsFlusher.shutdown();
    documentsBufferFullFlusher.shutdown();

    // 刷写缓冲区中剩余的所有文档
    flush();

    // 等待定时刷写线程终止，最多等待指定超时时间
    scheduledDocumentsFlusher.awaitTermination(
        AWAIT_TIMEOUT_SECS, TimeUnit.SECONDS);
    // 等待异步刷写线程终止，最多等待指定超时时间
    documentsBufferFullFlusher.awaitTermination(
        AWAIT_TIMEOUT_SECS, TimeUnit.SECONDS);
    // 关闭底层文档存储写入器
    documentStoreWriter.close();
  }
}