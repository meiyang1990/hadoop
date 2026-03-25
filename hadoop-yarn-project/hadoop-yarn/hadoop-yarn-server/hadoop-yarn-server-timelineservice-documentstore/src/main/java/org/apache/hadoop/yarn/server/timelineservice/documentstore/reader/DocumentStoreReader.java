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
 * Unless required by applicable law or agreed to writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.timelineservice.documentstore.reader;

import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.NoDocumentFoundException;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;

import java.util.List;
import java.util.Set;

// 文档存储读取器接口，所有DocumentStoreVendor都必须实现此接口以创建其后端读取器
/**
 * 文档存储读取器接口，每种文档存储厂商需要实现该接口，提供对后端存储的读取能力。
 * 
 * @param <Document> 继承自TimelineDocument的具体文档类型
 */
public interface DocumentStoreReader<Document extends TimelineDocument>
    extends AutoCloseable {

  /**
   * 读取单个文档。
   * 
   * @param collectionName 集合名称
   * @param context 时间线读取上下文
   * @param documentClass 文档类对象
   * @return 读取到的文档
   * @throws NoDocumentFoundException 未找到文档时抛出
   */
  Document readDocument(String collectionName, TimelineReaderContext context,
      Class<Document> documentClass) throws NoDocumentFoundException;

  /**
   * 批量读取文档列表。
   * 
   * @param collectionName 集合名称
   * @param context 时间线读取上下文
   * @param documentClass 文档类对象
   * @param documentsSize 最大读取文档数量
   * @return 读取到的文档列表
   * @throws NoDocumentFoundException 未找到任何文档时抛出
   */
  List<Document> readDocumentList(String collectionName,
      TimelineReaderContext context, Class<Document> documentClass,
      long documentsSize) throws NoDocumentFoundException;

  /**
   * 获取指定集合中所有实体类型。
   * 
   * @param collectionName 集合名称
   * @param context 时间线读取上下文
   * @return 所有实体类型的集合
   */
  Set<String> fetchEntityTypes(String collectionName,
      TimelineReaderContext context);
}