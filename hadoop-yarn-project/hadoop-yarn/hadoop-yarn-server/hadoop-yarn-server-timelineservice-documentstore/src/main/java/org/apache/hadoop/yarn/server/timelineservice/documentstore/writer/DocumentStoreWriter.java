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

import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.CollectionType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;

/**
 * 文档存储写入器抽象接口，不同文档存储厂商需要实现该接口，提供对后端存储的写入能力
 * 为时间线服务指标数据写入提供统一的存储操作抽象
 * @param <Document> 文档类型泛型
 */
public interface DocumentStoreWriter<Document> extends AutoCloseable {

  /**
   * 创建存储时间线数据的数据库
   */
  void createDatabase();

  /**
   * 创建指定名称的文档集合
   * @param collectionName 集合名称
   */
  void createCollection(String collectionName);

  /**
   * 将单个文档写入对应类型的集合中
   * @param document 待写入文档
   * @param collectionType 集合类型（区分不同类型的时间线数据）
   */
  void writeDocument(Document document, CollectionType collectionType);
}