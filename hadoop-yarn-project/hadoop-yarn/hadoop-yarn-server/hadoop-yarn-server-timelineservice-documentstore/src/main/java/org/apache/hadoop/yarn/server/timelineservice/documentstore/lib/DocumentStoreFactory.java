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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.lib;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.reader.DocumentStoreReader;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.reader.cosmosdb.CosmosDBDocumentStoreReader;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.cosmosdb.CosmosDBDocumentStoreWriter;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.DocumentStoreUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.DocumentStoreWriter;

import static org.apache.hadoop.yarn.server.timelineservice.documentstore.DocumentStoreUtils.getStoreVendor;

// 文档存储工厂类，提供创建时间线文档存储读写器的工厂方法。根据配置的DocumentStoreVendor实例化相应的读写器
/**
 * 时间线文档存储读写器工厂类，根据配置的文档存储供应商类型创建对应的读/写实例。
 */
public final class DocumentStoreFactory {

  // 禁止实例化工厂类
  private DocumentStoreFactory(){
  }

  /**
   * 根据配置创建对应供应商的文档存储写入器实例。
   * @param conf Hadoop配置对象，用于创建客户端连接
   * @param <Document> 写入文档类型，如TimelineEntityDocument、FlowActivityDocument等
   * @return 文档存储写入器实例
   * @throws DocumentStoreNotSupportedException 配置的供应商不存在或不支持时抛出
   * @throws YarnException 缺少必要配置项时抛出
   */
  public static <Document extends TimelineDocument>
      DocumentStoreWriter <Document> createDocumentStoreWriter(
          Configuration conf) throws YarnException {
    // 从配置中获取文档存储供应商类型
    final DocumentStoreVendor storeType = getStoreVendor(conf);
    switch (storeType) {
    case COSMOS_DB:
      // 验证Azure Cosmos DB配置合法性
      DocumentStoreUtils.validateCosmosDBConf(conf);
      return new CosmosDBDocumentStoreWriter<>(conf);
    default:
      throw new DocumentStoreNotSupportedException(
          "Unable to create DocumentStoreWriter for type : "
              + storeType);
    }
  }

  /**
 * 根据配置创建对应供应商的文档存储读取器实例。
 * @param conf Hadoop配置对象，用于创建客户端连接
 * @param <Document> 读取文档类型，如TimelineEntityDocument、FlowActivityDocument等
 * @return 文档存储读取器实例
 * @throws DocumentStoreNotSupportedException 配置的供应商不存在或不支持时抛出
 * @throws YarnException 缺少必要配置项时抛出
 * */
  public static <Document extends TimelineDocument>
      DocumentStoreReader<Document> createDocumentStoreReader(
          Configuration conf) throws YarnException {
    // 从配置中获取文档存储供应商类型
    final DocumentStoreVendor storeType = getStoreVendor(conf);
    switch (storeType) {
    case COSMOS_DB:
      // 验证Azure Cosmos DB配置合法性
      DocumentStoreUtils.validateCosmosDBConf(conf);
      return new CosmosDBDocumentStoreReader<>(conf);
    default:
      throw new DocumentStoreNotSupportedException(
          "Unable to create DocumentStoreReader for type : "
              + storeType);
    }
  }
}