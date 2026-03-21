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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreFactory;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.CollectionType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.DocumentStoreWriter;
import org.apache.hadoop.yarn.server.timelineservice.storage.SchemaCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文档存储后端的时间线服务集合创建器，用于在文档数据库中创建存储应用时间线信息所需的集合。
 * 实现SchemaCreator接口，为不同文档存储类型初始化存储结构。
 */
public class DocumentStoreCollectionCreator implements SchemaCreator {

  private static final Logger LOG = LoggerFactory
      .getLogger(DocumentStoreCollectionCreator.class);


  /**
   * 创建时间线服务所需的数据库和集合结构。
   * @param args 命令行参数，此处未使用
   */
  @Override
  public void createTimelineSchema(String[] args) {
    try {
      // 初始化Yarn配置对象
      Configuration conf = new YarnConfiguration();

      LOG.info("Creating database and collections for DocumentStore : {}",
          DocumentStoreUtils.getStoreVendor(conf));

      // 创建文档存储写入器并自动关闭资源
      try(DocumentStoreWriter documentStoreWriter = DocumentStoreFactory
          .createDocumentStoreWriter(conf)) {
        // 创建数据库
        documentStoreWriter.createDatabase();
        // 创建应用信息集合
        documentStoreWriter.createCollection(
            CollectionType.APPLICATION.getCollectionName());
        // 创建实体信息集合
        documentStoreWriter.createCollection(
            CollectionType.ENTITY.getCollectionName());
        // 创建流活动信息集合
        documentStoreWriter.createCollection(
            CollectionType.FLOW_ACTIVITY.getCollectionName());
        // 创建流运行信息集合
        documentStoreWriter.createCollection(
            CollectionType.FLOW_RUN.getCollectionName());
      }
    } catch (Exception e) {
      LOG.error("Error while creating Timeline Collections", e);
    }
  }
}