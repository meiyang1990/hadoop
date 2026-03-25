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

// 文档存储供应商枚举，定义支持的文档存储厂商类型
/**
 * 文档存储供应商枚举，定义了时间线服务文档存储支持的不同底层数据库供应商。
 */
public enum DocumentStoreVendor {

  /** Cosmos DB文档数据库 */
  COSMOS_DB,
  /** MongoDB文档数据库 */
  MONGO_DB,
  /** ElasticSearch搜索引擎 */
  ELASTIC_SEARCH;

  /**
   * 根据字符串名称解析对应的文档存储供应商枚举。
   * @param storeTypeStr 供应商名称字符串
   * @return 匹配的文档存储供应商枚举
   */
  public static DocumentStoreVendor getStoreType(String storeTypeStr) {
    for (DocumentStoreVendor storeType : DocumentStoreVendor.values()) {
      if (storeType.name().equalsIgnoreCase(storeTypeStr)) {
        return DocumentStoreVendor.valueOf(storeTypeStr.toUpperCase());
      }
    }
    throw new DocumentStoreNotSupportedException(
        storeTypeStr + " is not a valid DocumentStoreVendor");
  }
}