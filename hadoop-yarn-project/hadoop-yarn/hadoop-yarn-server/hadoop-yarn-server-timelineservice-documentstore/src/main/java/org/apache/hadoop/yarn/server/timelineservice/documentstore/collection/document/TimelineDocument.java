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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document;

<<<<<<< HEAD

// 时间线条目文档接口，定义所有时间线条目文档的通用方法。任何需要持久化到文档存储的新文档都应实现此接口
=======
>>>>>>> a7f26154e2430da367a92d826f772851319cf52d
/**
 * 时间线文档通用接口，所有需要持久化到文档存储的时间线文档都必须实现该接口。
 * 定义了时间线文档需要具备的基础能力和公共属性。
 */
public interface TimelineDocument<Document> {

  /**
   * 获取文档唯一标识ID。
   * @return 文档ID字符串
   */
  String getId();

  /**
   * 获取文档类型。
   * @return 文档类型字符串
   */
  String getType();

  /**
   * 获取文档创建时间戳。
   * @return 创建时间（毫秒级时间戳）
   */
  long getCreatedTime();

  /**
   * 设置文档创建时间戳。
   * @param time 创建时间（毫秒级时间戳）
   */
  void setCreatedTime(long time);

  /**
   * 将传入文档合并到当前文档。
   * 用于处理同一文档的更新合并场景。
   * @param timelineDocument 需要合并的源文档
   */
  void merge(Document timelineDocument);
}