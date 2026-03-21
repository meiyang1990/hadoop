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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.collection;

/**
 * 文档存储集合类型枚举，定义时间线服务文档存储中使用的不同集合类型
 */
public enum CollectionType {
  /** 实体集合，存储时间线实体数据 */
  ENTITY("EntityCollection"),
  /** 应用集合，存储YARN应用数据 */
  APPLICATION("AppCollection"),
  /** 流运行集合，存储工作流运行数据 */
  FLOW_RUN("FlowRunCollection"),
  /** 流活动集合，存储工作流活动数据 */
  FLOW_ACTIVITY("FlowActivityCollection");

  private final String collectionName;

  CollectionType(String collectionName) {
    this.collectionName = collectionName;
  }

  /**
   * 判断当前集合类型名称与给定名称是否相等
   * @param otherCollectionName 待比较的集合名称
   * @return 比较结果
   */
  public boolean equals(String otherCollectionName) {
    return this.collectionName.equals(otherCollectionName);
  }

  /**
   * 获取集合的实际名称
   * @return 集合名称字符串
   */
  public String getCollectionName() {
    return collectionName;
  }
}