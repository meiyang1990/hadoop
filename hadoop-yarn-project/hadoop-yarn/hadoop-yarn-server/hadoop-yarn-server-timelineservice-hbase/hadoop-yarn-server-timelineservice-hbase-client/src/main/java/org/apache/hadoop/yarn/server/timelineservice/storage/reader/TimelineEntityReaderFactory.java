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
package org.apache.hadoop.yarn.server.timelineservice.storage.reader;

import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;

/**
 * 时间线实体读取器工厂，根据实体类型创建对应的读取器实例。
 * 不同类型实体存储在不同HBase表中，使用不同读取器实现查询。
 */
public final class TimelineEntityReaderFactory {
  private TimelineEntityReaderFactory() {
  }

  /**
   * 创建单实体查询的时间线实体读取器。
   * 根据实体类型和存储方式返回对应读取器实现。
   *
   * @param context 读取器上下文，定义查询范围信息
   * @param dataToRetrieve 指定需要获取的实体数据内容
   * @return 对应实体类型的TimelineEntityReader实现实例
   */
  public static TimelineEntityReader createSingleEntityReader(
      TimelineReaderContext context, TimelineDataToRetrieve dataToRetrieve) {
    // 非通用实体类型，使用专用表存储，返回对应专用读取器
    if (!context.isGenericEntity()) {
      if (TimelineEntityType.
          YARN_APPLICATION.matches(context.getEntityType())) {
        return new ApplicationEntityReader(context, dataToRetrieve);
      } else if (TimelineEntityType.
          YARN_FLOW_RUN.matches(context.getEntityType())) {
        return new FlowRunEntityReader(context, dataToRetrieve);
      } else if (TimelineEntityType.
          YARN_FLOW_ACTIVITY.matches(context.getEntityType())) {
        return new FlowActivityEntityReader(context, dataToRetrieve);
      }
    }
    // 通用实体类型，使用通用实体表查询，返回通用读取器
    return new GenericEntityReader(context, dataToRetrieve);
  }

  /**
   * 创建批量实体查询的时间线实体读取器。
   * 根据实体类型、存储方式和用户信息返回对应读取器实现。
   *
   * @param context 读取器上下文，定义查询范围信息
   * @param filters 过滤条件，限制返回的实体范围
   * @param dataToRetrieve 指定需要获取的实体数据内容
   * @return 对应实体类型的TimelineEntityReader实现实例
   */
  public static TimelineEntityReader createMultipleEntitiesReader(
      TimelineReaderContext context, TimelineEntityFilters filters,
      TimelineDataToRetrieve dataToRetrieve) {
    // 非通用实体类型，使用专用表存储，返回对应专用读取器
    if (!context.isGenericEntity()) {
      if (TimelineEntityType.
          YARN_APPLICATION.matches(context.getEntityType())) {
        return new ApplicationEntityReader(context, filters, dataToRetrieve);
      } else if (TimelineEntityType.
          YARN_FLOW_ACTIVITY.matches(context.getEntityType())) {
        return new FlowActivityEntityReader(context, filters, dataToRetrieve);
      } else if (TimelineEntityType.
          YARN_FLOW_RUN.matches(context.getEntityType())) {
        return new FlowRunEntityReader(context, filters, dataToRetrieve);
      }
    }
    // 存在代理用户时，返回子应用实体读取器，处理代理用户场景查询
    if (context.getDoAsUser() != null) {
      return new SubApplicationEntityReader(context, filters, dataToRetrieve);
    }
    // 通用实体类型，使用通用实体表查询，返回通用读取器
    return new GenericEntityReader(context, filters, dataToRetrieve);
  }

  /**
   * 创建实体类型查询读取器，用于查询指定范围内所有可用实体类型。
   *
   * @param context 读取器上下文，定义查询范围，仅支持应用级别查询
   * @return 实体类型查询读取器实例
   */
  public static EntityTypeReader createEntityTypeReader(
      TimelineReaderContext context) {
    return new EntityTypeReader(context);
  }
}