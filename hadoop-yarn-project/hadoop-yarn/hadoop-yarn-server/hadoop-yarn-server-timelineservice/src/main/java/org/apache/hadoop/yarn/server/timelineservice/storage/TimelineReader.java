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

package org.apache.hadoop.yarn.server.timelineservice.storage;

import java.io.IOException;

import java.util.Set;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.timeline.TimelineHealth;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;

/**
 * 文件说明：ATSv2（时间线服务V2）存储层读取接口，定义了从后端存储读取时间线实体数据的核心方法契约。
 * 核心职责：为上层查询服务提供统一的存储读取抽象，支持不同后端存储的实现接入。
 */
@Private
@Unstable
public interface TimelineReader extends Service {

  /**
   * 可指定需要获取的时间线实体字段枚举，用于控制查询返回数据范围，减少不必要的数据传输。
   */
  public enum Field {
    ALL,
    EVENTS,
    INFO,
    METRICS,
    CONFIGS,
    RELATES_TO,
    IS_RELATED_TO
  }

  /**
   * 根据实体标识符和上下文查询单个时间线实体。
   * @param context 查询上下文，定义查询范围，包含集群ID、用户ID、流信息、应用ID、实体ID等层级信息
   * @param dataToRetrieve 指定需要获取的实体数据字段，控制返回内容大小
   * @return 查询到的时间线实体，未找到则返回null，实体类型根据查询实体类型变化
   * @throws IOException 从后端存储读取数据发生异常时抛出
   */
  TimelineEntity getEntity(TimelineReaderContext context,
      TimelineDataToRetrieve dataToRetrieve) throws IOException;

  /**
   * 根据过滤条件在指定上下文范围内批量查询符合条件的时间线实体集合。
   * @param context 查询上下文，定义查询范围
   * @param filters 过滤条件，包括创建时间窗口、返回数量限制、键值对过滤、指标过滤、事件过滤、关系过滤等
   * @param dataToRetrieve 指定每个实体需要获取的字段，控制返回内容大小
   * @return 符合条件的时间线实体集合，按实体ID前缀排序
   * @throws IOException 从后端存储读取数据发生异常时抛出
   */
  Set<TimelineEntity> getEntities(
      TimelineReaderContext context,
      TimelineEntityFilters filters,
      TimelineDataToRetrieve dataToRetrieve) throws IOException;

  /**
   * 列出指定上下文范围内所有可用的实体类型。
   * @param context 查询上下文，至少需要包含集群ID和应用ID
   * @return 指定上下文中存在的所有实体类型集合
   * @throws IOException 从后端存储读取数据发生异常时抛出
   */
  Set<String> getEntityTypes(TimelineReaderContext context) throws IOException;

  /**
   * 检查时间线读取器与后端存储的连接健康状态。
   * @return 读取器健康状态信息，包含连接是否正常等状态
   */
  TimelineHealth getHealthStatus();
}