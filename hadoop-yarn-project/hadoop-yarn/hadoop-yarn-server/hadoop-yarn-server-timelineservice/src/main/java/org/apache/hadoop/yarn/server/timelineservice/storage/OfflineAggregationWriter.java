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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineWriteResponse;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.OfflineAggregationInfo;

import java.io.IOException;

/**
 * 文件说明：YARN Timeline Service v2 离线聚合数据存储抽象接口，定义离线聚合后时间线数据的写入规范
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class OfflineAggregationWriter extends AbstractService {

  /**
   * 构造离线聚合写入器服务
   *
   * @param name 服务名称
   */
  public OfflineAggregationWriter(String name) {
    super(name);
  }

  /**
   * 将聚合后的时间线实体持久化存储到离线存储中，聚合路径由聚合信息对象指定
   *
   * @param context 时间线收集器上下文，描述聚合数据的上下文信息，根据聚合类型不同，部分字段可能为空
   * @param entities 需要持久化的聚合后时间线实体集合
   * @param info 离线聚合信息对象，描述聚合的具体信息，当前支持流聚合(FLOW_AGGREGATION)
   * @return 时间线写入响应对象，包含写入结果信息
   * @throws IOException 写入聚合实体过程中发生I/O异常时抛出
   */
  abstract TimelineWriteResponse writeAggregatedEntity(
      TimelineCollectorContext context, TimelineEntities entities,
      OfflineAggregationInfo info) throws IOException;
}