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
package org.apache.hadoop.mapreduce.util;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

/**
 * 文件级注释：作业历史事件处理工具类，为作业历史事件处理提供通用工具能力，支撑作业指标转换和输出。
 * 包含作业计数器转换为JSON格式和YARN时间线服务指标格式的功能，供JobHistoryEventHandler使用。
 *
 * 工具类，存放JobHistoryEventHandler使用的通用工具方法。
 * 核心职责是将MapReduce作业计数器转换为不同格式，供作业历史存储系统使用。
 */
public final class JobHistoryEventUtils {
  /** 私有构造方法，禁止实例化工具类 */
  private JobHistoryEventUtils() {
  }

  // 单次发布到ATSv2的配置字节数上限
  public static final int ATS_CONFIG_PUBLISH_SIZE_BYTES = 10 * 1024;

  /**
   * 将MapReduce计数器转换为JSON格式，用于作业历史存储。
   * @param counters 待转换的MapReduce计数器对象
   * @return 转换完成的JSON节点，包含所有计数器组和计数器信息
   */
  public static JsonNode countersToJSON(Counters counters) {
    ObjectMapper mapper = new ObjectMapper();
    ArrayNode nodes = mapper.createArrayNode();
    if (counters != null) {
      // 遍历所有计数器组
      for (CounterGroup counterGroup : counters) {
        // 创建计数器组JSON节点
        ObjectNode groupNode = nodes.addObject();
        groupNode.put("NAME", counterGroup.getName());
        groupNode.put("DISPLAY_NAME", counterGroup.getDisplayName());
        // 创建计数器数组节点
        ArrayNode countersNode = groupNode.putArray("COUNTERS");
        // 遍历当前组内所有计数器
        for (Counter counter : counterGroup) {
          // 创建单个计数器JSON节点
          ObjectNode counterNode = countersNode.addObject();
          counterNode.put("NAME", counter.getName());
          counterNode.put("DISPLAY_NAME", counter.getDisplayName());
          counterNode.put("VALUE", counter.getValue());
        }
      }
    }
    return nodes;
  }

  /**
   * 将MapReduce计数器转换为YARN时间线服务的指标集合，使用空分组前缀。
   * @param counters 待转换的MapReduce计数器对象
   * @param timestamp 指标时间戳
   * @return 转换完成的时间线指标集合
   */
  public static Set<TimelineMetric> countersToTimelineMetric(Counters counters,
      long timestamp) {
    return countersToTimelineMetric(counters, timestamp, "");
  }

  /**
   * 将MapReduce计数器转换为YARN时间线服务的指标集合，支持自定义分组前缀。
   * @param counters 待转换的MapReduce计数器对象
   * @param timestamp 指标时间戳，标记指标生成时间
   * @param groupNamePrefix 指标名称分组前缀，用于区分不同来源的指标
   * @return 转换完成的时间线指标集合
   */
  public static Set<TimelineMetric> countersToTimelineMetric(Counters counters,
      long timestamp, String groupNamePrefix) {
    Set<TimelineMetric> entityMetrics = new HashSet<TimelineMetric>();
    // 遍历所有计数器组
    for (CounterGroup g : counters) {
      String groupName = g.getName();
      // 遍历当前组内所有计数器
      for (Counter c : g) {
        // 拼接完整指标名称：前缀 + 分组名 + 计数器名
        String name = groupNamePrefix + groupName + ":" + c.getName();
        // 构建时间线指标对象
        TimelineMetric metric = new TimelineMetric();
        metric.setId(name);
        metric.addValue(timestamp, c.getValue());
        entityMetrics.add(metric);
      }
    }
    return entityMetrics;
  }

}