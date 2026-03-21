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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowrun;

import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetricOperation;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineMetricSubDoc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 流运行文档，存储YARN中每个作业流运行的元信息和指标数据，用于时序服务文档存储。
 */
public class FlowRunDocument implements TimelineDocument<FlowRunDocument> {

  private static final Logger LOG = LoggerFactory
      .getLogger(FlowRunDocument.class);

  private String id;
  private final String type = TimelineEntityType.YARN_FLOW_RUN.toString();
  private String clusterId;
  private String username;
  private String flowName;
  private Long flowRunId;
  private String flowVersion;
  private long minStartTime;
  private long maxEndTime;
  private final Map<String, TimelineMetricSubDoc>
      metrics = new HashMap<>();

  public FlowRunDocument() {
  }

  /**
   * 基于时间线收集器上下文和指标集合构造流运行文档。
   * @param collectorContext 时间线收集器上下文，包含流运行基本信息
   * @param metrics 流运行指标集合
   */
  public FlowRunDocument(TimelineCollectorContext collectorContext,
      Set<TimelineMetric> metrics) {
    this.clusterId = collectorContext.getClusterId();
    this.username = collectorContext.getUserId();
    this.flowName = collectorContext.getFlowName();
    this.flowRunId = collectorContext.getFlowRunId();
    transformMetrics(metrics);
  }

  /**
   * 将输入指标转换为子文档格式存入当前文档。
   * @param timelineMetrics 输入时间线指标集合
   */
  private void transformMetrics(Set<TimelineMetric> timelineMetrics) {
    for (TimelineMetric metric : timelineMetrics) {
      TimelineMetricSubDoc metricSubDoc = new TimelineMetricSubDoc(metric);
      this.metrics.put(metric.getId(), metricSubDoc);
    }
  }

  /**
   * 将传入的流运行文档合并到当前文档，用于upsert操作。
   *
   * @param flowRunDoc 需要合并的流运行文档
   */
  @Override
  public void merge(FlowRunDocument flowRunDoc) {
    if (flowRunDoc.getMinStartTime() > 0) {
      this.minStartTime = flowRunDoc.getMinStartTime();
    }
    if (flowRunDoc.getMaxEndTime() > 0) {
      this.maxEndTime = flowRunDoc.getMaxEndTime();
    }
    this.clusterId = flowRunDoc.getClusterId();
    this.flowName = flowRunDoc.getFlowName();
    this.id = flowRunDoc.getId();
    this.username = flowRunDoc.getUsername();
    this.flowVersion = flowRunDoc.getFlowVersion();
    this.flowRunId = flowRunDoc.getFlowRunId();
    aggregateMetrics(flowRunDoc.getMetrics());
  }

  /**
   * 聚合传入的指标到当前文档已有指标，按聚合规则处理冲突。
   * @param metricSubDocMap 待聚合的指标子文档映射
   */
  private void aggregateMetrics(
      Map<String, TimelineMetricSubDoc> metricSubDocMap) {
    for(Map.Entry<String, TimelineMetricSubDoc> metricEntry :
        metricSubDocMap.entrySet()) {
      final String metricId = metricEntry.getKey();
      final TimelineMetricSubDoc metricValue = metricEntry.getValue();

      if (this.metrics.containsKey(metricId)) {
        TimelineMetric incomingMetric =
            metricValue.fetchTimelineMetric();
        TimelineMetric baseMetric =
            this.metrics.get(metricId).fetchTimelineMetric();
        if (incomingMetric.getValues().size() > 0) {
          // 执行聚合操作后更新子文档
          baseMetric = aggregate(incomingMetric, baseMetric);
          this.metrics.put(metricId, new TimelineMetricSubDoc(baseMetric));
        } else {
          LOG.debug("No incoming metric to aggregate for : {}",
              baseMetric.getId());
        }
      } else {
        // 不存在则直接添加新指标
        this.metrics.put(metricId, metricValue);
      }
    }
  }

  /**
   * 根据聚合操作类型对两个指标进行聚合计算。
   * @param incomingMetric 新输入指标
   * @param baseMetric 当前已有基础指标
   * @return 聚合后的指标
   */
  private TimelineMetric aggregate(TimelineMetric incomingMetric,
      TimelineMetric baseMetric) {
    switch (baseMetric.getRealtimeAggregationOp()) {
    case SUM:
      // 累加聚合
      baseMetric = TimelineMetricOperation.SUM
          .aggregate(incomingMetric, baseMetric, null);
      break;
    case AVG:
      // 平均值聚合
      baseMetric = TimelineMetricOperation.AVG
          .aggregate(incomingMetric, baseMetric, null);
      break;
    case MAX:
      // 最大值聚合
      baseMetric = TimelineMetricOperation.MAX
          .aggregate(incomingMetric, baseMetric, null);
      break;
    case REPLACE:
      // 替换聚合
      baseMetric = TimelineMetricOperation.REPLACE
          .aggregate(incomingMetric, baseMetric, null);
    default:
      // 未知聚合类型打印警告日志
      LOG.warn("Unknown TimelineMetricOperation: {}",
          baseMetric.getRealtimeAggregationOp());
    }
    return baseMetric;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getClusterId() {
    return clusterId;
  }

  public void setClusterId(String clusterId) {
    this.clusterId = clusterId;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getFlowName() {
    return flowName;
  }

  public void setFlowName(String flowName) {
    this.flowName = flowName;
  }

  public Long getFlowRunId() {
    return flowRunId;
  }

  public void setFlowRunId(Long flowRunId) {
    this.flowRunId = flowRunId;
  }

  public Map<String, TimelineMetricSubDoc> getMetrics() {
    return metrics;
  }

  public void setMetrics(Map<String, TimelineMetricSubDoc> metrics) {
    this.metrics.putAll(metrics);
  }

  /**
   * 从子文档中提取所有原始时间线指标。
   * @return 原始时间线指标集合
   */
  public Set<TimelineMetric> fetchTimelineMetrics() {
    Set<TimelineMetric> metricSet = new HashSet<>();
    for(TimelineMetricSubDoc metricSubDoc : metrics.values()) {
      metricSet.add(metricSubDoc.fetchTimelineMetric());
    }
    return metricSet;
  }

  public long getMinStartTime() {
    return minStartTime;
  }

  public void setMinStartTime(long minStartTime) {
    this.minStartTime = minStartTime;
  }

  public long getMaxEndTime() {
    return maxEndTime;
  }

  public void setMaxEndTime(long maxEndTime) {
    this.maxEndTime = maxEndTime;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public long getCreatedTime() {
    return minStartTime;
  }

  @Override
  public void setCreatedTime(long createdTime) {
    // 仅当未设置开始时间时才赋值
    if(minStartTime == 0) {
      minStartTime = createdTime;
    }
  }

  public String getFlowVersion() {
    return flowVersion;
  }

  public void setFlowVersion(String flowVersion) {
    this.flowVersion = flowVersion;
  }
}