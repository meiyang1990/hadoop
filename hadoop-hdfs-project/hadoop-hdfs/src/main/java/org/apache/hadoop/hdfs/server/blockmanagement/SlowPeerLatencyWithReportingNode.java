// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.blockmanagement;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 存储数据节点上报的慢节点延迟统计信息，用于慢节点JSON日志上报。
 * 记录了报告节点自身观测到的目标慢节点的延迟数据，以及基于统计得到的异常判定阈值。
 */
@InterfaceAudience.Private
final class SlowPeerLatencyWithReportingNode
    implements Comparable<SlowPeerLatencyWithReportingNode> {

  @JsonProperty("ReportingNode")
  private final String reportingNode;

  @JsonProperty("ReportedLatency")
  private final Double reportedLatency;

  @JsonProperty("MedianLatency")
  private final Double medianLatency;

  @JsonProperty("MadLatency")
  private final Double madLatency;

  @JsonProperty("UpperLimitLatency")
  private final Double upperLimitLatency;

  /**
   * 构造慢节点延迟上报信息对象，封装报告节点观测到的延迟统计数据。
   * @param reportingNode 报告延迟信息的数据节点地址
   * @param reportedLatency 报告节点观测到的目标慢节点延迟值
   * @param medianLatency 全局延迟中位数，用于异常判定
   * @param madLatency 延迟绝对偏差中位数，用于异常判定
   * @param upperLimitLatency 判定慢节点的延迟上限阈值
   */
  SlowPeerLatencyWithReportingNode(
      @JsonProperty("ReportingNode")
          String reportingNode,
      @JsonProperty("ReportedLatency")
          Double reportedLatency,
      @JsonProperty("MedianLatency")
          Double medianLatency,
      @JsonProperty("MadLatency")
          Double madLatency,
      @JsonProperty("UpperLimitLatency")
          Double upperLimitLatency) {
    this.reportingNode = reportingNode;
    this.reportedLatency = reportedLatency;
    this.medianLatency = medianLatency;
    this.madLatency = madLatency;
    this.upperLimitLatency = upperLimitLatency;
  }

  /**
   * 获取报告延迟信息的数据节点地址。
   * @return 报告节点地址字符串
   */
  public String getReportingNode() {
    return reportingNode;
  }

  /**
   * 获取报告节点观测到的目标慢节点延迟值。
   * @return 观测到的延迟值（毫秒）
   */
  public Double getReportedLatency() {
    return reportedLatency;
  }

  /**
   * 获取全局延迟中位数统计值。
   * @return 延迟中位数
   */
  public Double getMedianLatency() {
    return medianLatency;
  }

  /**
   * 获取延迟绝对偏差中位数（MAD）统计值。
   * @return 绝对偏差中位数
   */
  public Double getMadLatency() {
    return madLatency;
  }

  /**
   * 获取判定慢节点的延迟上限阈值。
   * @return 延迟上限阈值（毫秒）
   */
  public Double getUpperLimitLatency() {
    return upperLimitLatency;
  }

  /**
   * 按报告节点地址字典序比较两个对象，用于排序。
   * @param o 待比较的另一个对象
   * @return 比较结果，小于0表示当前节点在前，大于0表示待比较节点在前
   */
  @Override
  public int compareTo(SlowPeerLatencyWithReportingNode o) {
    return this.reportingNode.compareTo(o.getReportingNode());
  }

  /**
   * 判断两个对象是否相等，对比所有字段。
   * @param o 待比较对象
   * @return 所有字段都相等返回true，否则返回false
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SlowPeerLatencyWithReportingNode that = (SlowPeerLatencyWithReportingNode) o;

    return new EqualsBuilder()
        .append(reportingNode, that.reportingNode)
        .append(reportedLatency, that.reportedLatency)
        .append(medianLatency, that.medianLatency)
        .append(madLatency, that.madLatency)
        .append(upperLimitLatency, that.upperLimitLatency)
        .isEquals();
  }

  /**
   * 计算对象哈希码，基于所有字段生成。
   * @return 对象哈希码
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(reportingNode)
        .append(reportedLatency)
        .append(medianLatency)
        .append(madLatency)
        .append(upperLimitLatency)
        .toHashCode();
  }
}