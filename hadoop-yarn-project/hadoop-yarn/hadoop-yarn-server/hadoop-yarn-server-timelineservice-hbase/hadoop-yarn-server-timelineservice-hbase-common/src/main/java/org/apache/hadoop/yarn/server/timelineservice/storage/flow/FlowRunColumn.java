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
package org.apache.hadoop.yarn.server.timelineservice.storage.flow;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Column;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.GenericConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineSchemaUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;

/**
 * 定义FlowRunTable中全列名（列限定符）的枚举，对应HBase表中各个列的结构定义
 */
public enum FlowRunColumn implements Column<FlowRunTable> {

  /**
   * 流运行最小启动时间，即当前已知所有应用启动时间的最小值
   */
  MIN_START_TIME(FlowRunColumnFamily.INFO, "min_start_time",
      AggregationOperation.GLOBAL_MIN, new LongConverter()),

  /**
   * 流运行最大结束时间，即当前已知所有应用结束时间的最大值
   */
  MAX_END_TIME(FlowRunColumnFamily.INFO, "max_end_time",
      AggregationOperation.GLOBAL_MAX, new LongConverter()),

  /**
   * 流所属的版本号
   */
  FLOW_VERSION(FlowRunColumnFamily.INFO, "flow_version", null);

  private final ColumnFamily<FlowRunTable> columnFamily;
  private final String columnQualifier;
  private final byte[] columnQualifierBytes;
  private final AggregationOperation aggOp;
  private final ValueConverter valueConverter;

  /**
   * 构造FlowRunColumn枚举实例，使用默认通用值转换器
   * @param columnFamily 所属列族
   * @param columnQualifier 列限定符名称
   * @param aggOp 聚合操作类型
   */
  private FlowRunColumn(ColumnFamily<FlowRunTable> columnFamily,
      String columnQualifier, AggregationOperation aggOp) {
    this(columnFamily, columnQualifier, aggOp,
        GenericConverter.getInstance());
  }

  /**
   * 构造FlowRunColumn枚举实例，使用自定义值转换器
   * @param columnFamily 所属列族
   * @param columnQualifier 列限定符名称
   * @param aggOp 聚合操作类型
   * @param converter 值转换器
   */
  private FlowRunColumn(ColumnFamily<FlowRunTable> columnFamily,
      String columnQualifier, AggregationOperation aggOp,
      ValueConverter converter) {
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
    this.aggOp = aggOp;
    // 编码列限定符确保格式正确，为未来扩展做准备
    this.columnQualifierBytes = Bytes.toBytes(Separator.SPACE
        .encode(columnQualifier));
    this.valueConverter = converter;
  }

  /**
   * @return 获取列限定符字符串
   */
  private String getColumnQualifier() {
    return columnQualifier;
  }

  @Override
  public byte[] getColumnQualifierBytes() {
    // 返回拷贝避免外部修改内部数组
    return columnQualifierBytes.clone();
  }

  @Override
  public byte[] getColumnFamilyBytes() {
    return columnFamily.getBytes();
  }

  /**
   * @return 获取该列对应的聚合操作类型
   */
  public AggregationOperation getAggregationOperation() {
    return aggOp;
  }

  @Override
  public ValueConverter getValueConverter() {
    return valueConverter;
  }

  @Override
  public Attribute[] getCombinedAttrsWithAggr(Attribute... attributes) {
    // 将聚合操作属性和传入属性合并为属性数组
    return HBaseTimelineSchemaUtils.combineAttributes(attributes, aggOp);
  }

  @Override
  public boolean supplementCellTimestamp() {
    // 需要补充单元格时间戳
    return true;
  }
}