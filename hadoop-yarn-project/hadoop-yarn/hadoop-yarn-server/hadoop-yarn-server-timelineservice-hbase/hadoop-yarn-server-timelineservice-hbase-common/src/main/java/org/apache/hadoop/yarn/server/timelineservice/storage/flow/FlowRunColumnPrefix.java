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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineSchemaUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;

/**
 * 定义FlowRunTable表中列名前缀，用于HBase列名的模块化构造
 * 实现了ColumnPrefix接口，提供统一的列前缀字节转换方法
 */
public enum FlowRunColumnPrefix implements ColumnPrefix<FlowRunTable> {

  /**
   * 用于存储流运行指标数据的列前缀
   */
  METRIC(FlowRunColumnFamily.INFO, "m", null, new LongConverter());

  private final ColumnFamily<FlowRunTable> columnFamily;

  /**
   * 列前缀字符串，若列前缀为空则表示当前列限定符即为完整列名
   */
  private final String columnPrefix;
  private final byte[] columnPrefixBytes;
  private final ValueConverter valueConverter;

  private final AggregationOperation aggOp;

  /**
   * 枚举构造方法，用于定义FlowRunTable的列前缀。
   *
   * @param columnFamily 该列前缀所属的列族
   * @param columnPrefix 列前缀字符串
   * @param fra 聚合操作类型
   * @param converter 列值转换器
   */
  private FlowRunColumnPrefix(ColumnFamily<FlowRunTable> columnFamily,
      String columnPrefix, AggregationOperation fra, ValueConverter converter) {
    this(columnFamily, columnPrefix, fra, converter, false);
  }

  private FlowRunColumnPrefix(ColumnFamily<FlowRunTable> columnFamily,
      String columnPrefix, AggregationOperation fra, ValueConverter converter,
      boolean compoundColQual) {
    this.valueConverter = converter;
    this.columnFamily = columnFamily;
    this.columnPrefix = columnPrefix;
    if (columnPrefix == null) {
      this.columnPrefixBytes = null;
    } else {
      // 对列前缀进行编码确保格式一致性
      this.columnPrefixBytes =
          Bytes.toBytes(Separator.SPACE.encode(columnPrefix));
    }
    this.aggOp = fra;
  }

  /**
   * 获取列前缀字符串。
   * @return 列前缀字符串
   */
  public String getColumnPrefix() {
    return columnPrefix;
  }

  public byte[] getColumnPrefixBytes() {
    return columnPrefixBytes.clone();
  }

  @Override
  public byte[] getColumnPrefixBytes(byte[] qualifierPrefix) {
    // 合并当前列前缀和传入的限定符前缀，生成完整列限定符
    return ColumnHelper.getColumnQualifier(this.columnPrefixBytes,
        qualifierPrefix);
  }

  @Override
  public byte[] getColumnPrefixBytes(String qualifierPrefix) {
    // 合并当前列前缀和传入的字符串限定符前缀，生成完整列限定符
    return ColumnHelper.getColumnQualifier(this.columnPrefixBytes,
        qualifierPrefix);
  }

  @Override
  public byte[] getColumnFamilyBytes() {
    // 获取所属列族的字节数组表示
    return columnFamily.getBytes();
  }

  @Override
  public byte[] getColumnPrefixInBytes() {
    // 获取列前缀的字节数组表示，为空则返回null
    return columnPrefixBytes != null ? columnPrefixBytes.clone() : null;
  }

  @Override
  public Attribute[] getCombinedAttrsWithAggr(Attribute... attributes) {
    // 合并聚合操作属性到现有属性数组
    return HBaseTimelineSchemaUtils.combineAttributes(attributes, aggOp);
  }

  @Override
  public boolean supplementCellTimeStamp() {
    // 需要补充单元格时间戳
    return true;
  }

  public AggregationOperation getAttribute() {
    // 获取聚合操作对象
    return aggOp;
  }

  @Override
  public ValueConverter getValueConverter() {
    // 获取列值转换器
    return valueConverter;
  }
}