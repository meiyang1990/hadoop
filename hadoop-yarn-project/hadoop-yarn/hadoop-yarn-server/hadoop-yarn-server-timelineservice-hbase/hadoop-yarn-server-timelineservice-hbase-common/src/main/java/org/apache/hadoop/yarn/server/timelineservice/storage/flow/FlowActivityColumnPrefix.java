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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.GenericConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineSchemaUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;

/**
 * 定义FlowActivityTable中部分限定列的列前缀，用于HBase存储流程活动数据。
 */
public enum FlowActivityColumnPrefix
    implements ColumnPrefix<FlowActivityTable> {

  /**
   * 用于存储流程的运行ID列表。
   */
  RUN_ID(FlowActivityColumnFamily.INFO, "r", null);

  // 该列前缀所属的列族
  private final ColumnFamily<FlowActivityTable> columnFamily;
  // 列值转换器
  private final ValueConverter valueConverter;
  // 字符串形式的列前缀，可为空（此时列限定符就是完整列名）
  private final String columnPrefix;
  // 字节数组形式的列前缀
  private final byte[] columnPrefixBytes;
  // 聚合操作类型
  private final AggregationOperation aggOp;

  /**
   * 枚举构造函数，供枚举定义使用。
   *
   * @param columnFamily 当前列前缀所属列族
   * @param columnPrefix 列前缀字符串
   * @param aggOp 聚合操作类型
   */
  private FlowActivityColumnPrefix(
      ColumnFamily<FlowActivityTable> columnFamily, String columnPrefix,
      AggregationOperation aggOp) {
    this(columnFamily, columnPrefix, aggOp, false);
  }

  /**
   * 完整私有构造函数，初始化列前缀所有属性。
   *
   * @param columnFamily 当前列前缀所属列族
   * @param columnPrefix 列前缀字符串
   * @param aggOp 聚合操作类型
   * @param compoundColQual 是否复合列限定符（当前未使用该参数）
   */
  private FlowActivityColumnPrefix(
      ColumnFamily<FlowActivityTable> columnFamily, String columnPrefix,
      AggregationOperation aggOp, boolean compoundColQual) {
    this.valueConverter = GenericConverter.getInstance();
    this.columnFamily = columnFamily;
    this.columnPrefix = columnPrefix;
    if (columnPrefix == null) {
      this.columnPrefixBytes = null;
    } else {
      // 对列前缀进行编码处理，确保符合HBase列命名规范
      this.columnPrefixBytes = Bytes.toBytes(Separator.SPACE
          .encode(columnPrefix));
    }
    this.aggOp = aggOp;
  }

  /**
   * 获取字符串形式的列前缀。
   * @return 列前缀字符串
   */
  public String getColumnPrefix() {
    return columnPrefix;
  }

  @Override
  public byte[] getColumnPrefixBytes(byte[] qualifierPrefix) {
    // 组合基础前缀和传入的限定符前缀，生成完整列限定符
    return ColumnHelper.getColumnQualifier(
        this.columnPrefixBytes, qualifierPrefix);
  }

  @Override
  public byte[] getColumnPrefixBytes(String qualifierPrefix) {
    // 组合基础前缀和传入的字符串限定符前缀，生成完整字节数组列限定符
    return ColumnHelper.getColumnQualifier(
        this.columnPrefixBytes, qualifierPrefix);
  }

  /**
   * 获取克隆后的字节数组形式列前缀。
   * @return 克隆后的列前缀字节数组
   */
  public byte[] getColumnPrefixBytes() {
    return columnPrefixBytes.clone();
  }

  @Override
  public byte[] getColumnFamilyBytes() {
    // 返回所属列族的字节数组表示
    return columnFamily.getBytes();
  }

  @Override
  public byte[] getColumnPrefixInBytes() {
    // 返回克隆后的列前缀字节数组，若前缀为空则返回null
    return columnPrefixBytes != null ? columnPrefixBytes.clone() : null;
  }

  @Override
  public ValueConverter getValueConverter() {
    // 返回该列的值转换器
    return valueConverter;
  }

  @Override
  public Attribute[] getCombinedAttrsWithAggr(Attribute... attributes) {
    // 合并传入属性与当前聚合操作，生成最终属性列表
    return HBaseTimelineSchemaUtils.combineAttributes(attributes, aggOp);
  }

  @Override
  public boolean supplementCellTimeStamp() {
    // 当前列前缀不需要补充单元格时间戳
    return false;
  }

  /**
   * 获取当前列前缀对应的聚合操作。
   * @return 聚合操作实例
   */
  public AggregationOperation getAttribute() {
    return aggOp;
  }
}