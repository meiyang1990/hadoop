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
package org.apache.hadoop.yarn.server.timelineservice.storage.entity;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.GenericConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;

/**
 * 实体表HBase列前缀枚举，用于标识实体表中不同类型数据的列前缀
 */
public enum EntityColumnPrefix implements ColumnPrefix<EntityTable> {

  /**
   * 存储Timeline实体的被关联关系集合
   */
  IS_RELATED_TO(EntityColumnFamily.INFO, "s"),

  /**
   * 存储Timeline实体的关联关系集合
   */
  RELATES_TO(EntityColumnFamily.INFO, "r"),

  /**
   * 存储Timeline实体的基础信息
   */
  INFO(EntityColumnFamily.INFO, "i"),

  /**
   * 存储实体的生命周期事件
   */
  EVENT(EntityColumnFamily.INFO, "e", true),

  /**
   * 存储实体配置信息，配置键作为列名后缀
   */
  CONFIG(EntityColumnFamily.CONFIGS, null),

  /**
   * 存储实体指标数据，指标名称作为列名后缀
   */
  METRIC(EntityColumnFamily.METRICS, null, new LongConverter());

  // 所属列族
  private final ColumnFamily<EntityTable> columnFamily;

  /**
   * 列前缀字符串，若列限定符本身就是完整列名则为null
   */
  private final String columnPrefix;
  // 列前缀字节数组（HBase存储使用）
  private final byte[] columnPrefixBytes;
  // 值转换器，用于HBase值的编解码
  private final ValueConverter valueConverter;

  /**
   * 私有构造函数，供枚举定义使用
   *
   * @param columnFamily 该列前缀所属列族
   * @param columnPrefix 列前缀字符串
   */
  EntityColumnPrefix(ColumnFamily<EntityTable> columnFamily,
      String columnPrefix) {
    this(columnFamily, columnPrefix, false, GenericConverter.getInstance());
  }

  /**
   * 私有构造函数，供枚举定义使用
   *
   * @param columnFamily 该列前缀所属列族
   * @param columnPrefix 列前缀字符串
   * @param compondColQual 是否为复合列限定符
   */
  EntityColumnPrefix(ColumnFamily<EntityTable> columnFamily,
      String columnPrefix, boolean compondColQual) {
    this(columnFamily, columnPrefix, compondColQual,
        GenericConverter.getInstance());
  }

  /**
   * 私有构造函数，供枚举定义使用
   *
   * @param columnFamily 该列前缀所属列族
   * @param columnPrefix 列前缀字符串
   * @param converter 值编解码转换器
   */
  EntityColumnPrefix(ColumnFamily<EntityTable> columnFamily,
      String columnPrefix, ValueConverter converter) {
    this(columnFamily, columnPrefix, false, converter);
  }

  /**
   * 私有构造函数，供枚举定义使用
   *
   * @param columnFamily 该列前缀所属列族
   * @param columnPrefix 列前缀字符串
   * @param compondColQual 是否为复合列限定符
   * @param converter 值编解码转换器
   */
  EntityColumnPrefix(ColumnFamily<EntityTable> columnFamily,
      String columnPrefix, boolean compondColQual, ValueConverter converter) {
    this.valueConverter = converter;
    this.columnFamily = columnFamily;
    this.columnPrefix = columnPrefix;
    // 列前缀为空时字节数组也置空
    if (columnPrefix == null) {
      this.columnPrefixBytes = null;
    } else {
      // 对列前缀编码后转换为字节数组，保证前缀格式一致性
      this.columnPrefixBytes =
          Bytes.toBytes(Separator.SPACE.encode(columnPrefix));
    }
  }

  /**
   * @return 获取列前缀字符串
   */
  public String getColumnPrefix() {
    return columnPrefix;
  }

  @Override
  public byte[] getColumnPrefixBytes(byte[] qualifierPrefix) {
    // 拼接前缀得到完整列限定符字节数组
    return ColumnHelper.getColumnQualifier(
        this.columnPrefixBytes, qualifierPrefix);
  }

  @Override
  public byte[] getColumnPrefixBytes(String qualifierPrefix) {
    // 拼接前缀得到完整列限定符字节数组
    return ColumnHelper.getColumnQualifier(
        this.columnPrefixBytes, qualifierPrefix);
  }

  @Override
  public byte[] getColumnPrefixInBytes() {
    // 返回字节数组拷贝，避免外部修改内部状态
    return columnPrefixBytes != null ? columnPrefixBytes.clone() : null;
  }

  @Override
  public byte[] getColumnFamilyBytes() {
    // 返回所属列族的字节数组
    return columnFamily.getBytes();
  }

  @Override
  public ValueConverter getValueConverter() {
    // 返回该列前缀对应值的编解码转换器
    return valueConverter;
  }

  @Override
  public Attribute[] getCombinedAttrsWithAggr(Attribute... attributes) {
    // 直接返回原始属性，该类型不需要聚合属性
    return attributes;
  }

  @Override
  public boolean supplementCellTimeStamp() {
    // 该类型不需要补充单元格时间戳
    return false;
  }
}