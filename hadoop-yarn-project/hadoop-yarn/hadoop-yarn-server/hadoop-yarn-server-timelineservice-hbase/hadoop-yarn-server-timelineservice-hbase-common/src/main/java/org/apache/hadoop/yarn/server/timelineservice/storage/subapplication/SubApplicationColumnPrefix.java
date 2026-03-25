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
package org.apache.hadoop.yarn.server.timelineservice.storage.subapplication;

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
 * 子应用HBase表的列前缀枚举定义，用于标识子应用表中不同类型的半限定列。
 */
public enum SubApplicationColumnPrefix
    implements ColumnPrefix<SubApplicationTable> {

  /**
   * 存储Timeline实体的被关联实体关系(isRelatedTo)。
   */
  IS_RELATED_TO(SubApplicationColumnFamily.INFO, "s"),

  /**
   * 存储Timeline实体的关联实体关系(relatesTo)。
   */
  RELATES_TO(SubApplicationColumnFamily.INFO, "r"),

  /**
   * 存储Timeline实体的基础信息。
   */
  INFO(SubApplicationColumnFamily.INFO, "i"),

  /**
   * 存储实体的生命周期事件。
   */
  EVENT(SubApplicationColumnFamily.INFO, "e", true),

  /**
   * 配置列，配置键作为列名后缀。
   */
  CONFIG(SubApplicationColumnFamily.CONFIGS, null),

  /**
   * 指标列，指标名称作为列名后缀，使用LongConverter转换值。
   */
  METRIC(SubApplicationColumnFamily.METRICS, null, new LongConverter());

  private final ColumnFamily<SubApplicationTable> columnFamily;

  /**
   * 如果列限定符就是完整列名，该值可为null。
   */
  private final String columnPrefix;
  private final byte[] columnPrefixBytes;
  private final ValueConverter valueConverter;

  /**
   * 供枚举定义使用的私有构造方法。
   *
   * @param columnFamily 当前列前缀所属的列族
   * @param columnPrefix 列前缀字符串
   */
  SubApplicationColumnPrefix(ColumnFamily<SubApplicationTable> columnFamily,
      String columnPrefix) {
    this(columnFamily, columnPrefix, false, GenericConverter.getInstance());
  }

  SubApplicationColumnPrefix(ColumnFamily<SubApplicationTable> columnFamily,
      String columnPrefix, boolean compondColQual) {
    this(columnFamily, columnPrefix, compondColQual,
        GenericConverter.getInstance());
  }

  SubApplicationColumnPrefix(ColumnFamily<SubApplicationTable> columnFamily,
      String columnPrefix, ValueConverter converter) {
    this(columnFamily, columnPrefix, false, converter);
  }

  /**
   * 完整参数的私有构造方法，供枚举定义使用。
   *
   * @param columnFamily 当前列前缀所属的列族
   * @param columnPrefix 列前缀字符串
   * @param compondColQual 是否为复合列限定符
   * @param converter 该列前缀下值的编解码器
   */
  SubApplicationColumnPrefix(ColumnFamily<SubApplicationTable> columnFamily,
      String columnPrefix, boolean compondColQual, ValueConverter converter) {
    this.valueConverter = converter;
    this.columnFamily = columnFamily;
    this.columnPrefix = columnPrefix;
    if (columnPrefix == null) {
      this.columnPrefixBytes = null;
    } else {
      // 对列前缀进行编码，确保格式正确
      this.columnPrefixBytes =
          Bytes.toBytes(Separator.SPACE.encode(columnPrefix));
    }
  }

  /**
   * 获取列前缀字符串。
   * @return 列前缀字符串
   */
  public String getColumnPrefix() {
    return columnPrefix;
  }

  @Override
  public byte[] getColumnPrefixBytes(byte[] qualifierPrefix) {
    // 合并列前缀和额外限定符得到完整列限定符
    return ColumnHelper.getColumnQualifier(
        this.columnPrefixBytes, qualifierPrefix);
  }

  @Override
  public byte[] getColumnPrefixBytes(String qualifierPrefix) {
    // 合并列前缀和字符串类型额外限定符得到完整列限定符
    return ColumnHelper.getColumnQualifier(
        this.columnPrefixBytes, qualifierPrefix);
  }

  @Override
  public byte[] getColumnPrefixInBytes() {
    // 返回列前缀的字节数组，不包含额外限定符
    return columnPrefixBytes != null ? columnPrefixBytes.clone() : null;
  }

  @Override
  public byte[] getColumnFamilyBytes() {
    // 返回所属列族的字节数组表示
    return columnFamily.getBytes();
  }

  @Override
  public ValueConverter getValueConverter() {
    // 获取当前列前缀的值编解码器
    return valueConverter;
  }

  @Override
  public Attribute[] getCombinedAttrsWithAggr(Attribute... attributes) {
    return attributes;
  }

  @Override
  public boolean supplementCellTimeStamp() {
    return false;
  }
}