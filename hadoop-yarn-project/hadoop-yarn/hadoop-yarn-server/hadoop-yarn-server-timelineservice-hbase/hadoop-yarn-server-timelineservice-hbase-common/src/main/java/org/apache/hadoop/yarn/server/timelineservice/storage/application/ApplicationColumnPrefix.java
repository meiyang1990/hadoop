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
package org.apache.hadoop.yarn.server.timelineservice.storage.application;

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
 * 应用表HBase存储的列前缀枚举定义，用于标识应用表中部分限定列。
 */
public enum ApplicationColumnPrefix implements ColumnPrefix<ApplicationTable> {

  /**
   * 存储TimelineEntity的isRelatedToEntities关系数据。
   */
  IS_RELATED_TO(ApplicationColumnFamily.INFO, "s"),

  /**
   * 存储TimelineEntity的relatesToEntities关系数据。
   */
  RELATES_TO(ApplicationColumnFamily.INFO, "r"),

  /**
   * 存储TimelineEntity基础信息数据。
   */
  INFO(ApplicationColumnFamily.INFO, "i"),

  /**
   * 存储应用生命周期事件数据。
   */
  EVENT(ApplicationColumnFamily.INFO, "e"),

  /**
   * 存储应用配置，配置键作为列名。
   */
  CONFIG(ApplicationColumnFamily.CONFIGS, null),

  /**
   * 存储应用指标，指标名作为列名。
   */
  METRIC(ApplicationColumnFamily.METRICS, null, new LongConverter());

  private final ColumnFamily<ApplicationTable> columnFamily;

  /**
   * 当列限定符就是完整列名时可为null。
   */
  private final String columnPrefix;
  private final byte[] columnPrefixBytes;
  private final ValueConverter valueConverter;

  /**
   * 枚举构造方法，供枚举定义使用，使用默认通用值转换器。
   *
   * @param columnFamily 当前列前缀所属列族
   * @param columnPrefix 列前缀字符串
   */
  private ApplicationColumnPrefix(ColumnFamily<ApplicationTable> columnFamily,
      String columnPrefix) {
    this(columnFamily, columnPrefix, GenericConverter.getInstance());
  }

  /**
   * 枚举构造方法，供枚举定义使用，指定自定义值转换器。
   *
   * @param columnFamily 当前列前缀所属列族
   * @param columnPrefix 列前缀字符串
   * @param converter 该列前缀对应值的编解码器，用于HBase存储编解码
   */
  private ApplicationColumnPrefix(ColumnFamily<ApplicationTable> columnFamily,
      String columnPrefix, ValueConverter converter) {
    this.valueConverter = converter;
    this.columnFamily = columnFamily;
    this.columnPrefix = columnPrefix;
    if (columnPrefix == null) {
      this.columnPrefixBytes = null;
    } else {
      // 提前对列前缀进行编码，保证格式一致性
      this.columnPrefixBytes =
          Bytes.toBytes(Separator.SPACE.encode(columnPrefix));
    }
  }

  /**
   * @return 获取列前缀字符串
   */
  private String getColumnPrefix() {
    return columnPrefix;
  }

  @Override
  public byte[] getColumnPrefixBytes(byte[] qualifierPrefix) {
    // 合并当前前缀和额外限定前缀得到完整列限定符
    return ColumnHelper.getColumnQualifier(
        this.columnPrefixBytes, qualifierPrefix);
  }

  @Override
  public byte[] getColumnPrefixBytes(String qualifierPrefix) {
    // 合并当前前缀和字符串形式的额外限定前缀得到完整列限定符
    return ColumnHelper.getColumnQualifier(
        this.columnPrefixBytes, qualifierPrefix);
  }

  @Override
  public byte[] getColumnFamilyBytes() {
    // 返回所属列族的字节数组形式
    return columnFamily.getBytes();
  }

  @Override
  public byte[] getColumnPrefixInBytes() {
    // 返回列前缀字节数组，返回拷贝避免外部修改内部状态
    return columnPrefixBytes != null ? columnPrefixBytes.clone() : null;
  }

  @Override
  public Attribute[] getCombinedAttrsWithAggr(Attribute... attributes) {
    // 当前列前缀不需要额外聚合属性，直接返回原属性数组
    return attributes;
  }

  @Override
  public boolean supplementCellTimeStamp() {
    // 当前列前缀不需要补充单元格时间戳
    return false;
  }

  public ValueConverter getValueConverter() {
    // 获取当前列前缀对应的值转换器
    return valueConverter;
  }
}