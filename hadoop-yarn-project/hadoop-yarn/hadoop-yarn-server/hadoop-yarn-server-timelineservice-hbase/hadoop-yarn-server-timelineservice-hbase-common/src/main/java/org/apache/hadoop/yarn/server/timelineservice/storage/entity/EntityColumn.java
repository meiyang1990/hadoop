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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Column;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.GenericConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;

/**
 * Entity表（存储时间线实体的HBase表）的列定义枚举，定义了该表所有全限定列的标识。
 */
public enum EntityColumn implements Column<EntityTable> {

  /**
   * 实体唯一标识列。
   */
  ID(EntityColumnFamily.INFO, "id"),

  /**
   * 实体类型列。
   */
  TYPE(EntityColumnFamily.INFO, "type"),

  /**
   * 实体创建时间列。
   */
  CREATED_TIME(EntityColumnFamily.INFO, "created_time", new LongConverter()),

  /**
   * 实体所属流的版本列。
   */
  FLOW_VERSION(EntityColumnFamily.INFO, "flow_version");

  private final ColumnFamily<EntityTable> columnFamily;
  private final String columnQualifier;
  private final byte[] columnQualifierBytes;
  private final ValueConverter valueConverter;

  /**
   * 构造EntityColumn，使用默认通用值转换器。
   * @param columnFamily 所属列族
   * @param columnQualifier 列名
   */
  EntityColumn(ColumnFamily<EntityTable> columnFamily,
      String columnQualifier) {
    this(columnFamily, columnQualifier, GenericConverter.getInstance());
  }

  /**
   * 构造EntityColumn，指定自定义值转换器。
   * @param columnFamily 所属列族
   * @param columnQualifier 列名
   * @param converter 值转换器
   */
  EntityColumn(ColumnFamily<EntityTable> columnFamily,
      String columnQualifier, ValueConverter converter) {
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
    // 提前处理列名格式，保证列前缀符合规范，向前兼容未来扩展
    this.columnQualifierBytes =
        Bytes.toBytes(Separator.SPACE.encode(columnQualifier));
    this.valueConverter = converter;
  }

  /**
   * @return 列限定符字符串
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

  @Override
  public ValueConverter getValueConverter() {
    return valueConverter;
  }

  @Override
  public Attribute[] getCombinedAttrsWithAggr(Attribute... attributes) {
    return attributes;
  }

  @Override
  public boolean supplementCellTimestamp() {
    return false;
  }
}