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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Column;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.GenericConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;

/**
 * 应用表ApplicationTable的HBase列定义枚举，标识应用表中所有列的全限定信息。
 */
public enum ApplicationColumn implements Column<ApplicationTable> {

  /**
   * 应用ID列。
   */
  ID(ApplicationColumnFamily.INFO, "id"),

  /**
   * 应用创建时间列。
   */
  CREATED_TIME(ApplicationColumnFamily.INFO, "created_time",
      new LongConverter()),

  /**
   * 应用所属流的版本列。
   */
  FLOW_VERSION(ApplicationColumnFamily.INFO, "flow_version");

  private final ColumnFamily<ApplicationTable> columnFamily;
  private final String columnQualifier;
  private final byte[] columnQualifierBytes;
  private final ValueConverter valueConverter;

  /**
   * 构造应用列定义，使用通用值转换器。
   * @param columnFamily 所属列族
   * @param columnQualifier 列限定符
   */
  private ApplicationColumn(ColumnFamily<ApplicationTable> columnFamily,
      String columnQualifier) {
    this(columnFamily, columnQualifier, GenericConverter.getInstance());
  }

  /**
   * 构造应用列定义，使用指定值转换器。
   * @param columnFamily 所属列族
   * @param columnQualifier 列限定符
   * @param converter 值转换器
   */
  private ApplicationColumn(ColumnFamily<ApplicationTable> columnFamily,
      String columnQualifier, ValueConverter converter) {
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
    // 预转换为字节数组，确保列前缀格式符合规范，兼容未来扩展
    this.columnQualifierBytes =
        Bytes.toBytes(Separator.SPACE.encode(columnQualifier);
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