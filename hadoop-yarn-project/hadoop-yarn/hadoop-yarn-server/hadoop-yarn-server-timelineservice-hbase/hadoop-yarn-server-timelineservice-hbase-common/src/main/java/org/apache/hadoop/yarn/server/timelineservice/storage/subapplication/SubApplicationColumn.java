// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain copy of the License at
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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Column;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.GenericConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;

/**
 * 子应用表SubApplicationTable的列枚举定义，标识HBase中子应用表全限定列信息。
 */
public enum SubApplicationColumn implements Column<SubApplicationTable> {

  /**
   * 子应用唯一标识符列。
   */
  ID(SubApplicationColumnFamily.INFO, "id"),

  /**
   * 子应用类型列。
   */
  TYPE(SubApplicationColumnFamily.INFO, "type"),

  /**
   * 子应用创建时间列。
   */
  CREATED_TIME(SubApplicationColumnFamily.INFO, "created_time",
      new LongConverter()),

  /**
   * 子应用所属流的版本列。
   */
  FLOW_VERSION(SubApplicationColumnFamily.INFO, "flow_version");

  private final ColumnFamily<SubApplicationTable> columnFamily;
  private final String columnQualifier;
  private final byte[] columnQualifierBytes;
  private final ValueConverter valueConverter;

  /**
   * 子应用列构造方法，使用通用值转换器。
   * @param columnFamily 列所属列族
   * @param columnQualifier 列限定符名称
   */
  SubApplicationColumn(ColumnFamily<SubApplicationTable> columnFamily,
      String columnQualifier) {
    this(columnFamily, columnQualifier, GenericConverter.getInstance());
  }

  /**
   * 子应用列构造方法，使用自定义值转换器。
   * @param columnFamily 列所属列族
   * @param columnQualifier 列限定符名称
   * @param converter 值转换器
   */
  SubApplicationColumn(ColumnFamily<SubApplicationTable> columnFamily,
      String columnQualifier, ValueConverter converter) {
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
    // 对列限定符编码，保证前缀规范，向前兼容
    this.columnQualifierBytes =
        Bytes.toBytes(Separator.SPACE.encode(columnQualifier));
    this.valueConverter = converter;
  }

  @Override
  public byte[] getColumnQualifierBytes() {
    // 返回克隆后的字节数组避免外部修改内部状态
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