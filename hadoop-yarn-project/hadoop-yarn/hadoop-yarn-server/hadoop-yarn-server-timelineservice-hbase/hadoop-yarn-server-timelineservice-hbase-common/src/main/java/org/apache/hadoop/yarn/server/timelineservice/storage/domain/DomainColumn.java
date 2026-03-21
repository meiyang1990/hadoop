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
package org.apache.hadoop.yarn.server.timelineservice.storage.domain;


import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Column;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.GenericConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;

/**
 * 域名表DomainTable的列定义枚举，标识域名表中所有全限定列。
 */
public enum DomainColumn implements Column<DomainTable> {

  /**
   * 域创建时间列。
   */
  CREATED_TIME(DomainColumnFamily.INFO, "created_time"),

  /**
   * 域描述列。
   */
  DESCRIPTION(DomainColumnFamily.INFO, "description"),

  /**
   * 域修改时间列。
   */
  MODIFICATION_TIME(DomainColumnFamily.INFO, "modification_time"),

  /**
   * 域所有者列。
   */
  OWNER(DomainColumnFamily.INFO, "owner"),

  /**
   * 域可读用户列表列。
   */
  READERS(DomainColumnFamily.INFO, "readers"),

  /**
   * 域可写用户列表列。
   */
  WRITERS(DomainColumnFamily.INFO, "writers");


  private final ColumnFamily<DomainTable> columnFamily;
  private final String columnQualifier;
  private final byte[] columnQualifierBytes;
  private final ValueConverter valueConverter;

  /**
   * 域名列构造方法。
   * @param columnFamily 列所属列族
   * @param columnQualifier 列限定符名称
   */
  DomainColumn(ColumnFamily<DomainTable> columnFamily,
               String columnQualifier) {
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
    // 对列限定符进行编码，保证格式一致性
    this.columnQualifierBytes =
        Bytes.toBytes(Separator.SPACE.encode(columnQualifier));
    this.valueConverter = GenericConverter.getInstance();
  }

  /**
   * @return 列限定符名称
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