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
package org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow;


import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Column;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.GenericConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;

/**
 * AppToFlow流表HBase列定义枚举，定义了应用到流映射表中所有列的完整元信息。
 */
public enum AppToFlowColumn implements Column<AppToFlowTable> {

  /**
   * 流ID列，存储应用所属流的唯一标识。
   */
  FLOW_ID(AppToFlowColumnFamily.MAPPING, "flow_id"),

  /**
   * 流运行ID列，存储应用所属流运行实例的标识。
   */
  FLOW_RUN_ID(AppToFlowColumnFamily.MAPPING, "flow_run_id"),

  /**
   * 用户ID列，存储提交应用的用户标识。
   */
  USER_ID(AppToFlowColumnFamily.MAPPING, "user_id");

  private final ColumnFamily<AppToFlowTable> columnFamily;
  private final String columnQualifier;
  private final byte[] columnQualifierBytes;
  private final ValueConverter valueConverter;

  /**
   * 构造AppToFlow列枚举实例，初始化列元信息并编码列名字节数组。
   * @param columnFamily 列所属列族
   * @param columnQualifier 列限定符名称
   */
  AppToFlowColumn(ColumnFamily<AppToFlowTable> columnFamily,
      String columnQualifier) {
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
    // 对列限定符进行编码，保证前缀格式合规，为未来扩展预留兼容性
    this.columnQualifierBytes =
        Bytes.toBytes(Separator.SPACE.encode(columnQualifier));
    this.valueConverter = GenericConverter.getInstance();
  }

  /**
   * @return 列限定符字符串
   */
  private String getColumnQualifier() {
    return columnQualifier;
  }

  @Override
  public byte[] getColumnQualifierBytes() {
    // 返回克隆数组避免外部修改内部字节内容
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