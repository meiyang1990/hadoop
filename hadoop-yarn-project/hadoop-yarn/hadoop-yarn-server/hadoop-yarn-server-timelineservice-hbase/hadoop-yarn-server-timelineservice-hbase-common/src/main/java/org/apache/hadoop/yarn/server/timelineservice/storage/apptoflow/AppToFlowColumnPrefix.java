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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.GenericConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;

/**
 * App-to-flow表(HBase存储应用流关联关系的表)的半限定列前缀枚举，
 * 用于定义该表中不同列类型的前缀规范，支持HBase列的动态构造。
 */
public enum AppToFlowColumnPrefix implements ColumnPrefix<AppToFlowTable> {

  /**
   * 流名称列前缀。
   */
  FLOW_NAME(AppToFlowColumnFamily.MAPPING, "flow_name"),

  /**
   * 流运行ID列前缀。
   */
  FLOW_RUN_ID(AppToFlowColumnFamily.MAPPING, "flow_run_id"),

  /**
   * 用户ID列前缀。
   */
  USER_ID(AppToFlowColumnFamily.MAPPING, "user_id");

  // 当前列前缀所属的列族
  private final ColumnFamily<AppToFlowTable> columnFamily;
  // 字符串形式的列前缀
  private final String columnPrefix;
  // 字节数组形式的列前缀(用于HBase存储)
  private final byte[] columnPrefixBytes;
  // 列值转换器，用于编解码列值
  private final ValueConverter valueConverter;

  /**
   * 构造AppToFlow列前缀实例，完成字符串前缀到字节数组的转换。
   * @param columnFamily 所属列族
   * @param columnPrefix 字符串形式的列前缀
   */
  AppToFlowColumnPrefix(ColumnFamily<AppToFlowTable> columnFamily,
      String columnPrefix) {
    this.columnFamily = columnFamily;
    this.columnPrefix = columnPrefix;
    if (columnPrefix == null) {
      this.columnPrefixBytes = null;
    } else {
      // 对列前缀进行编码处理，确保格式符合规范
      this.columnPrefixBytes =
          Bytes.toBytes(Separator.SPACE.encode(columnPrefix));
    }
    this.valueConverter = GenericConverter.getInstance();
  }

  @Override
  public byte[] getColumnPrefixBytes(String qualifierPrefix) {
    // 合并固定前缀和动态限定符前缀，生成完整列限定符
    return ColumnHelper.getColumnQualifier(
        columnPrefixBytes, qualifierPrefix);
  }

  @Override
  public byte[] getColumnPrefixBytes(byte[] qualifierPrefix) {
    // 合并固定前缀和字节形式动态限定符前缀，生成完整列限定符
    return ColumnHelper.getColumnQualifier(
        columnPrefixBytes, qualifierPrefix);
  }

  @Override
  public byte[] getColumnPrefixInBytes() {
    // 返回列前缀字节数组的克隆，避免外部修改内部状态
    return columnPrefixBytes != null ? columnPrefixBytes.clone() : null;
  }

  @Override
  public byte[] getColumnFamilyBytes() {
    // 获取所属列族的字节数组形式
    return columnFamily.getBytes();
  }

  @Override
  public ValueConverter getValueConverter() {
    // 获取当前列前缀对应的列值转换器
    return valueConverter;
  }

  @Override
  public Attribute[] getCombinedAttrsWithAggr(Attribute... attributes) {
    // 直接返回传入属性，当前列前缀无需聚合额外属性
    return attributes;
  }

  @Override
  public boolean supplementCellTimeStamp() {
    // 当前列前缀不需要补充单元格时间戳
    return false;
  }
}