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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;

/**
 * HBase存储中完全限定列的抽象接口，定义了列在特定表中的存储元信息，
 * 用于YARN时间线服务HBase存储层统一管理不同表的列定义。
 * 
 * @param <T> 该列所属的HBase表类型
 */
public interface Column<T extends BaseTable<T>> {
  /**
   * 获取该列对应的列族字节数组形式.
   * @return 列族编码后的字节数组
   */
  byte[] getColumnFamilyBytes();

  /**
   * 获取该列限定符的字节数组形式.
   * @return 列限定符编码后的字节数组
   */
  byte[] getColumnQualifierBytes();

  /**
   * 获取该列关联的值转换器，用于列值在Java类型和HBase字节存储之间的转换.
   * @return 值转换器实例
   */
  ValueConverter getValueConverter();

  /**
   * 获取与聚合操作结合后的属性数组.
   * @param attributes 输入属性数组
   * @return 聚合处理后的属性数组
   */
  Attribute[] getCombinedAttrsWithAggr(Attribute... attributes);

  /**
   * 判断是否需要对单元格时间戳进行补充.
   * @return true表示需要补充单元格时间戳，false表示不需要
   */
  boolean supplementCellTimestamp();
}