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
 * 表示HBase中部分限定列名的抽象接口，完整列名由前缀和后缀拼接而成。
 * 当前缀为null时，完整列名将在数据存储时动态确定。
 * 用于YARN时间线服务HBase存储，统一管理不同列前缀的列操作。
 */
public interface ColumnPrefix<T extends BaseTable<T>> {

  /**
   * 拼接并获取编码后的完整列前缀字节数组，传入字符串类型的后缀。
   * @param qualifierPrefix 列限定符后缀
   * @return 拼接后的完整列前缀字节数组
   */
  byte[] getColumnPrefixBytes(String qualifierPrefix);

  /**
   * 拼接并获取编码后的完整列前缀字节数组，传入字节数组类型的后缀。
   * @param qualifierPrefix 列限定符后缀
   * @return 拼接后的完整列前缀字节数组
   */
  byte[] getColumnPrefixBytes(byte[] qualifierPrefix);

  /**
   * 获取基础列前缀的字节数组。
   * @return 基础列前缀字节数组
   */
  byte[] getColumnPrefixInBytes();

  /**
   * 获取该列前缀所属列族的字节数组。
   * @return 列族对应的字节数组
   */
  byte[] getColumnFamilyBytes();

  /**
   * 获取该列前缀关联的值转换器。
   * @return 值转换器实例
   */
  ValueConverter getValueConverter();

  /**
   * 获取合并聚合信息后的属性数组。
   * @param attributes 原始属性数组
   * @return 合并聚合属性后的属性数组
   */
  Attribute[] getCombinedAttrsWithAggr(Attribute... attributes);

  /**
   * 判断是否需要补充单元格时间戳。
   * @return true表示需要补充单元格时间戳
   */
  boolean supplementCellTimeStamp();
}