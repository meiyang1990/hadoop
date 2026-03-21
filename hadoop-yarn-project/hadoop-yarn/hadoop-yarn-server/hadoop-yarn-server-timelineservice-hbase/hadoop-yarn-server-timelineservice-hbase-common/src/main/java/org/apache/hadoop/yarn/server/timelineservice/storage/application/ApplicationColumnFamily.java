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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * 应用表HBase列族定义，代表应用时间线存储表中所有列族的枚举定义。
 */
public enum ApplicationColumnFamily implements ColumnFamily<ApplicationTable> {

  /**
   * 基础信息列族，存储应用基本信息元数据，可通过列族过滤高效查询。
   */
  INFO("i"),

  /**
   * 配置信息列族，单独存放配置有两个原因：配置值可能很大，且配置通常与指标/基础信息分开访问。
   */
  CONFIGS("c"),

  /**
   * 指标数据列族，单独存放指标因为指标需要不同的TTL过期策略。
   */
  METRICS("m");

  /**
   * 当前列族的字节数组表示，用于HBase存储。
   */
  private final byte[] bytes;

  /**
   * 构造函数，根据字符串名称创建列族定义并转换为字节数组。
   * @param value 列族名称缩写，要求小写且不含空格
   */
  private ApplicationColumnFamily(String value) {
    // column families should be lower case and not contain any spaces.
    this.bytes = Bytes.toBytes(Separator.SPACE.encode(value));
  }

  /**
   * 获取当前列族的字节数组副本，避免外部修改内部状态。
   * @return 列族字节数组的拷贝
   */
  public byte[] getBytes() {
    return Bytes.copy(bytes);
  }

}