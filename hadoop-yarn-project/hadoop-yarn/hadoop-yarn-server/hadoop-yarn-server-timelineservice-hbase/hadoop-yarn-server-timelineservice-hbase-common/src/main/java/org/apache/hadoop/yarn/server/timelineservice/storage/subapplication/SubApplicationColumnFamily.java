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
package org.apache.hadoop.yarn.server.timelineservice.storage.subapplication;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * 子应用时间线表HBase列族枚举定义，定义了子应用表所有列族及其编码。
 */
public enum SubApplicationColumnFamily
    implements ColumnFamily<SubApplicationTable> {

  /**
   * 基础信息列族，存储子应用基础信息，支持列族过滤查询。
   */
  INFO("i"),

  /**
   * 配置列族，单独存储配置信息，原因：1.配置值可能很大 2.配置通常与指标、基础信息分开访问。
   */
  CONFIGS("c"),

  /**
   * 指标列族，单独存储指标数据，因为指标有独立的TTL过期策略。
   */
  METRICS("m");

  /**
   * 列族的字节数组表示（用于HBase存储）。
   */
  private final byte[] bytes;

  /**
   * 构造列族枚举，将字符串名称编码为HBase可用的字节数组。
   * @param value 列族名称（单字符缩写）
   */
  SubApplicationColumnFamily(String value) {
    // column families should be lower case and not contain any spaces.
    this.bytes = Bytes.toBytes(Separator.SPACE.encode(value));
  }

  /**
   * 获取列族的字节数组副本，避免外部修改内部状态。
   * @return 列族字节数组的拷贝
   */
  public byte[] getBytes() {
    return Bytes.copy(bytes);
  }

}