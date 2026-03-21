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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * 实体表HBase列族定义，定义了时间线服务实体表中所有列族。
 */
public enum EntityColumnFamily implements ColumnFamily<EntityTable> {

  /**
   * 基础信息列族，存储实体的基本已知信息，支持列族过滤查询。
   */
  INFO("i"),

  /**
   * 配置列族，单独存储配置信息，原因：配置值可能很大，且通常和其他信息分开访问。
   */
  CONFIGS("c"),

  /**
   * 指标列族，单独存储指标数据，因为指标有独立的TTL过期策略。
   */
  METRICS("m");

  /**
   * 列族对应的字节数组表示，用于HBase存储。
   */
  private final byte[] bytes;

  /**
   * 构造列族枚举实例，将列族名称编码为字节数组。
   * @param value 列族短名称，必须小写且无空格
   */
  EntityColumnFamily(String value) {
    // column families should be lower case and not contain any spaces.
    this.bytes = Bytes.toBytes(Separator.SPACE.encode(value));
  }

  /**
   * 获取列族的字节数组拷贝，避免外部修改内部状态。
   * @return 列族对应的字节数组
   */
  public byte[] getBytes() {
    return Bytes.copy(bytes);
  }

}