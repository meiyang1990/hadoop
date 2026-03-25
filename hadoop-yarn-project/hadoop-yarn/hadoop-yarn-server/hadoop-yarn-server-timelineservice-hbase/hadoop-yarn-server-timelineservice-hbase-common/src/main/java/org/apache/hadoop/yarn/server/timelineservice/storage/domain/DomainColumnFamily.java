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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * 定义HBase域表中所有列族，用于时间线服务存储域元数据
 */
public enum DomainColumnFamily implements ColumnFamily<DomainTable> {
  /**
   * 信息列族，存储域的基础信息：创建时间、所有者、可读用户等
   */
  INFO("i");

  /**
   * 列族名称的字节数组形式（用于HBase读写）
   */
  private final byte[] bytes;

  /**
   * 构造函数，根据字符串名称生成列族对应的字节数组
   * @param value 列族名称缩写
   */
  DomainColumnFamily(String value) {
    // column families should be lower case and not contain any spaces.
    this.bytes = Bytes.toBytes(Separator.SPACE.encode(value));
  }

  /**
   * 获取列族名称的字节数组副本
   * @return 列族字节数组
   */
  public byte[] getBytes() {
    return Bytes.copy(bytes);
  }

}