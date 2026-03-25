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

/**
 * 封装应用表和实体表中HBase事件列名的相关信息，用于事件列名的编码与解码。
 */
public class EventColumnName {

  // 事件ID
  private final String id;
  // 事件时间戳
  private final Long timestamp;
  // 事件信息键名
  private final String infoKey;
  // 事件列名转换器，用于编解码
  private final KeyConverter<EventColumnName> eventColumnNameConverter =
      new EventColumnNameConverter();

  /**
   * 构造事件列名对象。
   * @param id 事件ID
   * @param timestamp 事件时间戳
   * @param infoKey 事件信息键名
   */
  public EventColumnName(String id, Long timestamp, String infoKey) {
    this.id = id;
    this.timestamp = timestamp;
    this.infoKey = infoKey;
  }

  /**
   * 获取事件ID。
   * @return 事件ID
   */
  public String getId() {
    return id;
  }

  /**
   * 获取事件时间戳。
   * @return 事件时间戳
   */
  public Long getTimestamp() {
    return timestamp;
  }

  /**
   * 获取事件信息键名。
   * @return 事件信息键名
   */
  public String getInfoKey() {
    return infoKey;
  }

  /**
   * 生成HBase列限定符，各字段使用分隔符拼接，不同字段组合会生成不同前缀格式，
   * 支持前缀扫描查询多个事件列。
   * @return 编码后的字节数组形式的列限定符
   */
  public byte[] getColumnQualifier() {
    return eventColumnNameConverter.encode(this);
  }

}