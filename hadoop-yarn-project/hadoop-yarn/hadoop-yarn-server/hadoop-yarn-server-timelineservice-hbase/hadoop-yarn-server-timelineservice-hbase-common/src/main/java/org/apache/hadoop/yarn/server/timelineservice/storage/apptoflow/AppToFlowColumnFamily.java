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
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * 表示应用到流映射表(AppToFlowTable)的列族定义，用于YARN时间线服务HBase存储层。
 * 应用到流映射表存储应用ID到流信息的映射关系，用于按流维度查询时间线数据。
 */
public enum AppToFlowColumnFamily implements ColumnFamily<AppToFlowTable> {
  /**
   * 映射列族，存储流名称、流运行ID等应用到流的映射元数据。
   */
  MAPPING("m");

  /**
   * 列族名称对应的字节数组表示，用于HBase存储。
   */
  private final byte[] bytes;

  /**
   * 构造列族枚举实例，将列族名称转换为HBase可用的字节数组。
   * @param value 列族名称字符串，要求小写且不含空格
   */
  AppToFlowColumnFamily(String value) {
    // column families should be lower case and not contain any spaces.
    this.bytes = Bytes.toBytes(Separator.SPACE.encode(value));
  }

  /**
   * 获取列族名称的字节数组副本，避免外部修改内部状态。
   * @return 列族名称的字节数组副本
   */
  public byte[] getBytes() {
    return Bytes.copy(bytes);
  }

}