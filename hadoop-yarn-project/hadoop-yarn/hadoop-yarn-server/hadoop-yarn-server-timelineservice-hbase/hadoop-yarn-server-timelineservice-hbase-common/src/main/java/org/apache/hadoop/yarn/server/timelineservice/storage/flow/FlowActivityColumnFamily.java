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
package org.apache.hadoop.yarn.server.timelineservice.storage.flow;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * 定义HBase中流活动表(FlowActivityTable)使用的列族
 * 为YARN时间线服务存储流活动元数据提供列族定义
 */
public enum FlowActivityColumnFamily
    implements ColumnFamily<FlowActivityTable> {

  /**
   * 信息列族，存储已知的固定列，可用于列族过滤。
   */
  INFO("i");

  /**
   * 列族名称的字节数组表示，用于HBase读写。
   */
  private final byte[] bytes;

  /**
   * 构造函数，根据字符串名称创建列族定义。
   * @param value 列族名称缩写，要求小写且不含空格
   */
  private FlowActivityColumnFamily(String value) {
    // 对列族名称编码并转换为字节数组
    this.bytes = Bytes.toBytes(Separator.SPACE.encode(value));
  }

  /**
   * 获取列族名称的字节数组副本。
   * @return 列族名称字节数组
   */
  public byte[] getBytes() {
    return Bytes.copy(bytes);
  }

}