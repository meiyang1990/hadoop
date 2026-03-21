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
 * 定义HBase中FlowRun流运行表的所有列族，是HBase存储层对时间线服务流数据结构的映射.
 */
public enum FlowRunColumnFamily implements ColumnFamily<FlowRunTable> {

  /**
   * 信息列族，存储流运行的已知属性列，可通过列族过滤直接查询.
   */
  INFO("i");

  /**
   * 列族名称对应的字节数组表示（HBase原生要求字节存储）.
   */
  private final byte[] bytes;

  /**
   * 构造列族枚举实例，生成对应的字节表示.
   * @param value 列族短名称，必须小写且无空格
   */
  private FlowRunColumnFamily(String value) {
    // column families should be lower case and not contain any spaces.
    this.bytes = Bytes.toBytes(Separator.SPACE.encode(value));
  }

  /**
   * 获取列族的字节数组表示，返回副本避免外部修改内部状态.
   * @return 列族对应的字节数组
   */
  public byte[] getBytes() {
    return Bytes.copy(bytes);
  }

}