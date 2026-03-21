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

/**
 * 定义写入FlowRunTable时需要聚合计算的操作类型，为HBase流运行表存储提供聚合标记。
 * 枚举tagType使用质数作为取值，用于唯一区分不同聚合操作。
 */
public enum AggregationOperation {

  /**
   * 流启动时间聚合，取全局最小值。
   */
  GLOBAL_MIN((byte) 71),

  /**
   * 流结束时间聚合，取全局最大值。
   */
  GLOBAL_MAX((byte) 73),

  /**
   * 流指标值聚合，累加求和。
   */
  SUM((byte) 79),

  /**
   * 已结束应用指标值聚合，累加求和。
   */
  SUM_FINAL((byte) 83),

  /**
   * 根据最新时间戳，取应用当前最小值。
   */
  LATEST_MIN((byte) 89),

  /**
   * 根据最新时间戳，取应用当前最大值。
   */
  LATEST_MAX((byte) 97);

  private byte tagType;
  private byte[] inBytes;

  private AggregationOperation(byte tagType) {
    this.tagType = tagType;
    this.inBytes = Bytes.toBytes(this.name());
  }

  /**
   * 获取当前聚合操作对应的属性对象。
   * @return 包含操作名称和字节数组形式名称的属性对象
   */
  public Attribute getAttribute() {
    return new Attribute(this.name(), this.inBytes);
  }

  public byte getTagType() {
    return tagType;
  }

  /**
   * 获取聚合操作名称的字节数组表示，用于HBase存储。
   * @return 名称字节数组的副本
   */
  public byte[] getInBytes() {
    return this.inBytes.clone();
  }

  /**
   * 根据字符串名称匹配对应的聚合操作枚举。
   * @param aggOpStr 聚合操作名称字符串
   * @return 匹配到的聚合操作枚举，无匹配则返回null
   */
  public static AggregationOperation getAggregationOperation(String aggOpStr) {
    for (AggregationOperation aggOp : AggregationOperation.values()) {
      if (aggOp.name().equals(aggOpStr)) {
        return aggOp;
      }
    }
    return null;
  }

}