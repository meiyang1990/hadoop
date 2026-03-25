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
 * 定义FlowRunTable中HBase数据聚合压实的维度类型，用于时间线服务流数据分层聚合存储。
 */
public enum AggregationCompactionDimension {

  /**
   * 按应用ID维度进行聚合压实。
   */
  APPLICATION_ID((byte) 101);

  private byte tagType;
  private byte[] inBytes;

  /**
   * 构造聚合压实维度枚举，初始化维度标识和字节表示。
   * @param tagType 维度类型字节标识
   */
  private AggregationCompactionDimension(byte tagType) {
    this.tagType = tagType;
    this.inBytes = Bytes.toBytes(this.name());
  }

  /**
   * 根据属性值构造当前维度对应的属性对象。
   * @param attributeValue 属性值字符串
   * @return 包装后的属性对象
   */
  public Attribute getAttribute(String attributeValue) {
    return new Attribute(this.name(), Bytes.toBytes(attributeValue));
  }

  /**
   * 获取维度类型的字节标识。
   * @return 字节标识
   */
  public byte getTagType() {
    return tagType;
  }

  /**
   * 获取维度名称的字节数组表示。
   * @return 维度名称字节数组的副本
   */
  public byte[] getInBytes() {
    return this.inBytes.clone();
  }

  /**
   * 根据字符串名称查找对应的聚合压实维度枚举。
   * @param aggCompactDimStr 维度名称字符串
   * @return 匹配的维度枚举，未找到返回null
   */
  public static AggregationCompactionDimension
      getAggregationCompactionDimension(String aggCompactDimStr) {
    for (AggregationCompactionDimension aggDim : AggregationCompactionDimension
        .values()) {
      if (aggDim.name().equals(aggCompactDimStr)) {
        return aggDim;
      }
    }
    return null;
  }
}