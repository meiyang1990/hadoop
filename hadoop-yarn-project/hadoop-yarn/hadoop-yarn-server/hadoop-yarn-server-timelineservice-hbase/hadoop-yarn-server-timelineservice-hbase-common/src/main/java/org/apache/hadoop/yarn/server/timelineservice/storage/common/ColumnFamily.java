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
 *  HBase列族的类型安全接口，为时间线服务HBase存储定义列族抽象。
 *  用于将Java类型映射到HBase物理列族，避免硬编码字节数组带来的类型不安全问题。
 *
 * @param <T> 该列族所属的表类型，限定列族只能作用于指定类型的表
 */
public interface ColumnFamily<T extends BaseTable<T>> {

  /**
   * 获取列族名称的字节数组表示。
   * 注意：如需避免重复克隆的性能开销，请调用方保留返回结果的本地副本。
   *
   * @return 列族名称的字节数组副本
   */
  byte[] getBytes();

}