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

/**
 * 存储写入FlowRunTable的属性元组，用于HBase流运行表存储属性键值对
 * 定义了写入{@link FlowRunTable}时需要设置的属性键值对结构
 */
public class Attribute {
  private final String name;
  private final byte[] value;

  /**
   * 构造属性对象，对输入值字节数组做深拷贝
   * @param name 属性名称
   * @param value 属性值字节数组
   */
  public Attribute(String name, byte[] value) {
    this.name = name;
    this.value = value.clone();
  }

  /**
   * 获取属性名称
   * @return 属性名称字符串
   */
  public String getName() {
    return name;
  }

  /**
   * 获取属性值的深拷贝
   * @return 属性值字节数组副本
   */
  public byte[] getValue() {
    return value.clone();
  }
}