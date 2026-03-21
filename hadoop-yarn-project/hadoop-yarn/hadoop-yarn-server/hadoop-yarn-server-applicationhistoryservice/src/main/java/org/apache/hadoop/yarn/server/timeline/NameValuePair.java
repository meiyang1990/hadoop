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
package org.apache.hadoop.yarn.server.timeline;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 存储名称-值对的简单数据类，用于为TimelineReader指定查询过滤条件
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NameValuePair {
  String name;
  Object value;

  /**
   * 构造名称值对对象
   * @param name 过滤条件名称
   * @param value 过滤条件值
   */
  public NameValuePair(String name, Object value) {
    this.name = name;
    this.value = value;
  }

  /**
   * 获取名称
   * @return 名称
   */
  public String getName() {

    return name;
  }

  /**
   * 获取值
   * @return 值
   */
  public Object getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "{ name: " + name + ", value: " + value + " }";
  }
}