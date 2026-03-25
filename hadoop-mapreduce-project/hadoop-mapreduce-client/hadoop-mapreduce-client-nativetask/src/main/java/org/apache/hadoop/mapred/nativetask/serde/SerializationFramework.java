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
package org.apache.hadoop.mapred.nativetask.serde;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 原生任务序列化框架类型枚举
 * 定义了MapReduce本地任务支持的两种序列化框架类型，用于选择Java Writable序列化还是原生序列化
 */
@InterfaceAudience.Private
public enum SerializationFramework {
  WRITABLE_SERIALIZATION(0), NATIVE_SERIALIZATION(1);

  private int type;

  /**
   * 构造序列化框架枚举实例
   * @param type 序列化框架类型编号
   */
  SerializationFramework(int type) {
    this.type = type;
  }

  /**
   * 获取序列化框架的类型编号
   * @return 序列化框架类型编号
   */
  public int getType() {
    return type;
  }
};