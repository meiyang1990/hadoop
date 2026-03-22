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

package org.apache.hadoop.mapred.nativetask;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 原生任务数据通道类型枚举，定义了Native Task不同的数据传输方向类型
 * 用于标识原生任务处理模块与数据通道的数据交互方向
 */
@InterfaceAudience.Private
public enum DataChannel {
  /**
   * 仅从该通道读取数据
   */
  IN,
  /**
   * 仅向该通道写入数据
   */
  OUT,
  /**
   * 同时从该通道读取和写入数据
   */
  INOUT,
  /**
   * 该通道无数据交换
   */
  NONE
}