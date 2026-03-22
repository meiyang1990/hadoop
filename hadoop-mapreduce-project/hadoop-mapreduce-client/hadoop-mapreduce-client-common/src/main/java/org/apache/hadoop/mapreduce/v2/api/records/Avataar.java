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

package org.apache.hadoop.mapreduce.v2.api.records;

/**
 * MapReduce任务尝试类型枚举，标识该任务尝试是初始任务还是推测执行任务
 * 用于YARN应用Master区分不同类型的任务尝试，支持推测执行功能
 */
public enum Avataar {
  /** 初始创建的普通任务尝试 */
  VIRGIN,
  /** 推测执行生成的任务尝试 */
  SPECULATIVE
}