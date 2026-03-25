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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import java.io.Closeable;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;

/**
 * 时间线数据查询过滤器解析接口，定义查询条件解析的统一规范
 * 用于将用户传入的查询条件解析为可执行的过滤规则列表
 */
@Private
@Unstable
interface TimelineParser extends Closeable {
  /**
   * 执行解析操作，将输入的查询条件解析为过滤规则列表
   *
   * @return 解析完成的时间线过滤规则列表
   * @throws TimelineParseException 解析过程中发生错误时抛出
   */
  TimelineFilterList parse() throws TimelineParseException;
}