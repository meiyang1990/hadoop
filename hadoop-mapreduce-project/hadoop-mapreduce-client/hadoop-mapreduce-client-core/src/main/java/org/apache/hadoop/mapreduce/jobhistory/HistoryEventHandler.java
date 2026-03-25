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

package org.apache.hadoop.mapreduce.jobhistory;

import java.io.IOException;

/**
 * 作业历史事件处理器接口，定义处理MapReduce作业历史事件的统一契约
 * 作为事件处理器的抽象接口，不同实现可对作业运行过程中产生的各类事件做不同处理
 * 核心职责是接收并处理作业历史事件，支撑作业历史数据的收集、存储与持久化
 */
public interface HistoryEventHandler {

  /**
   * 处理单个作业历史事件
   * @param event 待处理的作业历史事件对象
   * @throws IOException 处理事件过程中发生IO异常时抛出
   */
  void handleEvent(HistoryEvent event) throws IOException;

}