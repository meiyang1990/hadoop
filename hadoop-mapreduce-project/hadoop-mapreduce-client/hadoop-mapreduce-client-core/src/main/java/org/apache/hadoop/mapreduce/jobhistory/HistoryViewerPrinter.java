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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
import java.io.PrintStream;

/**
 * 文件：HistoryViewerPrinter.java
 * 所属模块：MapReduce 客户端核心模块
 * 核心职责：定义作业历史记录打印器接口，为HistoryViewer提供不同格式输出作业历史的扩展点
 * 
 * 该接口被HistoryViewer使用，用于以不同格式输出作业历史信息，支持多种输出格式的扩展
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
interface HistoryViewerPrinter {

  /**
   * 将作业历史输出到指定的打印流
   * @param ps 目标输出打印流
   * @throws IOException 输出过程中发生IO异常时抛出
   */
  void print(PrintStream ps) throws IOException;
}