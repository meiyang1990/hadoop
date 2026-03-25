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

package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import java.util.List;

/**
 * 获取任务诊断信息响应接口，定义了客户端查询MapReduce任务诊断信息后，服务端返回结果的结构规范。
 * 用于YARN与MapReduce客户端之间的RPC通信，承载任务运行出错后的诊断日志信息。
 */
public interface GetDiagnosticsResponse {
  /**
   * 获取全部诊断信息列表
   * @return 包含所有诊断信息字符串的列表
   */
  public abstract List<String> getDiagnosticsList();

  /**
   * 根据索引获取指定位置的诊断信息
   * @param index 诊断信息的索引位置
   * @return 指定位置的诊断信息字符串
   */
  public abstract String getDiagnostics(int index);

  /**
   * 获取当前诊断信息的总数量
   * @return 诊断信息条目数量
   */
  public abstract int getDiagnosticsCount();
  
  /**
   * 批量添加多个诊断信息到响应中
   * @param diagnostics 待添加的诊断信息列表
   */
  public abstract void addAllDiagnostics(List<String> diagnostics);

  /**
   * 添加单条诊断信息到响应中
   * @param diagnostic 待添加的诊断信息字符串
   */
  public abstract void addDiagnostics(String diagnostic);

  /**
   * 移除指定索引位置的诊断信息
   * @param index 待移除诊断信息的索引位置
   */
  public abstract void removeDiagnostics(int index);

  /**
   * 清空所有诊断信息
   */
  public abstract void clearDiagnostics();

}