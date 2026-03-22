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

import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;

/**
 * 获取任务报告响应接口，定义了GetTaskReports RPC调用返回结果的结构规范
 * 用于ApplicationMaster向ResourceManager获取作业中所有任务的状态报告
 */
public interface GetTaskReportsResponse {
  /**
   * 获取所有任务报告列表
   * @return 包含所有任务报告的列表
   */
  public abstract List<TaskReport> getTaskReportList();
  
  /**
   * 根据索引获取指定位置的任务报告
   * @param index 任务报告在列表中的索引
   * @return 指定索引的任务报告
   */
  public abstract TaskReport getTaskReport(int index);
  
  /**
   * 获取任务报告的总数量
   * @return 任务报告数量
   */
  public abstract int getTaskReportCount();
  
  /**
   * 批量添加多个任务报告到响应中
   * @param taskReports 要添加的任务报告列表
   */
  public abstract void addAllTaskReports(List<TaskReport> taskReports);
  
  /**
   * 添加单个任务报告到响应中
   * @param taskReport 要添加的任务报告
   */
  public abstract void addTaskReport(TaskReport taskReport);
  
  /**
   * 移除指定索引位置的任务报告
   * @param index 要移除的任务报告索引
   */
  public abstract void removeTaskReport(int index);
  
  /**
   * 清空所有任务报告
   */
  public abstract void clearTaskReports();
}