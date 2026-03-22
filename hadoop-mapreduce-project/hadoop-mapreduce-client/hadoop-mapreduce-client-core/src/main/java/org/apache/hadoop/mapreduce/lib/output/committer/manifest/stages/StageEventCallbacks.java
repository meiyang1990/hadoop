// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages;

/**
 * 提交任务处理流水线阶段的进入/退出事件回调接口，用于监控阶段执行流程。
 * 为Manifest输出提交器的阶段化处理流程提供事件钩子，支持日志记录、监控统计等扩展能力。
 */
public interface StageEventCallbacks {

  /**
   * 进入新阶段时的回调方法。
   * @param stage 进入的阶段名称
   */
  void enterStage(String stage);

  /**
   * 退出阶段时的回调方法。
   * @param stage 退出的阶段名称
   */
  void exitStage(String stage);
}