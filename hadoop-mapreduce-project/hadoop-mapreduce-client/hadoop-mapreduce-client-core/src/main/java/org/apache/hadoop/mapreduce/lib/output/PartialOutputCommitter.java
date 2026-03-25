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
package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 文件说明：Partial输出提交器接口，定义任务输出部分提交的能力，主要用于任务抢占场景
 *
 * 接口说明：支持OutputCommitter实现任务输出的部分提交，适用于任务被抢占后需要处理已有输出的场景，
 * 允许清理任务之前执行产生的部分提交输出，为任务重新执行准备干净环境。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface PartialOutputCommitter {

  /**
   * 清理当前任务之前执行产生的所有已部分提交输出
   * @param context 任务尝试上下文，用于获取任务信息和配置
   * @throws IOException 清理失败时抛出异常，此时任务状态可能未正确定义
   */
  public void cleanUpPartialOutputForTask(TaskAttemptContext context)
    throws IOException;

}