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

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_SETUP;

/**
 * 任务准备阶段，负责MapReduce任务尝试的目录初始化工作
 * 执行流程：先确认作业尝试目录已存在，再创建当前任务尝试的工作目录
 * 该阶段在作业启动后、任务实际执行前被调用，入参仅用于日志输出
 */
public class SetupTaskStage extends
    AbstractJobOrTaskStage<String, Path> {

  /**
   * 构造任务准备阶段实例
   * @param stageConfig 阶段配置信息
   */
  public SetupTaskStage(final StageConfig stageConfig) {
    super(true, stageConfig, OP_STAGE_TASK_SETUP, false);
  }

  /**
   * 执行任务准备逻辑，创建任务尝试工作目录
   * @param name 任务名称，仅用于日志输出
   * @return 创建好的任务尝试目录路径
   * @throws IOException IO操作失败时抛出
   */
  @Override
  protected Path executeStage(final String name) throws IOException {
    return createNewDirectory("Task setup " + name,
        requireNonNull(getTaskAttemptDir(), "No task attempt directory"));
  }

}