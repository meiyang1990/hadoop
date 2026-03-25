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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_DELETE_DIR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_ABORT_TASK;

/**
 * 任务中止执行处理阶段，属于Manifest提交器流程中的一个步骤
 * 
 * 核心职责是删除任务尝试目录清理临时数据，根据配置决定是否忽略删除过程中的异常
 */
public class AbortTaskStage extends
    AbstractJobOrTaskStage<Boolean, Path> {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbortTaskStage.class);

  /**
   * 构造任务中止处理阶段实例
   * @param stageConfig 阶段配置信息
   */
  public AbortTaskStage(final StageConfig stageConfig) {
    super(true, stageConfig, OP_STAGE_TASK_ABORT_TASK, false);
  }

  /**
   * 执行任务中止逻辑：删除任务尝试的临时工作目录
   * @param suppressExceptions 是否忽略删除过程中抛出的异常
   * @return 被删除的任务尝试目录路径
   * @throws IOException 当不忽略异常且删除操作失败时抛出IO异常
   */
  @Override
  protected Path executeStage(final Boolean suppressExceptions)
      throws IOException {
    final Path dir = getTaskAttemptDir();
    if (dir != null) {
      LOG.info("{}: Deleting task attempt directory {}", getName(), dir);
      if (suppressExceptions) {
        // 递归删除目录并忽略异常
        deleteRecursiveSuppressingExceptions(dir, OP_DELETE_DIR);
      } else {
        // 递归删除目录，异常向外抛出
        deleteRecursive(dir, OP_DELETE_DIR);
      }
    }
    return dir;
  }

}