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
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_SAVE_TASK_MANIFEST;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_SAVE_MANIFEST;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.manifestPathForTask;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.manifestTempPathForTaskAttempt;

/**
 * 文件作用：保存任务输出清单到作业尝试目录的执行阶段，是Manifest提交器流程中的一个步骤
 * 核心功能：通过先写入临时文件再重命名的原子写方式，保证任务清单文件的一致性；支持重试应对并发竞态问题
 * 业务场景：在MapReduce任务提交阶段，当前任务尝试完成输出后，将输出文件清单保存到集群，供后续作业提交阶段汇总处理
 * <p>
 * 最终文件名使用任务ID标识，保证同一个任务的多次尝试会覆盖生成最终清单
 * 临时文件名同时使用任务ID和任务尝试ID，不同尝试间互不干扰
 * 重命名前会先删除已存在的最终文件，避免冲突
 * <p>
 * 针对任务多次尝试提交的场景做了竞态处理：
 * <ol>
 *   <li>如果前一次任务尝试已提交成功，本次重命名会直接覆盖，两次尝试均会报告成功</li>
 *   <li>如果本次写入后，另一个任务尝试覆盖了最终文件，两次尝试也均报告成功</li>
 *   <li>如果另一个任务在删除和重命名操作之间写入，重试机制会重复写入并最终保证成功</li>
 * </ol>
 * 最终只会保留最后一次成功尝试的任务清单，符合MapReduce任务提交语义：只要有一次尝试成功即可
 * 重试机制用于解决分区集群中原始任务尝试提交还在进行时，二次尝试仍能成功提交的问题
 * <p>
 * 返回值：(最终保存的清单文件路径, 清单对象)
 */
public class SaveTaskManifestStage extends
    AbstractJobOrTaskStage<Supplier<TaskManifest>, Pair<Path, TaskManifest>> {

  private static final Logger LOG = LoggerFactory.getLogger(
      SaveTaskManifestStage.class);

  /**
   * 构造保存任务清单执行阶段实例
   * @param stageConfig 阶段配置，包含作业上下文、文件系统等信息
   */
  public SaveTaskManifestStage(final StageConfig stageConfig) {
    super(true, stageConfig, OP_STAGE_TASK_SAVE_MANIFEST, false);
  }

  /**
   * 执行保存任务清单流程：生成清单写入临时文件，然后原子重命名到最终路径
   * 每次重试都会重新生成清单对象
   * @param manifestSource 任务清单对象的供应者，用于获取当前任务尝试的输出清单
   *
   * @return 对，包含最终清单文件路径和清单对象
   * @throws IOException IO操作失败时抛出
   */
  @Override
  protected Pair<Path, TaskManifest> executeStage(Supplier<TaskManifest> manifestSource)
      throws IOException {

    // 获取任务清单的存储目录
    final Path manifestDir = getTaskManifestDir();
    // 根据任务ID生成最终清单文件路径
    Path manifestFile = manifestPathForTask(manifestDir,
        getRequiredTaskId());
    // 根据任务尝试ID生成临时清单文件路径
    Path manifestTempFile = manifestTempPathForTaskAttempt(manifestDir,
        getRequiredTaskAttemptId());
    // 打印日志记录保存位置
    LOG.info("{}: Saving manifest file to {}", getName(), manifestFile);
    // 执行保存操作，包含临时文件写入和原子重命名重试流程
    final TaskManifest manifest =
        saveManifest(manifestSource, manifestTempFile, manifestFile, OP_SAVE_TASK_MANIFEST);
    // 返回结果
    return Pair.of(manifestFile, manifest);
  }

}