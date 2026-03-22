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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_COMMIT;

/**
 * 文件说明：任务提交阶段，基于Manifest提交协议实现任务尝试提交
 * 核心功能：扫描任务尝试输出目录，收集任务产出文件信息，生成并保存任务清单文件，供后续作业提交阶段使用
 */

/**
 * 提交任务尝试阶段，先扫描任务尝试目录收集产出文件，再生成并保存任务清单
 */
public class CommitTaskStage extends
    AbstractJobOrTaskStage<Void, CommitTaskStage.Result> {
  private static final Logger LOG = LoggerFactory.getLogger(
      CommitTaskStage.class);

  /**
   * 构造任务提交阶段实例
   * @param stageConfig 阶段配置
   */
  public CommitTaskStage(final StageConfig stageConfig) {
    super(true, stageConfig, OP_STAGE_TASK_COMMIT, false);
  }

  /**
   * 执行任务提交流程：先扫描目录收集文件，再保存任务清单
   * @param arguments 入参，本阶段无参数
   * @return 任务提交结果，包含清单保存路径和清单对象
   * @throws IOException IO异常
   */
  @Override
  protected CommitTaskStage.Result executeStage(final Void arguments)
      throws IOException {
    LOG.info("{}: Committing task \"{}\"", getName(), getTaskAttemptId());

    // 初始化目录扫描阶段并执行扫描
    final TaskAttemptScanDirectoryStage scanStage =
        new TaskAttemptScanDirectoryStage(getStageConfig());
    TaskManifest manifest = scanStage.apply(arguments);

    // 将扫描阶段耗时统计到任务提交总统计中
    scanStage.addExecutionDurationToStatistics(getIOStatistics(), OP_STAGE_TASK_COMMIT);

    // 执行任务清单保存（支持重试），聚合IO统计信息到清单中
    Pair<Path, TaskManifest> p = new SaveTaskManifestStage(getStageConfig())
        .apply(() -> {
          // 创建IO统计快照并聚合当前阶段统计
          final IOStatisticsSnapshot manifestStats = snapshotIOStatistics();
          manifestStats.aggregate(getIOStatistics());
          manifest.setIOStatistics(manifestStats);
          return manifest;
        });
    return new CommitTaskStage.Result(p.getLeft(), p.getRight());
  }

  /**
   * 任务提交阶段结果封装类，保存任务清单的保存路径和清单对象
   */
  public static final class Result {
    /** 任务清单保存路径 */
    private final Path path;
    /** 任务清单对象 */
    private final TaskManifest taskManifest;

    /**
     * 构造结果对象
     * @param path 任务清单保存路径
     * @param taskManifest 任务清单对象
     */
    public Result(Path path,
        TaskManifest taskManifest) {
      this.path = path;
      this.taskManifest = taskManifest;
    }

    /**
     * 获取任务清单保存路径
     * @return 任务清单保存路径
     */
    public Path getPath() {
      return path;
    }

    /**
     * 获取任务清单对象
     * @return 任务清单对象
     */
    public TaskManifest getTaskManifest() {
      return taskManifest;
    }

    @Override
    public String toString() {
      return "Result{path=" + path + '}';
    }
  }
}