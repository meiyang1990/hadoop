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
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.SUCCESS_MARKER;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.TMP_SUFFIX;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_SAVE_SUMMARY_FILE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_COMMIT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_SAVE_SUCCESS;

/**
 * 作业提交完成阶段：将_SUCCESS标记文件保存到输出目标目录
 * 先写入作业尝试临时目录的临时文件，再原子重命名到最终位置，保证一致性
 * 最终返回成功标记文件的路径
 */
public class SaveSuccessFileStage extends
    AbstractJobOrTaskStage<ManifestSuccessData, Path> {

  private static final Logger LOG = LoggerFactory.getLogger(
      SaveSuccessFileStage.class);

  /**
   * 构造保存_SUCCESS标记文件阶段实例
   * @param stageConfig 阶段配置信息
   */
  public SaveSuccessFileStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_SAVE_SUCCESS, false);
  }

  /**
   * 获取当前阶段名称，始终返回作业提交阶段名称
   * @param arguments 输入参数，本次为成功元数据
   * @return 阶段名称
   */
  @Override
  protected String getStageName(ManifestSuccessData arguments) {
    // 始终归为作业提交阶段统计
    return OP_STAGE_JOB_COMMIT;
  }

  /**
   * 执行保存_SUCCESS标记文件的核心逻辑
   * @param successData 需要保存的作业成功元数据
   * @return 最终成功标记文件路径
   * @throws IOException 文件操作失败时抛出异常
   */
  @Override
  protected Path executeStage(final ManifestSuccessData successData)
      throws IOException {
    // 获取最终_SUCCESS文件的目标路径
    Path successFile = getStageConfig().getJobSuccessMarkerPath();
    // 先创建临时文件到作业尝试目录，避免失败留下不完整标记
    Path successTempFile = new Path(getJobAttemptDir(), SUCCESS_MARKER + TMP_SUFFIX);
    LOG.debug("{}: Saving _SUCCESS file to {} via {}", successFile,
        getName(),
        successTempFile);
    // 保存元数据到临时文件并重命名到最终位置，完成后统计操作指标
    saveManifest(() -> successData, successTempFile, successFile, OP_SAVE_SUMMARY_FILE);
    return successFile;
  }

}