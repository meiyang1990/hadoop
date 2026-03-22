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

import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_DELETE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_SETUP;

/**
 * 文件：作业设置阶段，用于在Manifest提交器流程中创建作业尝试目录
 * 核心职责：完成MapReduce作业提交前的初始化工作，创建所需目录结构，可选清理旧的成功标记
 * 执行前提：调用此阶段前，作业尝试目录必须不存在
 */
public class SetupJobStage extends
    AbstractJobOrTaskStage<Boolean, Path> {

  private static final Logger LOG = LoggerFactory.getLogger(
      SetupJobStage.class);

  /**
   * 构造作业设置阶段实例，初始化阶段配置
   * @param stageConfig 阶段通用配置
   */
  public SetupJobStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_SETUP, false);
  }

  /**
   * 执行作业设置阶段逻辑，创建作业所需目录结构，可选删除旧成功标记
   * @param deleteMarker 是否需要删除旧的作业成功标记
   * @return 创建完成的作业尝试目录路径
   * @throws IOException IO操作失败时抛出异常
   */
  @Override
  protected Path executeStage(final Boolean deleteMarker) throws IOException {
    // 获取当前作业尝试目录路径
    final Path path = getJobAttemptDir();
    LOG.info("{}: Creating Job Attempt directory {}", getName(), path);
    // 创建全新的作业尝试目录，若目录已存在则报错
    createNewDirectory("Job setup", path);
    // 创建全新的任务清单目录，用于存储各个任务的输出清单
    createNewDirectory("Creating task manifest dir", getTaskManifestDir());
    // 配置要求删除成功标记时，执行删除操作
    if (deleteMarker) {
      deleteFile(getStageConfig().getJobSuccessMarkerPath(), OP_DELETE);
    }
    return path;
  }

}