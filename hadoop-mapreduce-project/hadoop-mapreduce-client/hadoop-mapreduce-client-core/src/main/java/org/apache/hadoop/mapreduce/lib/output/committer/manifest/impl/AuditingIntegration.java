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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.audit.CommonAuditContext;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConfig;

import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_JOB_ID;
import static org.apache.hadoop.fs.audit.CommonAuditContext.currentAuditContext;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.CONTEXT_ATTR_STAGE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.CONTEXT_ATTR_TASK_ATTEMPT_ID;

/**
 * 支持与Hadoop 3.3.2+审计功能集成的工具类。
 * 该类必须是唯一使用fs.audit相关方法的位置，因此在向后移植时可以直接用桩类替换。
 */
@InterfaceAudience.Private
public final class AuditingIntegration {
  private AuditingIntegration() {
  }

  /**
   * 在提交器进入工作流程时，向当前审计上下文添加作业ID，若存在任务尝试ID也一并添加。
   * @param committerConfig 清单提交器配置
   */
  public static void updateCommonContextOnCommitterEntry(
      ManifestCommitterConfig committerConfig) {
    CommonAuditContext context = currentAuditContext();
    context.put(PARAM_JOB_ID,
        committerConfig.getJobUniqueId());
    // 添加任务尝试ID（如果存在）
    if (!committerConfig.getTaskAttemptId().isEmpty()) {
      context.put(CONTEXT_ATTR_TASK_ATTEMPT_ID,
          committerConfig.getTaskAttemptId());
    }
  }

  /**
   * 进入提交阶段的回调，设置当前活跃阶段并更新审计上下文。
   * @param stage 新阶段名称
   */
  public static void enterStage(String stage) {
    currentAuditContext().put(CONTEXT_ATTR_STAGE, stage);
  }

  /**
   * 退出提交阶段时，从公共审计上下文中移除阶段信息。
   */
  public static void exitStage() {
    currentAuditContext().remove(CONTEXT_ATTR_STAGE);
  }

  /**
   * 任务或作业提交结束时，移除上下文中的提交相关信息。
   */
  public static void updateCommonContextOnCommitterExit() {
    currentAuditContext().remove(PARAM_JOB_ID);
    currentAuditContext().remove(CONTEXT_ATTR_TASK_ATTEMPT_ID);
  }

  /**
   * 为工作线程更新线程上下文，添加作业ID和阶段名称。
   * 必须在辅助线程执行方法的开头调用，确保所有操作都能被正确标注作业和阶段信息。
   * @param jobId 作业ID
   * @param stage 阶段名称
   */
  public static void enterStageWorker(String jobId, String stage) {
    CommonAuditContext context = currentAuditContext();
    context.put(PARAM_JOB_ID, jobId);
    context.put(CONTEXT_ATTR_STAGE, stage);
  }
}