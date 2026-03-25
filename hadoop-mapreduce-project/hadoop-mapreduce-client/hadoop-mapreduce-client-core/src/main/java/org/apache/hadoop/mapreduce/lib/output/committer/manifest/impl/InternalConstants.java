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

import java.util.Set;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;

import org.apache.hadoop.classification.InterfaceAudience;

import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_CONTINUE_LIST_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_LIST_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_DELETE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_GET_FILE_STATUS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_IS_DIRECTORY;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_IS_FILE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_LIST_STATUS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_MKDIRS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.STORE_IO_RATE_LIMITED;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.*;

/**
 * Manifest输出提交器内部使用的常量定义类，包含统计项名称、格式字符串、不兼容文件系统配置等。
 * 所有常量均为全局共享，不允许实例化此类。
 */
@InterfaceAudience.Private
public final class InternalConstants {
  private InternalConstants() {
  }

  /**
   * 耗时统计名称数组，收集各个阶段和操作的执行时长统计。
   */
  public static final String[] DURATION_STATISTICS = {

      /* 作业生命周期各个阶段统计 */
      OP_STAGE_JOB_ABORT,
      OP_STAGE_JOB_CLEANUP,
      OP_STAGE_JOB_COMMIT,
      OP_STAGE_JOB_CREATE_TARGET_DIRS,
      OP_STAGE_JOB_LOAD_MANIFESTS,
      OP_STAGE_JOB_RENAME_FILES,
      OP_STAGE_JOB_SAVE_SUCCESS,
      OP_STAGE_JOB_SETUP,
      OP_STAGE_JOB_VALIDATE_OUTPUT,

      /* 任务生命周期各个阶段统计 */

      OP_STAGE_TASK_ABORT_TASK,
      OP_STAGE_TASK_COMMIT,
      OP_STAGE_TASK_SAVE_MANIFEST,
      OP_STAGE_TASK_SCAN_DIRECTORY,
      OP_STAGE_TASK_SETUP,

      /* 底层文件系统操作统计 */
      OP_COMMIT_FILE_RENAME,
      OP_CREATE_DIRECTORIES,
      OP_CREATE_ONE_DIRECTORY,
      OP_DIRECTORY_SCAN,
      OP_DELETE,
      OP_DELETE_DIR,
      OP_DELETE_FILE_UNDER_DESTINATION,
      OP_GET_FILE_STATUS,
      OP_IS_DIRECTORY,
      OP_IS_FILE,
      OP_LIST_STATUS,
      OP_LOAD_MANIFEST,
      OP_LOAD_ALL_MANIFESTS,
      OP_MKDIRS,
      OP_MKDIRS_RETURNED_FALSE,
      OP_MSYNC,
      OP_PREPARE_DIR_ANCESTORS,
      OP_RENAME_FILE,
      OP_SAVE_SUMMARY_FILE,
      OP_SAVE_TASK_MANIFEST,

      OBJECT_LIST_REQUEST,
      OBJECT_CONTINUE_LIST_REQUEST,

      STORE_IO_RATE_LIMITED
  };

  /**
   * 计数器名称数组，收集各个计数类型指标统计。
   */
  public static final String[] COUNTER_STATISTICS = {
      COMMITTER_BYTES_COMMITTED_COUNT,
      COMMITTER_FILES_COMMITTED_COUNT,
      COMMITTER_TASKS_COMPLETED_COUNT,
      COMMITTER_TASKS_FAILED_COUNT,
      COMMITTER_TASK_DIRECTORY_COUNT_MEAN,
      COMMITTER_TASK_DIRECTORY_DEPTH_MEAN,
      COMMITTER_TASK_FILE_COUNT_MEAN,
      COMMITTER_TASK_FILE_SIZE_MEAN,
      COMMITTER_TASK_MANIFEST_FILE_SIZE,
      OP_COMMIT_FILE_RENAME_RECOVERED,
  };

  /**
   * ABFS存储连接器超时错误标识字符串。
   */
  public static final String OPERATION_TIMED_OUT = "OperationTimedOut";

  /**
   * 任务尝试日志名称格式字符串。
   */
  public static final String NAME_FORMAT_TASK_ATTEMPT = "[Task-Attempt %s]";

  /**
   * 作业尝试日志名称格式字符串。
   */
  public static final String NAME_FORMAT_JOB_ATTEMPT = "[Job-Attempt %s]";

  /** 不兼容当前Manifest提交器的文件系统schema集合 */
  public static final Set<String> UNSUPPORTED_FS_SCHEMAS =
      ImmutableSet.of("s3a", "wasb");

  /**
   * 保存重试时的间隔时间，单位为毫秒，默认值500ms。
   */
  public static final int SAVE_SLEEP_INTERVAL = 500;

}