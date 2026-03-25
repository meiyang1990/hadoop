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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;

import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_RENAME;

/**
 * Manifest输出提交器的统计指标名称常量定义。
 * 需要与S3A的统计定义保持同步，保证云存储和manifest提交器统计指标对齐。
 * 本类仅存放常量定义，不包含任何业务逻辑。
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class ManifestCommitterStatisticNames {


  /** Amount of data committed: {@value}. */
  public static final String COMMITTER_BYTES_COMMITTED_COUNT =
      "committer_bytes_committed";

  /** Duration Tracking of time to commit an entire job: {@value}. */
  public static final String COMMITTER_COMMIT_JOB =
      "committer_commit_job";

  /** Number of files committed: {@value}. */
  public static final String COMMITTER_FILES_COMMITTED_COUNT =
      "committer_files_committed";

  /** "Count of successful tasks:: {@value}. */
  public static final String COMMITTER_TASKS_COMPLETED_COUNT =
      "committer_tasks_completed";

  /** Count of failed tasks: {@value}. */
  public static final String COMMITTER_TASKS_FAILED_COUNT =
      "committer_tasks_failed";

  /** Count of commits aborted: {@value}. */
  public static final String COMMITTER_COMMITS_ABORTED_COUNT =
      "committer_commits_aborted";

  /** Count of commits reverted: {@value}. */
  public static final String COMMITTER_COMMITS_REVERTED_COUNT =
      "committer_commits_reverted";

  /** Count of commits failed: {@value}. */
  public static final String COMMITTER_COMMITS_FAILED =
      "committer_commits" + StoreStatisticNames.SUFFIX_FAILURES;

  /**
   * 单个任务包含文件数量平均值统计。使用MeanStatistic计算。
   */
  public static final String COMMITTER_FILE_COUNT_MEAN =
      "committer_task_file_count";

  /**
   * 文件大小平均值统计。
   */
  public static final String COMMITTER_FILE_SIZE_MEAN =
      "committer_task_file_size";

  /**
   * 单个任务尝试目录数量平均值统计。
   */
  public static final String COMMITTER_TASK_DIRECTORY_COUNT_MEAN =
      "committer_task_directory_count";

  /**
   * 单个任务尝试目录树深度平均值统计。
   */
  public static final String COMMITTER_TASK_DIRECTORY_DEPTH_MEAN =
      "committer_task_directory_depth";

  /**
   * 单个任务包含文件数量平均值统计。使用MeanStatistic计算。
   */
  public static final String COMMITTER_TASK_FILE_COUNT_MEAN =
      "committer_task_file_count";

  /**
   * 单个任务文件大小平均值统计。使用MeanStatistic计算。
   */
  public static final String COMMITTER_TASK_FILE_SIZE_MEAN =
      "committer_task_file_size";

  /**
   * 任务manifest文件大小平均值统计。使用MeanStatistic计算。
   * 用于分析manifest文件大小是否过大，指导IO和内存优化。
   */
  public static final String COMMITTER_TASK_MANIFEST_FILE_SIZE =
      "committer_task_manifest_file_size";

  /**
   * 提交过程中重命名文件操作次数统计 {@value}.
   */
  public static final String OP_COMMIT_FILE_RENAME =
      "commit_file_rename";

  /**
   * 提交过程中从失败恢复的重命名操作次数统计 {@value}.
   */
  public static final String OP_COMMIT_FILE_RENAME_RECOVERED =
      "commit_file_rename_recovered";

  /** Directory creation {@value}. */
  public static final String OP_CREATE_DIRECTORIES = "op_create_directories";

  /** Creating a single directory {@value}. */
  public static final String OP_CREATE_ONE_DIRECTORY =
      "op_create_one_directory";

  /**
   * 删除目标目录树中已有文件操作次数统计
   *  {@value}.
   */
  public static final String OP_DELETE_FILE_UNDER_DESTINATION =
      "op_delete_file_under_destination";

  /** Directory scan {@value}. */
  public static final String OP_DIRECTORY_SCAN = "op_directory_scan";

  /**
   * 整体作业提交耗时统计 {@value}.
   */
  public static final String OP_STAGE_JOB_COMMIT = COMMITTER_COMMIT_JOB;

  /** {@value}. */
  public static final String OP_LOAD_ALL_MANIFESTS = "op_load_all_manifests";

  /**
   * 加载单个任务manifest文件耗时统计: {@value}.
   */
  public static final String OP_LOAD_MANIFEST = "op_load_manifest";

  /**
   * mkdir操作失败统计: {@value}.
   * 当mkdir()返回false时递增（例如路径已被文件占用）。
   */
  public static final String OP_MKDIRS_RETURNED_FALSE = "op_mkdir_returned_false";

  /**
   * msync操作统计: {@value}.
   * 此处与StoreStatisticNames.OP_MSYNC定义保持一致，重复定义是为了将该提交器隔离到独立JAR，便于测试。
   */
  public static final String OP_MSYNC = "op_msync";

  /**
   * 准备父目录操作耗时统计: {@value}.
   * 操作包括探测路径是否已被文件占用，如果是则删除该文件。
   */
  public static final String OP_PREPARE_DIR_ANCESTORS = "op_prepare_dir_ancestors";

  /** Rename a dir: {@value}. */
  public static final String OP_RENAME_DIR = OP_RENAME;


  /** Rename a file: {@value}. */
  public static final String OP_RENAME_FILE = OP_RENAME;

  /**
   * 保存任务manifest文件耗时统计: {@value}.
   */
  public static final String OP_SAVE_TASK_MANIFEST =
      "task_stage_save_task_manifest";

  /**
   * 保存作业汇总文件耗时统计: {@value}.
   */
  public static final String OP_SAVE_SUMMARY_FILE =
      "task_stage_save_summary_file";

  /**
   * 任务中止操作耗时统计: {@value}.
   */
  public static final String OP_STAGE_TASK_ABORT_TASK
      = "task_stage_abort_task";

  /**
   * 作业中止操作耗时统计: {@value}.
   */
  public static final String OP_STAGE_JOB_ABORT = "job_stage_abort";

  /**
   * 作业清理操作耗时统计: {@value}.
   */
  public static final String OP_STAGE_JOB_CLEANUP = "job_stage_cleanup";

  /**
   * 准备目标目录阶段耗时统计: {@value}.
   */
  public static final String OP_STAGE_JOB_CREATE_TARGET_DIRS =
      "job_stage_create_target_dirs";

  /**
   * 加载所有manifest文件阶段耗时统计: {@value}.
   */
  public static final String OP_STAGE_JOB_LOAD_MANIFESTS =
      "job_stage_load_manifests";

  /**
   * 文件重命名阶段耗时统计: {@value}.
   */
  public static final String OP_STAGE_JOB_RENAME_FILES =
      "job_stage_rename_files";


  /**
   * 作业初始化阶段耗时统计: {@value}.
   */
  public static final String OP_STAGE_JOB_SETUP = "job_stage_setup";

  /**
   * 作业保存_SUCCESS标记阶段耗时统计: {@value}.
   */
  public static final String OP_STAGE_JOB_SAVE_SUCCESS =
      "job_stage_save_success_marker";

  /**
   * 输出结果验证阶段（作业提交内）耗时统计: {@value}.
   */
  public static final String OP_STAGE_JOB_VALIDATE_OUTPUT =
      "job_stage_optional_validate_output";

  /**
   * 任务保存manifest文件阶段耗时统计: {@value}.
   */
  public static final String OP_STAGE_TASK_SAVE_MANIFEST =
      "task_stage_save_manifest";

  /**
   * 任务初始化阶段耗时统计: {@value}.
   */
  public static final String OP_STAGE_TASK_SETUP = "task_stage_setup";

  /**
   * 任务提交阶段耗时统计: {@value}.
   */
  public static final String OP_STAGE_TASK_COMMIT = "task_stage_commit";

  /** Task Scan directory Stage: {@value}. */
  public static final String OP_STAGE_TASK_SCAN_DIRECTORY
      = "task_stage_scan_directory";

  /** Delete a directory: {@value}. */
  public static final String OP_DELETE_DIR = "op_delete_dir";

  /**
   * 私有构造方法，禁止实例化此类。
   */
  private ManifestCommitterStatisticNames() {
  }
}