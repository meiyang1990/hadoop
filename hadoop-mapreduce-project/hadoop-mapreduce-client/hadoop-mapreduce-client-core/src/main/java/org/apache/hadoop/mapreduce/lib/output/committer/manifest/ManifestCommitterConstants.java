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
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperationsThroughFileSystem;

/**
 * Manifest输出提交器的公共常量定义类，包含所有配置选项及其默认值，为整个ManifestCommitter模块提供统一的常量基础。
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class ManifestCommitterConstants {

  /**
   * 清单目录中清单文件的后缀名。
   */
  public static final String MANIFEST_SUFFIX = "-manifest.json";

  /**
   * 报告目录中摘要文件的前缀名。
   */
  public static final String SUMMARY_FILENAME_PREFIX = "summary-";

  /**
   * 根据作业ID构建摘要文件名的格式字符串。
   */
  public static final String SUMMARY_FILENAME_FORMAT =
      SUMMARY_FILENAME_PREFIX + "%s.json";

  /**
   * 临时文件重命名前使用的后缀名。
   */
  public static final String TMP_SUFFIX = ".tmp";

  /**
   * 应用尝试ID的初始值，在YARN和Spark作业中均统一使用0作为初始值。
   */
  public static final int INITIAL_APP_ATTEMPT_ID = 0;

  /**
   * 构建作业目录路径的格式字符串。
   */
  public static final String JOB_DIR_FORMAT_STR = "%s";

  /**
   * 构建作业尝试目录路径的格式字符串，使用作业尝试编号生成目录名，方便查找历史版本。
   */
  public static final String JOB_ATTEMPT_DIR_FORMAT_STR = "%02d";

  /**
   * 作业尝试目录下存放清单文件的子目录名称。
   */
  public static final String JOB_TASK_MANIFEST_SUBDIR = "manifests";

  /**
   * 作业尝试目录下存放任务尝试数据的子目录名称。
   */
  public static final String JOB_TASK_ATTEMPT_SUBDIR = "tasks";


  /**
   * 记录在提交成功标记文件中的Manifest提交器完整类名。
   */
  public static final String MANIFEST_COMMITTER_CLASSNAME =
      ManifestCommitter.class.getName();

  /**
   * 作业成功完成后创建的标记文件名。
   */
  public static final String SUCCESS_MARKER = "_SUCCESS";

  /** 是否默认创建作业成功目录标记，默认值: true。 */
  public static final boolean DEFAULT_CREATE_SUCCESSFUL_JOB_DIR_MARKER = true;

  /**
   * 作业提交过程中跟踪并保存到成功标记文件中的已提交对象数量上限。
   */
  public static final int SUCCESS_MARKER_FILE_LIMIT = 100;

  /**
   * Spark作业写入UUID的配置属性名，该参数在新版Spark中被恢复使用，如果存在则优先使用它代替MapReduce作业尝试ID。
   */
  public static final String SPARK_WRITE_UUID = "spark.sql.sources.writeJobUUID";

  /**
   * 标识作业ID来源为MapReduce作业ID的字符串，需要和AbstractS3ACommitter.JobUUIDSource保持一致。
   */
  public static final String JOB_ID_SOURCE_MAPREDUCE = "JobID";

  /**
   * 所有Manifest提交器配置选项的前缀。
   */
  public static final String OPT_PREFIX = "mapreduce.manifest.committer.";

  /**
   * 清理配置选项：是否在删除顶级目录前并行删除任务尝试目录，在部分云存储服务中可以提升速度避免超时。
   */
  public static final String OPT_CLEANUP_PARALLEL_DELETE =
      OPT_PREFIX + "cleanup.parallel.delete";

  /** 并行删除任务尝试目录默认值: true。 */
  public static final boolean OPT_CLEANUP_PARALLEL_DELETE_DIRS_DEFAULT = true;

  /**
   * 并行清理配置选项：是否优先尝试删除顶级基础目录，仅当顶级目录删除失败时才删除子目录，对Azure存储优化明显。
   */
  public static final String OPT_CLEANUP_PARALLEL_DELETE_BASE_FIRST =
      OPT_PREFIX + "cleanup.parallel.delete.base.first";

  /** 优先删除基础目录选项默认值: false。 */
  public static final boolean OPT_CLEANUP_PARALLEL_DELETE_BASE_FIRST_DEFAULT = false;

  /**
   * IO处理线程数量配置选项。
   */
  public static final String OPT_IO_PROCESSORS = OPT_PREFIX + "io.threads";

  /** IO处理线程数量默认值: 32。 */
  public static final int OPT_IO_PROCESSORS_DEFAULT = 32;

  /**
   * 保存作业摘要报告的目录配置选项，摘要即使作业失败也会保存。
   */
  public static final String OPT_SUMMARY_REPORT_DIR =
      OPT_PREFIX + "summary.report.directory";

  /**
   * 用于诊断的清单文件保存目录配置选项。
   */
  public static final String OPT_DIAGNOSTICS_MANIFEST_DIR =
      OPT_PREFIX + "diagnostics.manifest.directory";

  /**
   * 是否验证输出结果配置选项，会检查预期与实际文件长度，如果支持ETag还会验证ETag。
   */
  public static final String OPT_VALIDATE_OUTPUT = OPT_PREFIX + "validate.output";

  /** 输出验证默认值: false。 */
  public static final boolean OPT_VALIDATE_OUTPUT_DEFAULT = false;

  /**
   * 作业提交配置选项：是否在重命名前删除目标路径已存在的文件/目录，这兼容旧版FileOutputCommitter行为，但会增加额外删除操作。
   * 如果输出目录是新建或使用唯一文件名，不需要开启该选项。
   */
  public static final String OPT_DELETE_TARGET_FILES =
      OPT_PREFIX + "delete.target.files";

  /** 删除目标文件默认值: false。 */
  public static final boolean OPT_DELETE_TARGET_FILES_DEFAULT = false;

  /**
   * Manifest输出提交器工厂的完整类名。
   */
  public static final String MANIFEST_COMMITTER_FACTORY =
      ManifestCommitterFactory.class.getName();

  /**
   * 存储操作实现类的配置选项，允许文件系统和测试自定义实现。
   */
  public static final String OPT_STORE_OPERATIONS_CLASS = OPT_PREFIX + "store.operations.classname";

  /**
   * 存储操作实现类默认值，使用基于Hadoop FileSystem的实现。
   */
  public static final String STORE_OPERATIONS_CLASS_DEFAULT =
      ManifestStoreOperationsThroughFileSystem.class.getName();

  /**
   * 审计上下文中阶段属性的键名。
   */
  public static final String CONTEXT_ATTR_STAGE = "st";

  /**
   * 审计上下文中任务尝试ID属性的键名。
   */
  public static final String CONTEXT_ATTR_TASK_ATTEMPT_ID = "ta";

  /**
   * 动态分区功能能力标识，用于兼容Spark动态分区场景。
   */
  public static final String CAPABILITY_DYNAMIC_PARTITIONING =
      "mapreduce.job.committer.dynamic.partitioning";


  /**
   * 任务清单加载和入口文件写入之间队列容量配置选项，超过容量时入队操作会阻塞，期望本地写入速度快于读取速度保证队列及时排空。
   */
  public static final String OPT_WRITER_QUEUE_CAPACITY =
      OPT_PREFIX + "writer.queue.capacity";


  /** 写入队列容量默认值，和IO处理线程数保持一致。 */
  public static final int DEFAULT_WRITER_QUEUE_CAPACITY = OPT_IO_PROCESSORS_DEFAULT;

  /**
   * 保存任务清单时重试次数配置选项，使用保存再重命名机制，达到次数后放弃。
   */
  public static final String OPT_MANIFEST_SAVE_ATTEMPTS =
      OPT_PREFIX + "manifest.save.attempts";

  /** 保存清单重试次数默认值: 5。 */
  public static final int OPT_MANIFEST_SAVE_ATTEMPTS_DEFAULT = 5;

  private ManifestCommitterConstants() {
  }

}