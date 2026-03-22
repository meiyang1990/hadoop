// 这个文件已经全部加上中文注释
/**
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
package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.util.Apps;

/**
 * MapReduce作业配置项常量接口，统一定义了所有MapReduce作业相关的配置属性键名和默认值
 * 确保Job和JobContext等组件使用的配置名称保持一致，为整个MapReduce框架提供统一的配置常量
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface MRJobConfig {

  // 用于MapTask的排序类配置
  public static final String MAP_SORT_CLASS = "map.sort.class";

  // 统一存放所有配置属性名，保证Job和JobContext配置一致性
  public static final String INPUT_FORMAT_CLASS_ATTR = "mapreduce.job.inputformat.class";

  public static final String MAP_CLASS_ATTR = "mapreduce.job.map.class";

  public static final String MAP_OUTPUT_COLLECTOR_CLASS_ATTR
                                  = "mapreduce.job.map.output.collector.class";

  public static final String COMBINE_CLASS_ATTR = "mapreduce.job.combine.class";

  public static final String REDUCE_CLASS_ATTR = "mapreduce.job.reduce.class";

  public static final String OUTPUT_FORMAT_CLASS_ATTR = "mapreduce.job.outputformat.class";

  public static final String PARTITIONER_CLASS_ATTR = "mapreduce.job.partitioner.class";

  public static final String SETUP_CLEANUP_NEEDED = "mapreduce.job.committer.setup.cleanup.needed";

  public static final String TASK_CLEANUP_NEEDED = "mapreduce.job.committer.task.cleanup.needed";

  // 作业本地文件系统单磁盘容量限制
  public static final String JOB_SINGLE_DISK_LIMIT_BYTES =
          "mapreduce.job.local-fs.single-disk-limit.bytes";
  // 负值表示不限制磁盘容量
  public static final long DEFAULT_JOB_SINGLE_DISK_LIMIT_BYTES = -1;

  // DFS存储容量超限是否杀死任务配置
  public static final String JOB_DFS_STORAGE_CAPACITY_KILL_LIMIT_EXCEED =
      "mapreduce.job.dfs.storage.capacity.kill-limit-exceed";
  public static final boolean DEFAULT_JOB_DFS_STORAGE_CAPACITY_KILL_LIMIT_EXCEED = false;
  // 本地单磁盘容量超限是否杀死任务配置
  public static final String JOB_SINGLE_DISK_LIMIT_KILL_LIMIT_EXCEED =
      "mapreduce.job.local-fs.single-disk-limit.check.kill-limit-exceed";
  // 设置为false仅记录日志不杀死任务
  public static final boolean DEFAULT_JOB_SINGLE_DISK_LIMIT_KILL_LIMIT_EXCEED = true;

  // 单磁盘容量检查间隔配置
  public static final String JOB_SINGLE_DISK_LIMIT_CHECK_INTERVAL_MS =
      "mapreduce.job.local-fs.single-disk-limit.check.interval-ms";
  public static final long DEFAULT_JOB_SINGLE_DISK_LIMIT_CHECK_INTERVAL_MS = 5000;

  // 任务本地磁盘写入容量限制
  public static final String TASK_LOCAL_WRITE_LIMIT_BYTES =
          "mapreduce.task.local-fs.write-limit.bytes";
  // 负值表示不限制
  public static final long DEFAULT_TASK_LOCAL_WRITE_LIMIT_BYTES = -1;

  public static final String JAR = "mapreduce.job.jar";

  public static final String ID = "mapreduce.job.id";

  public static final String JOB_NAME = "mapreduce.job.name";

  public static final String JAR_UNPACK_PATTERN = "mapreduce.job.jar.unpack.pattern";

  public static final String USER_NAME = "mapreduce.job.user.name";

  public static final String PRIORITY = "mapreduce.job.priority";

  public static final String QUEUE_NAME = "mapreduce.job.queuename";

  /**
   *  适用于作业所有容器的节点标签表达式
   */
  public static final String JOB_NODE_LABEL_EXP = "mapreduce.job.node-label-expression";

  /**
   * 适用于AM容器的节点标签表达式
   */
  public static final String AM_NODE_LABEL_EXP = "mapreduce.job.am.node-label-expression";

  /**
   *  适用于Map容器的节点标签表达式
   */
  public static final String MAP_NODE_LABEL_EXP = "mapreduce.map.node-label-expression";

  /**
   * 适用于Reduce容器的节点标签表达式
   */
  public static final String REDUCE_NODE_LABEL_EXP = "mapreduce.reduce.node-label-expression";

  /**
   * 指定AM严格本地化匹配，逗号分隔机架/节点列表
   * 语法: /rack 或 /rack/node 或 node (默认机架为/default-rack)
   */
  public static final String AM_STRICT_LOCALITY = "mapreduce.job.am.strict-locality";

  public static final String RESERVATION_ID = "mapreduce.job.reservation.id";

  public static final String JOB_TAGS = "mapreduce.job.tags";

  public static final String JVM_NUMTASKS_TORUN = "mapreduce.job.jvm.numtasks";

  public static final String SPLIT_FILE = "mapreduce.job.splitfile";

  public static final String SPLIT_METAINFO_MAXSIZE = "mapreduce.job.split.metainfo.maxsize";
  public static final long DEFAULT_SPLIT_METAINFO_MAXSIZE = 10000000L;

  public static final String NUM_MAPS = "mapreduce.job.maps";

  public static final String MAX_TASK_FAILURES_PER_TRACKER = "mapreduce.job.maxtaskfailures.per.tracker";

  public static final String COMPLETED_MAPS_FOR_REDUCE_SLOWSTART = "mapreduce.job.reduce.slowstart.completedmaps";

  public static final String NUM_REDUCES = "mapreduce.job.reduces";

  public static final String SKIP_RECORDS = "mapreduce.job.skiprecords";

  public static final String SKIP_OUTDIR = "mapreduce.job.skip.outdir";

  // SPECULATIVE_SLOWNODE_THRESHOLD 已废弃，将在未来版本删除
  @Deprecated
  public static final String SPECULATIVE_SLOWNODE_THRESHOLD = "mapreduce.job.speculative.slownodethreshold";

  public static final String SPECULATIVE_SLOWTASK_THRESHOLD = "mapreduce.job.speculative.slowtaskthreshold";

  // SPECULATIVECAP 已废弃，将在未来版本删除
  @Deprecated
  public static final String SPECULATIVECAP = "mapreduce.job.speculative.speculativecap";

  // 推测执行中运行任务容量占总任务容量的上限比例
  public static final String SPECULATIVECAP_RUNNING_TASKS =
      "mapreduce.job.speculative.speculative-cap-running-tasks";
  public static final double DEFAULT_SPECULATIVECAP_RUNNING_TASKS =
      0.1;

  // 推测执行总任务容量占所有任务容量的上限比例
  public static final String SPECULATIVECAP_TOTAL_TASKS =
      "mapreduce.job.speculative.speculative-cap-total-tasks";
  public static final double DEFAULT_SPECULATIVECAP_TOTAL_TASKS =
      0.01;

  // 允许开启推测执行的最少任务数
  public static final String SPECULATIVE_MINIMUM_ALLOWED_TASKS =
      "mapreduce.job.speculative.minimum-allowed-tasks";
  public static final int DEFAULT_SPECULATIVE_MINIMUM_ALLOWED_TASKS =
      10;

  // 未触发推测执行后重试间隔
  public static final String SPECULATIVE_RETRY_AFTER_NO_SPECULATE =
      "mapreduce.job.speculative.retry-after-no-speculate";
  public static final long DEFAULT_SPECULATIVE_RETRY_AFTER_NO_SPECULATE =
      1000L;

  // 触发推测执行后重试间隔
  public static final String SPECULATIVE_RETRY_AFTER_SPECULATE =
      "mapreduce.job.speculative.retry-after-speculate";
  public static final long DEFAULT_SPECULATIVE_RETRY_AFTER_SPECULATE =
      15000L;

  public static final String JOB_LOCAL_DIR = "mapreduce.job.local.dir";

  public static final String OUTPUT_KEY_CLASS = "mapreduce.job.output.key.class";

  public static final String OUTPUT_VALUE_CLASS = "mapreduce.job.output.value.class";

  public static final String KEY_COMPARATOR = "mapreduce.job.output.key.comparator.class";

  public static final String COMBINER_GROUP_COMPARATOR_CLASS = "mapreduce.job.combiner.group.comparator.class";

  public static final String GROUP_COMPARATOR_CLASS = "mapreduce.job.output.group.comparator.class";

  public static final String WORKING_DIR = "mapreduce.job.working.dir";

  public static final String CLASSPATH_ARCHIVES = "mapreduce.job.classpath.archives";

  public static final String CLASSPATH_FILES = "mapreduce.job.classpath.files";

  public static final String CACHE_FILES = "mapreduce.job.cache.files";

  public static final String CACHE_ARCHIVES = "mapreduce.job.cache.archives";

  public static final String CACHE_FILES_SIZES = "mapreduce.job.cache.files.filesizes"; // internal use only

  public static final String CACHE_ARCHIVES_SIZES = "mapreduce.job.cache.archives.filesizes"; // ditto

  public static final String CACHE_LOCALFILES = "mapreduce.job.cache.local.files";

  public static final String CACHE_LOCALARCHIVES = "mapreduce.job.cache.local.archives";

  public static final String CACHE_FILE_TIMESTAMPS = "mapreduce.job.cache.files.timestamps";

  public static final String CACHE_ARCHIVES_TIMESTAMPS = "mapreduce.job.cache.archives.timestamps";

  public static final String CACHE_FILE_VISIBILITIES = "mapreduce.job.cache.files.visibilities";

  public static final String CACHE_ARCHIVES_VISIBILITIES = "mapreduce.job.cache.archives.visibilities";

  /**
   * 该参数控制NodeManager上本地化作业jar的可见性。如果设置为true，可见性设为
   * LocalResourceVisibility.PUBLIC；如果为false，可见性设为
   * LocalResourceVisibility.APPLICATION。这是自动生成参数，不建议手动在配置文件中设置。
   */
  String JOBJAR_VISIBILITY = "mapreduce.job.jobjar.visibility";
  boolean JOBJAR_VISIBILITY_DEFAULT = false;

  /**
   * 这是自动生成参数，不建议手动在配置文件中设置。
   */
  String JOBJAR_SHARED_CACHE_UPLOAD_POLICY =
      "mapreduce.job.jobjar.sharedcache.uploadpolicy";
  boolean JOBJAR_SHARED_CACHE_UPLOAD_POLICY_DEFAULT = false;

  /**
   * 这是自动生成参数，不建议手动在配置文件中设置。
   */
  String CACHE_FILES_SHARED_CACHE_UPLOAD_POLICIES =
      "mapreduce.job.cache.files.sharedcache.uploadpolicies";

  /**
   * 这是自动生成参数，不建议手动在配置文件中设置。
   */
  String CACHE_ARCHIVES_SHARED_CACHE_UPLOAD_POLICIES =
      "mapreduce.job.cache.archives.sharedcache.uploadpolicies";

  /**
   * 逗号分隔的本MapReduce作业所需文件资源列表。如果启用了files资源类型，
   * 这些资源应使用共享缓存或上传到共享缓存。该参数可通过MapReduce Job API编程修改。
   */
  String FILES_FOR_SHARED_CACHE = "mapreduce.job.cache.sharedcache.files";

  /**
   * 逗号分隔的本MapReduce作业所需libjar资源列表。如果启用了libjars资源类型，
   * 这些资源应使用共享缓存或上传到共享缓存。这些资源也会添加到本作业所有任务的classpath中。
   * 该参数可通过MapReduce Job API编程修改。
   */
  String FILES_FOR_CLASSPATH_AND_SHARED_CACHE =
      "mapreduce.job.cache.sharedcache.files.addtoclasspath";

  /**
   * 逗号分隔的本MapReduce作业所需归档资源列表。如果启用了archives资源类型，
   * 这些资源应使用共享缓存或上传到共享缓存。该参数可通过MapReduce Job API编程修改。
   */
  String ARCHIVES_FOR_SHARED_CACHE =
      "mapreduce.job.cache.sharedcache.archives";

  /**
   * 逗号分隔的共享缓存启用资源类别列表。如果某个类别启用，该类别下的资源会
   * 上传到共享缓存。合法类别包括：jobjar、libjars、files、archives。
   * 如果指定"disabled"则禁用所有类别，如果指定"enabled"则启用所有类别。
   */
  String SHARED_CACHE_MODE = "mapreduce.job.sharedcache.mode";

  String SHARED_CACHE_MODE_DEFAULT = "disabled";

  /**
   * @deprecated 符号链接始终启用，无法禁用。
   */
  @Deprecated
  public static final String CACHE_SYMLINK = "mapreduce.job.cache.symlink.create";

  public static final String MAPREDUCE_JOB_USER_CLASSPATH_FIRST = "mapreduce.job.user.classpath.first";

  public static final String MAPREDUCE_JOB_CLASSLOADER = "mapreduce.job.classloader";

  /**
   * 逗号分隔的ShuffleProvider辅助服务列表（除了内置ShuffleHandler之外）
   * 这些服务可以处理reduce任务的shuffle请求。
   */
  public static final String MAPREDUCE_JOB_SHUFFLE_PROVIDER_SERVICES = "mapreduce.job.shuffle.provider.services";

  public static final String MAPREDUCE_JOB_CLASSLOADER_SYSTEM_CLASSES = "mapreduce.job.classloader.system.classes";

  public static final String MAPREDUCE_JVM_SYSTEM_PROPERTIES_TO_LOG = "mapreduce.jvm.system-properties-to-log";
  public static final String DEFAULT_MAPREDUCE_JVM_SYSTEM_PROPERTIES_TO_LOG =
    "os.name,os.version,java.home,java.runtime.version,java.vendor," +
    "java.version,java.vm.name,java.class.path,java.io.tmpdir,user.dir,user.name";

  /*
   * 标记是否无论用户指定的Java选项如何，都需要为MR AM和Map/Reduce容器
   * 添加JDK17必需的add-opens参数。
   */
  public static final String MAPREDUCE_JVM_ADD_OPENS_JAVA_OPT =
    "mapreduce.jvm.add-opens-as-default";

  public static final boolean MAPREDUCE_JVM_ADD_OPENS_JAVA_OPT_DEFAULT = false;

  public static final String IO_SORT_FACTOR = "mapreduce.task.io.sort.factor";

  public static final int DEFAULT_IO_SORT_FACTOR = 10;

  public static final String IO_SORT_MB = "mapreduce.task.io.sort.mb";

  public static final int DEFAULT_IO_SORT_MB = 100;

  public static final String INDEX_CACHE_MEMORY_LIMIT = "mapreduce.task.index.cache.limit.bytes";
  String SPILL_FILES_COUNT_LIMIT = "mapreduce.task.spill.files.count.limit";

  public static final String PRESERVE_FAILED_TASK_FILES = "mapreduce.task.files.preserve.failedtasks";

  public static final String PRESERVE_FILES_PATTERN = "mapreduce.task.files.preserve.filepattern";

  public static final String TASK_DEBUGOUT_LINES = "mapreduce.task.debugout.lines";

  public static final String RECORDS_BEFORE_PROGRESS = "mapreduce.task.merge.progress.records";

  public static final String SKIP_START_ATTEMPTS = "mapreduce.task.skip.start.attempts";

  public static final String TASK_ATTEMPT_ID = "mapreduce.task.attempt.id";

  public static final String TASK_ISMAP = "mapreduce.task.ismap";
  public static final boolean DEFAULT_TASK_ISMAP = true;

  public static final String TASK_PARTITION = "mapreduce.task.partition";

  public static final String TASK_PROFILE = "mapreduce.task.profile";

  public static final String TASK_PROFILE_PARAMS = "mapreduce.task.profile.params";

  public static final String DEFAULT_TASK_PROFILE_PARAMS =
      "-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,"
          + "verbose=n,file=%s";

  public static final String NUM_MAP_PROFILES = "mapreduce.task.profile.maps";

  public static final String NUM_REDUCE_PROFILES = "mapreduce.task.profile.reduces";

  public static final String TASK_MAP_PROFILE_PARAMS = "mapreduce.task.profile.map.params";
  
  public static final String TASK_REDUCE