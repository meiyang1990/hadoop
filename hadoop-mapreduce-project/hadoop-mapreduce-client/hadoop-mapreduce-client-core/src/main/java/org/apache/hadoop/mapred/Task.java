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

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.SecretKey;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;
import org.apache.hadoop.mapreduce.util.MRJobConfUtil;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明: MapReduce任务抽象基类，定义了Map任务和Reduce任务共享的通用属性和方法，
 * 是MapReduce计算模型中任务执行框架的核心基础类，负责任务生命周期管理、进度汇报、计数器更新等通用逻辑。
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
abstract public class Task implements Writable, Configurable {
  private static final Logger LOG =
      LoggerFactory.getLogger(Task.class);

  public static String MERGED_OUTPUT_PREFIX = ".merged";
  public static final long DEFAULT_COMBINE_RECORDS_BEFORE_PROGRESS = 10000;
  private static final String HDFS_URI_SCHEME = "hdfs";
  
  /**
   * @deprecated Provided for compatibility. Use {@link TaskCounter} instead.
   */
  @Deprecated
  public enum Counter {
    MAP_INPUT_RECORDS, 
    MAP_OUTPUT_RECORDS,
    MAP_SKIPPED_RECORDS,
    MAP_INPUT_BYTES, 
    MAP_OUTPUT_BYTES,
    MAP_OUTPUT_MATERIALIZED_BYTES,
    COMBINE_INPUT_RECORDS,
    COMBINE_OUTPUT_RECORDS,
    REDUCE_INPUT_GROUPS,
    REDUCE_SHUFFLE_BYTES,
    REDUCE_INPUT_RECORDS,
    REDUCE_OUTPUT_RECORDS,
    REDUCE_SKIPPED_GROUPS,
    REDUCE_SKIPPED_RECORDS,
    SPILLED_RECORDS,
    SPLIT_RAW_BYTES,
    CPU_MILLISECONDS,
    PHYSICAL_MEMORY_BYTES,
    VIRTUAL_MEMORY_BYTES,
    COMMITTED_HEAP_BYTES,
    MAP_PHYSICAL_MEMORY_BYTES_MAX,
    MAP_VIRTUAL_MEMORY_BYTES_MAX,
    REDUCE_PHYSICAL_MEMORY_BYTES_MAX,
    REDUCE_VIRTUAL_MEMORY_BYTES_MAX
  }

  /**
   * 根据URI scheme获取文件系统计数器名称数组，第一个元素是BYTES_READ计数器名称，第二个是BYTES_WRITTEN计数器名称
   * @param uriScheme 文件系统URI scheme
   * @return 包含两个计数器名称的字符串数组
   */
  protected static String[] getFileSystemCounterNames(String uriScheme) {
    String scheme = StringUtils.toUpperCase(uriScheme);
    return new String[]{scheme+"_BYTES_READ", scheme+"_BYTES_WRITTEN"};
  }
  
  /**
   * 文件系统计数器组名称
   */
  protected static final String FILESYSTEM_COUNTER_GROUP = "FileSystemCounters";

  ///////////////////////////////////////////////////////////
  // Helper methods to construct task-output paths
  ///////////////////////////////////////////////////////////
  
  /** Construct output file names so that, when an output directory listing is
   * sorted lexicographically, positions correspond to output partitions.*/
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  /**
   * 根据分区编号生成格式化的输出文件名，保证字典序与分区编号顺序一致
   * @param partition 分区编号
   * @return 格式化后的输出文件名，格式为 part-xxxxx
   */
  static synchronized String getOutputName(int partition) {
    return "part-" + NUMBER_FORMAT.format(partition);
  }

  ////////////////////////////////////////////
  // Fields
  ////////////////////////////////////////////

  private String jobFile;                         // job configuration file
  private String user;                            // user running the job
  private TaskAttemptID taskId;                   // unique, includes job id
  private int partition;                          // id within job
  private byte[] encryptedSpillKey = new byte[] {0};  // Key Used to encrypt
  // intermediate spills
  TaskStatus taskStatus;                          // current status of the task
  protected JobStatus.State jobRunStateForCleanup;
  protected boolean jobCleanup = false;
  protected boolean jobSetup = false;
  protected boolean taskCleanup = false;
 
  // An opaque data field used to attach extra data to each task. This is used
  // by the Hadoop scheduler for Mesos to associate a Mesos task ID with each
  // task and recover these IDs on the TaskTracker.
  protected BytesWritable extraData = new BytesWritable(); 
  
  //skip ranges based on failed ranges from previous attempts
  private SortedRanges skipRanges = new SortedRanges();
  private boolean skipping = false;
  private boolean writeSkipRecs = true;
  
  //currently processing record start index
  private volatile long currentRecStartIndex; 
  private Iterator<Long> currentRecIndexIterator = 
    skipRanges.skipRangeIterator();

  private ResourceCalculatorProcessTree pTree;
  private long initCpuCumulativeTime = ResourceCalculatorProcessTree.UNAVAILABLE;

  protected JobConf conf;
  protected MapOutputFile mapOutputFile;
  protected LocalDirAllocator lDirAlloc;
  private final static int MAX_RETRIES = 10;
  protected JobContext jobContext;
  protected TaskAttemptContext taskContext;
  protected org.apache.hadoop.mapreduce.OutputFormat<?,?> outputFormat;
  protected org.apache.hadoop.mapreduce.OutputCommitter committer;
  protected final Counters.Counter spilledRecordsCounter;
  protected final Counters.Counter failedShuffleCounter;
  protected final Counters.Counter mergedMapOutputsCounter;
  private int numSlotsRequired;
  protected TaskUmbilicalProtocol umbilical;
  protected SecretKey tokenSecret;
  protected SecretKey shuffleSecret;
  protected GcTimeUpdater gcUpdater;
  final AtomicBoolean mustPreempt = new AtomicBoolean(false);
  private boolean uberized = false;

  ////////////////////////////////////////////
  // Constructors
  ////////////////////////////////////////////

  /**
   * 空构造函数，初始化任务状态和基础计数器
   */
  public Task() {
    taskStatus = TaskStatus.createTaskStatus(isMapTask());
    taskId = new TaskAttemptID();
    spilledRecordsCounter = 
      counters.findCounter(TaskCounter.SPILLED_RECORDS);
    failedShuffleCounter = 
      counters.findCounter(TaskCounter.FAILED_SHUFFLE);
    mergedMapOutputsCounter = 
      counters.findCounter(TaskCounter.MERGED_MAP_OUTPUTS);
    gcUpdater = new GcTimeUpdater();
  }

  /**
   * 构造函数，初始化任务核心属性
   * @param jobFile 作业配置文件路径
   * @param taskId 任务尝试ID
   * @param partition 任务分区编号
   * @param numSlotsRequired 任务需要的资源槽数量
   */
  public Task(String jobFile, TaskAttemptID taskId, int partition, 
              int numSlotsRequired) {
    this.jobFile = jobFile;
    this.taskId = taskId;
     
    this.partition = partition;
    this.numSlotsRequired = numSlotsRequired;
    this.taskStatus = TaskStatus.createTaskStatus(isMapTask(), this.taskId, 
                                                  0.0f, numSlotsRequired, 
                                                  TaskStatus.State.UNASSIGNED, 
                                                  "", "", "", 
                                                  isMapTask() ? 
                                                    TaskStatus.Phase.MAP : 
                                                    TaskStatus.Phase.SHUFFLE, 
                                                  counters);
    spilledRecordsCounter = counters.findCounter(TaskCounter.SPILLED_RECORDS);
    failedShuffleCounter = counters.findCounter(TaskCounter.FAILED_SHUFFLE);
    mergedMapOutputsCounter = 
      counters.findCounter(TaskCounter.MERGED_MAP_OUTPUTS);
    gcUpdater = new GcTimeUpdater();
  }

  @VisibleForTesting
  void setTaskDone() {
    taskDone.set(true);
  }

  ////////////////////////////////////////////
  // Accessors
  ////////////////////////////////////////////
  public void setJobFile(String jobFile) { this.jobFile = jobFile; }
  public String getJobFile() { return jobFile; }
  public TaskAttemptID getTaskID() { return taskId; }
  public int getNumSlotsRequired() {
    return numSlotsRequired;
  }

  Counters getCounters() { return counters; }
  
  /**
   * 获取当前任务所属作业ID
   * @return 作业ID
   */
  public JobID getJobID() {
    return taskId.getJobID();
  }

  /**
   * 设置作业令牌密钥，用于身份认证
   * @param tokenSecret 作业令牌密钥
   */
  public void setJobTokenSecret(SecretKey tokenSecret) {
    this.tokenSecret = tokenSecret;
  }

  /**
   * 获取加密Spill文件使用的密钥
   * @return 加密Spill密钥
   */
  public byte[] getEncryptedSpillKey() {
    return encryptedSpillKey;
  }

  /**
   * 设置加密Spill文件使用的密钥
   * @param encryptedSpillKey 加密Spill密钥
   */
  public void setEncryptedSpillKey(byte[] encryptedSpillKey) {
    if (encryptedSpillKey != null) {
      this.encryptedSpillKey = encryptedSpillKey;
    }
  }

  /**
   * 获取作业令牌密钥
   * @return 作业令牌密钥
   */
  public SecretKey getJobTokenSecret() {
    return this.tokenSecret;
  }

  /**
   * 设置Shuffle阶段认证使用的密钥
   * @param shuffleSecret Shuffle认证密钥
   */
  public void setShuffleSecret(SecretKey shuffleSecret) {
    this.shuffleSecret = shuffleSecret;
  }

  /**
   * 获取Shuffle阶段认证使用的密钥
   * @return Shuffle认证密钥
   */
  public SecretKey getShuffleSecret() {
    return this.shuffleSecret;
  }

  /**
   * 获取当前任务在作业中的分区编号
   * @return 分区编号
   */
  public int getPartition() {
    return partition;
  }
  /**
   * 获取任务当前执行阶段，同步方法保证线程安全
   * @return 当前执行阶段
   */
  public synchronized TaskStatus.Phase getPhase(){
    return this.taskStatus.getPhase(); 
  }
  /**
   * 设置任务当前执行阶段，同步方法保证线程安全
   * @param phase 执行阶段
   */
  protected synchronized void setPhase(TaskStatus.Phase phase){
    this.taskStatus.setPhase(phase); 
  }
  
  /**
   * 获取是否需要写入跳过的记录
   * @return 是否写入跳过记录
   */
  protected boolean toWriteSkipRecs() {
    return writeSkipRecs;
  }
      
  /**
   * 设置是否需要写入跳过的记录
   * @param writeSkipRecs 是否写入跳过记录
   */
  protected void setWriteSkipRecs(boolean writeSkipRecs) {
    this.writeSkipRecs = writeSkipRecs;
  }
  
  /**
   * 向父TaskTracker报告致命错误，终止任务执行
   * @param id 任务尝试ID
   * @param throwable 异常对象
   * @param logMsg 日志消息
   * @param fastFail 是否快速失败
   */
  protected void reportFatalError(TaskAttemptID id, Throwable throwable, 
                                  String logMsg, boolean fastFail) {
    LOG.error(logMsg);
    
    if (ShutdownHookManager.get().isShutdownInProgress()) {
      return;
    }
    
    Throwable tCause = throwable.getCause();
    String cause = tCause == null 
                   ? StringUtils.stringifyException(throwable)
                   : StringUtils.stringifyException(tCause);
    try {
      umbilical.fatalError(id, cause, fastFail);
    } catch (IOException ioe) {
      LOG.error("Failed to contact the tasktracker", ioe);
      System.exit(-1);
    }
  }

  /**
   * 根据路径获取对应文件系统scheme的统计信息列表
   * @param path 目标路径
   * @param conf 配置对象
   * @return 匹配到的文件系统统计信息列表
   * @throws IOException IO异常
   */
  protected static List<Statistics> getFsStatistics(Path path, Configuration conf) throws IOException {
    List<Statistics> matchedStats = new ArrayList<FileSystem.Statistics>();
    path = path.getFileSystem(conf).makeQualified(path);
    String scheme = path.toUri().getScheme();
    for (Statistics stats : FileSystem.getAllStatistics()) {
      if (stats.getScheme().equals(scheme)) {
        matchedStats.add(stats);
      }
    }
    return matchedStats;
  }

  /**
   * 获取需要跳过的记录范围
   * @return 跳过范围对象
   */
  public SortedRanges getSkipRanges() {
    return skipRanges;
  }

  /**
   * 设置需要跳过的记录范围
   * @param skipRanges 跳过范围对象
   */
  public void setSkipRanges(SortedRanges skipRanges) {
    this.skipRanges = skipRanges;
  }

  /**
   * 判断任务是否处于跳过错误记录模式
   * @return 是否跳过模式
   */
  public boolean isSkipping() {
    return skipping;
  }

  /**
   * 设置任务是否开启跳过错误记录模式
   * @param skipping 是否跳过模式
   */
  public void setSkipping(boolean skipping) {
    this.skipping = skipping;
  }

  /**
   * 获取任务当前运行状态，同步方法保证线程安全
   * @return 任务运行状态
   */
  synchronized TaskStatus.State getState(){
    return this.taskStatus.getRunState(); 
  }
  /**
   * 设置任务当前运行状态，同步方法保证线程安全
   * @param state 任务运行状态
   */
  synchronized void setState(TaskStatus.State state){
    this.taskStatus.setRunState(state); 
  }

  void setTaskCleanupTask() {
    taskCleanup = true;
  }
	   
  boolean isTaskCleanupTask() {
    return taskCleanup;
  }

  boolean isJobCleanupTask() {
    return jobCleanup;
  }

  boolean isJobAbortTask() {
    // the task is an abort task if its marked for cleanup and the final 
    // expected state is either failed or killed.
    return isJobCleanupTask() 
           && (jobRunStateForCleanup == JobStatus.State.KILLED 
               || jobRunStateForCleanup == JobStatus.State.FAILED);
  }
  
  boolean isJobSetupTask() {