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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.Merger;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.Task.CombineOutputCollector;
import org.apache.hadoop.mapred.Task.CombineValuesIterator;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.security.IntermediateEncryptedStream;
import org.apache.hadoop.mapreduce.task.reduce.MapOutput.MapOutputComparator;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Reduce阶段合并管理器实现类，负责管理Map输出结果的合并流程，
 * 支持内存到内存、内存到磁盘、磁盘到磁盘多阶段合并，合理控制内存使用，
 * 最终生成供Reduce任务消费的有序键值对迭代器。
 */
@SuppressWarnings(value={"unchecked"})
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class MergeManagerImpl<K, V> implements MergeManager<K, V> {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(MergeManagerImpl.class);
  
  /* Maximum percentage of the in-memory limit that a single shuffle can 
   * consume*/ 
  private static final float DEFAULT_SHUFFLE_MEMORY_LIMIT_PERCENT
    = 0.25f;

  private final TaskAttemptID reduceId;
  
  private final JobConf jobConf;
  private final FileSystem localFS;
  private final FileSystem rfs;
  private final LocalDirAllocator localDirAllocator;
  
  protected MapOutputFile mapOutputFile;
  
  // 存储已经过内存合并后的内存Map输出结果
  Set<InMemoryMapOutput<K, V>> inMemoryMergedMapOutputs = 
    new TreeSet<InMemoryMapOutput<K,V>>(new MapOutputComparator<K, V>());
  private IntermediateMemoryToMemoryMerger memToMemMerger;

  // 存储刚拉取完成、尚未合并的内存Map输出结果
  Set<InMemoryMapOutput<K, V>> inMemoryMapOutputs = 
    new TreeSet<InMemoryMapOutput<K,V>>(new MapOutputComparator<K, V>());
  private final MergeThread<InMemoryMapOutput<K,V>, K,V> inMemoryMerger;
  
  // 存储已经溢出到磁盘的Map输出结果
  Set<CompressAwarePath> onDiskMapOutputs = new TreeSet<CompressAwarePath>();
  private final OnDiskMerger onDiskMerger;

  @VisibleForTesting
  final long memoryLimit;

  private long usedMemory;
  private long commitMemory;

  @VisibleForTesting
  final long maxSingleShuffleLimit;
  
  private final int memToMemMergeOutputsThreshold; 
  private final long mergeThreshold;
  
  private final int ioSortFactor;

  private final Reporter reporter;
  private final ExceptionReporter exceptionReporter;
  
  /**
   * Combiner class to run during in-memory merge, if defined.
   */
  private final Class<? extends Reducer> combinerClass;

  /**
   * Resettable collector used for combine.
   */
  private final CombineOutputCollector<K,V> combineCollector;

  private final Counters.Counter spilledRecordsCounter;

  private final Counters.Counter reduceCombineInputCounter;

  private final Counters.Counter mergedMapOutputsCounter;
  
  private final CompressionCodec codec;
  
  private final Progress mergePhase;

  /**
   * 构造合并管理器，初始化内存限制、合并线程和各类参数
   * @param reduceId 当前Reduce任务尝试ID
   * @param jobConf 作业配置
   * @param localFS 本地文件系统
   * @param localDirAllocator 本地目录分配器
   * @param reporter 任务报告器
   * @param codec 压缩编解码器
   * @param combinerClass Combiner类
   * @param combineCollector Combiner输出收集器
   * @param spilledRecordsCounter 溢出记录计数器
   * @param reduceCombineInputCounter Combiner输入记录计数器
   * @param mergedMapOutputsCounter 合并Map输出计数器
   * @param exceptionReporter 异常报告器
   * @param mergePhase 合并阶段进度
   * @param mapOutputFile Map输出文件管理对象
   */
  public MergeManagerImpl(TaskAttemptID reduceId, JobConf jobConf, 
                      FileSystem localFS,
                      LocalDirAllocator localDirAllocator,  
                      Reporter reporter,
                      CompressionCodec codec,
                      Class<? extends Reducer> combinerClass,
                      CombineOutputCollector<K,V> combineCollector,
                      Counters.Counter spilledRecordsCounter,
                      Counters.Counter reduceCombineInputCounter,
                      Counters.Counter mergedMapOutputsCounter,
                      ExceptionReporter exceptionReporter,
                      Progress mergePhase, MapOutputFile mapOutputFile) {
    this.reduceId = reduceId;
    this.jobConf = jobConf;
    this.localDirAllocator = localDirAllocator;
    this.exceptionReporter = exceptionReporter;
    
    this.reporter = reporter;
    this.codec = codec;
    this.combinerClass = combinerClass;
    this.combineCollector = combineCollector;
    this.reduceCombineInputCounter = reduceCombineInputCounter;
    this.spilledRecordsCounter = spilledRecordsCounter;
    this.mergedMapOutputsCounter = mergedMapOutputsCounter;
    this.mapOutputFile = mapOutputFile;
    this.mapOutputFile.setConf(jobConf);
    
    this.localFS = localFS;
    this.rfs = ((LocalFileSystem)localFS).getRaw();
    
    final float maxInMemCopyUse =
      jobConf.getFloat(MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT,
          MRJobConfig.DEFAULT_SHUFFLE_INPUT_BUFFER_PERCENT);
    if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
      throw new IllegalArgumentException("Invalid value for " +
          MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT + ": " +
          maxInMemCopyUse);
    }

    // Allow unit tests to fix Runtime memory
    this.memoryLimit = (long)(jobConf.getLong(
        MRJobConfig.REDUCE_MEMORY_TOTAL_BYTES,
        Runtime.getRuntime().maxMemory()) * maxInMemCopyUse);

    this.ioSortFactor = jobConf.getInt(MRJobConfig.IO_SORT_FACTOR,
        MRJobConfig.DEFAULT_IO_SORT_FACTOR);

    final float singleShuffleMemoryLimitPercent =
        jobConf.getFloat(MRJobConfig.SHUFFLE_MEMORY_LIMIT_PERCENT,
            DEFAULT_SHUFFLE_MEMORY_LIMIT_PERCENT);
    if (singleShuffleMemoryLimitPercent < 0.0f
        || singleShuffleMemoryLimitPercent > 1.0f) {
      throw new IllegalArgumentException("Invalid value for "
          + MRJobConfig.SHUFFLE_MEMORY_LIMIT_PERCENT + ": "
          + singleShuffleMemoryLimitPercent);
    }

    usedMemory = 0L;
    commitMemory = 0L;
    long maxSingleShuffleLimitConfiged =
        (long)(memoryLimit * singleShuffleMemoryLimitPercent);
    if(maxSingleShuffleLimitConfiged > Integer.MAX_VALUE) {
      maxSingleShuffleLimitConfiged = Integer.MAX_VALUE;
      LOG.info("The max number of bytes for a single in-memory shuffle cannot" +
          " be larger than Integer.MAX_VALUE. Setting it to Integer.MAX_VALUE");
    }
    this.maxSingleShuffleLimit = maxSingleShuffleLimitConfiged;
    this.memToMemMergeOutputsThreshold =
        jobConf.getInt(MRJobConfig.REDUCE_MEMTOMEM_THRESHOLD, ioSortFactor);
    this.mergeThreshold = (long)(this.memoryLimit * 
                          jobConf.getFloat(
                            MRJobConfig.SHUFFLE_MERGE_PERCENT,
                            MRJobConfig.DEFAULT_SHUFFLE_MERGE_PERCENT));
    LOG.info("MergerManager: memoryLimit=" + memoryLimit + ", " +
             "maxSingleShuffleLimit=" + maxSingleShuffleLimit + ", " +
             "mergeThreshold=" + mergeThreshold + ", " + 
             "ioSortFactor=" + ioSortFactor + ", " +
             "memToMemMergeOutputsThreshold=" + memToMemMergeOutputsThreshold);

    if (this.maxSingleShuffleLimit >= this.mergeThreshold) {
      throw new RuntimeException("Invalid configuration: "
          + "maxSingleShuffleLimit should be less than mergeThreshold "
          + "maxSingleShuffleLimit: " + this.maxSingleShuffleLimit
          + "mergeThreshold: " + this.mergeThreshold);
    }

    boolean allowMemToMemMerge = 
      jobConf.getBoolean(MRJobConfig.REDUCE_MEMTOMEM_ENABLED, false);
    if (allowMemToMemMerge) {
      this.memToMemMerger = 
        new IntermediateMemoryToMemoryMerger(this,
                                             memToMemMergeOutputsThreshold);
      this.memToMemMerger.start();
    } else {
      this.memToMemMerger = null;
    }
    
    this.inMemoryMerger = createInMemoryMerger();
    this.inMemoryMerger.start();
    
    this.onDiskMerger = new OnDiskMerger(this);
    this.onDiskMerger.start();
    
    this.mergePhase = mergePhase;
  }
  
  /**
   * 创建内存合并线程，将内存中多个Map输出合并后溢出到磁盘
   * @return 内存合并线程实例
   */
  protected MergeThread<InMemoryMapOutput<K,V>, K,V> createInMemoryMerger() {
    return new InMemoryMerger(this);
  }

  /**
   * 创建磁盘合并线程，合并多个磁盘上的Map输出文件
   * @return 磁盘合并线程实例
   */
  protected MergeThread<CompressAwarePath,K,V> createOnDiskMerger() {
    return new OnDiskMerger(this);
  }

  /**
   * 获取当前Reduce任务尝试ID
   * @return Reduce任务尝试ID
   */
  TaskAttemptID getReduceId() {
    return reduceId;
  }

  @VisibleForTesting
  ExceptionReporter getExceptionReporter() {
    return exceptionReporter;
  }

  @Override
  public void waitForResource() throws InterruptedException {
    inMemoryMerger.waitForMerge();
  }

  @Override
  public synchronized MapOutput<K,V> reserve(TaskAttemptID mapId, 
                                             long requestedSize,
                                             int fetcher
                                             ) throws IOException {
    // 单个Map输出超过内存限制，直接写到磁盘
    if (requestedSize > maxSingleShuffleLimit) {
      LOG.info(mapId + ": Shuffling to disk since " + requestedSize + 
               " is greater than maxSingleShuffleLimit (" + 
               maxSingleShuffleLimit + ")");
      return new OnDiskMapOutput<K,V>(mapId, this, requestedSize, jobConf,
         fetcher, true, FileSystem.getLocal(jobConf).getRaw(),
         mapOutputFile.getInputFileForWrite(mapId.getTaskID(), requestedSize));
    }
    
    // Stall shuffle if we are above the memory limit

    // It is possible that all threads could just be stalling and not make
    // progress at all. This could happen when:
    //
    // requested size is causing the used memory to go above limit &&
    // requested size < singleShuffleLimit &&
    // current used size < mergeThreshold (merge will not get triggered)
    //
    // To avoid this from happening, we allow exactly one thread to go past
    // the memory limit. We check(usedMemory > memoryLimit) and not
    // (usedMemory + requestedSize > memoryLimit). When this thread is done
    // fetching, this will automatically trigger a merge thereby unlocking
    // all the stalled threads
    
    // 已用内存超过限制，阻塞等待合并释放内存
    if (usedMemory > memoryLimit) {
      LOG.debug(mapId + ": Stalling shuffle since usedMemory (" + usedMemory
          + ") is greater than memoryLimit (" + memoryLimit + ")." + 
          " CommitMemory is (" + commitMemory + ")"); 
      return null;
    }
    
    // 内存足够，直接分配内存空间
    LOG.debug(mapId + ": Proceeding with shuffle since usedMemory ("
        + usedMemory + ") is lesser than memoryLimit (" + memoryLimit + ")."
        + "CommitMemory is (" + commitMemory + ")"); 
    return unconditionalReserve(mapId, requestedSize, true);
  }
  
  /**
   * 不做内存检查，直接分配内存存放Map输出，供内存-内存合并线程使用
   * @param mapId Map任务尝试ID
   * @param requestedSize 请求分配的内存大小
   * @param primaryMapOutput 是否是原始Map输出（非合并输出）
   * @return 内存Map输出对象
   */
  private synchronized InMemoryMapOutput<K, V> unconditionalReserve(
      TaskAttemptID mapId, long requestedSize, boolean primaryMapOutput) {
    usedMemory += requestedSize;
    return new InMemoryMapOutput<K,V>(jobConf, mapId, this, (int)requestedSize,
                                      codec, primaryMapOutput);
  }
  
  /**
   * 释放已分配的内存空间
   * @param size 要释放的内存大小
   */
  synchronized void unreserve(long size) {
    usedMemory -= size;
  }

  /**
   * 处理拉取完成的内存Map输出，触发合并条件满足时启动合并
   * @param mapOutput 已完成拉取的内存Map输出
   */
  public synchronized void closeInMemoryFile(InMemoryMapOutput<K,V> mapOutput) { 
    inMemoryMapOutputs.add(mapOutput);
    LOG.info("closeInMemoryFile -> map-output of size: " + mapOutput.getSize()
        + ", inMemoryMapOutputs.size() -> " + inMemoryMapOutputs.size()
        + ", commitMemory -> " + commitMemory + ", usedMemory ->" + usedMemory);

    commitMemory+= mapOutput.getSize();

    // 已提交内存超过合并阈值，启动内存合并溢出到磁盘
    if (commitMemory >= mergeThreshold) {
      LOG.info("Starting inMemoryMerger's merge since commitMemory=" +
          commitMemory + " > mergeThreshold=" + mergeThreshold + 
          ". Current usedMemory=" + usedMemory);
      inMemoryMapOutputs.addAll(inMemoryMergedMapOutputs);
      inMemoryMergedMapOutputs.clear();
      inMemoryMerger.startMerge(inMemoryMapOutputs);
      commitMemory = 0L;  // Reset commitMemory.
    }
    
    // 启用内存-内存合并且输出数量达到阈值，启动内存-内存合并
    if (memToMemMerger != null) {
      if (inMemoryMapOutputs.size() >= memToMemMergeOutputsThreshold) { 
        memToMemMerger.startMerge(inMemoryMapOutputs);
      }
    }
  }
  
  
  /**
   * 保存内存-内存合并后的输出结果
   * @param mapOutput 合并后的内存输出
   */
  public synchronized void closeInMemoryMergedFile(InMemoryMapOutput<K,V> mapOutput) {
    inMemoryMergedMapOutputs.add(mapOutput);
    LOG.info("closeInMemoryMergedFile -> size: " + mapOutput.getSize() + 
             ", inMemoryMergedMapOutputs.size() -> " + 
             inMemoryMergedMapOutputs.size());
  }
  
  /**
   * 处理溢出到磁盘的Map输出，满足条件时启动磁盘合并
   * @param file 磁盘文件路径（带压缩信息）
   */
  public synchronized void closeOnDiskFile(CompressAwarePath file) {
    onDiskMapOutputs.add(file);
    
    // 磁盘文件