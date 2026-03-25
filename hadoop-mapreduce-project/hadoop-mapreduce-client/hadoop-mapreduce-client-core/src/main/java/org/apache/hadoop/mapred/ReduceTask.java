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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.SortedRanges.SkipRangeIterator;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter;
import org.apache.hadoop.mapreduce.task.reduce.Shuffle;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce Reduce任务实现类，负责执行Reduce阶段的核心逻辑：
 * 包括混洗（Shuffle）拉取Map输出、排序分组、调用用户Reduce函数处理并输出结果
 * 支持新旧两种MapReduce API，支持跳过错误记录、本地作业运行等特性
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ReduceTask extends Task {

  static {                                        // register a ctor
    WritableFactories.setFactory
      (ReduceTask.class,
       new WritableFactory() {
         public Writable newInstance() { return new ReduceTask(); }
       });
  }
  
  private static final Logger LOG =
      LoggerFactory.getLogger(ReduceTask.class.getName());
  private int numMaps;

  private CompressionCodec codec;

  // If this is a LocalJobRunner-based job, this will
  // be a mapping from map task attempts to their output files.
  // This will be null in other cases.
  private Map<TaskAttemptID, MapOutputFile> localMapFiles;

  { 
    getProgress().setStatus("reduce"); 
    setPhase(TaskStatus.Phase.SHUFFLE);        // phase to start with 
  }

  private Progress copyPhase;
  private Progress sortPhase;
  private Progress reducePhase;
  private Counters.Counter shuffledMapsCounter = 
    getCounters().findCounter(TaskCounter.SHUFFLED_MAPS);
  private Counters.Counter reduceShuffleBytes = 
    getCounters().findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES);
  private Counters.Counter reduceInputKeyCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_INPUT_GROUPS);
  private Counters.Counter reduceInputValueCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_INPUT_RECORDS);
  private Counters.Counter reduceOutputCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS);
  private Counters.Counter reduceCombineInputCounter =
    getCounters().findCounter(TaskCounter.COMBINE_INPUT_RECORDS);
  private Counters.Counter reduceCombineOutputCounter =
    getCounters().findCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);
  private Counters.Counter fileOutputByteCounter =
    getCounters().findCounter(FileOutputFormatCounter.BYTES_WRITTEN);

  // A custom comparator for map output files. Here the ordering is determined
  // by the file's size and path. In case of files with same size and different
  // file paths, the first parameter is considered smaller than the second one.
  // In case of files with same size and path are considered equal.
  private Comparator<FileStatus> mapOutputFileComparator = 
      new Comparator<FileStatus>() {
    public int compare(FileStatus a, FileStatus b) {
      if (a.getLen() < b.getLen())
        return -1;
      else if (a.getLen() == b.getLen())
        if (a.getPath().toString().equals(b.getPath().toString()))
          return 0;
        else
          return -1; 
      else
        return 1;
    }
  };

  // A sorted set for keeping a set of map output files on disk
  private final SortedSet<FileStatus> mapOutputFilesOnDisk = 
      new TreeSet<FileStatus>(mapOutputFileComparator);
  
  /**
   * 空构造函数，用于Writable反序列化
   */
  public ReduceTask() {
    super();
  }

  /**
   * 构造Reduce任务实例
   * @param jobFile 作业配置文件路径
   * @param taskId 任务尝试ID
   * @param partition Reduce分区编号
   * @param numMaps 该作业对应的Map任务总数
   * @param numSlotsRequired 需要占用的资源槽位数
   */
  public ReduceTask(String jobFile, TaskAttemptID taskId,
                    int partition, int numMaps, int numSlotsRequired) {
    super(jobFile, taskId, partition, numSlotsRequired);
    this.numMaps = numMaps;
  }
  

  /**
   * 为LocalJobRunner本地运行模式设置本地Map输出文件位置映射，
   * 让Reduce任务直接从本地文件拉取Map输出，不需要网络传输
   * @param mapFiles Map任务尝试ID到输出文件的映射
   */
  public void setLocalMapFiles(Map<TaskAttemptID, MapOutputFile> mapFiles) {
    this.localMapFiles = mapFiles;
  }

  /**
   * 初始化Map输出压缩编解码器，如果作业启用了Map输出压缩则创建对应实例
   * @return 压缩编解码器实例，未启用压缩则返回null
   */
  private CompressionCodec initCodec() {
    // check if map-outputs are to be compressed
    if (conf.getCompressMapOutput()) {
      Class<? extends CompressionCodec> codecClass =
        conf.getMapOutputCompressorClass(DefaultCodec.class);
      return ReflectionUtils.newInstance(codecClass, conf);
    } 

    return null;
  }

  @Override
  /**
   * 判断当前任务是否为Map任务，Reduce任务固定返回false
   */
  public boolean isMapTask() {
    return false;
  }

  public int getNumMaps() { return numMaps; }
  
  /**
   * 本地化当前Reduce任务的配置，写入Map任务总数到配置中
   * @param conf 任务配置对象
   */
  @Override
  public void localizeConfiguration(JobConf conf) throws IOException {
    super.localizeConfiguration(conf);
    conf.setNumMapTasks(numMaps);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeInt(numMaps);                        // 写入Map任务总数
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    numMaps = in.readInt();
  }
  
  // Get the input files for the reducer (for local jobs).
  private Path[] getMapFiles(FileSystem fs) throws IOException {
    List<Path> fileList = new ArrayList<Path>();
    for(int i = 0; i < numMaps; ++i) {
      fileList.add(mapOutputFile.getInputFile(i));
    }
    return fileList.toArray(new Path[0]);
  }

  /**
   * Reduce阶段值迭代器实现，统计输入记录数并更新Reduce阶段进度
   * @param <KEY> 键类型
   * @param <VALUE> 值类型
   */
  private class ReduceValuesIterator<KEY,VALUE> 
          extends ValuesIterator<KEY,VALUE> {
    public ReduceValuesIterator (RawKeyValueIterator in,
                                 RawComparator<KEY> comparator, 
                                 Class<KEY> keyClass,
                                 Class<VALUE> valClass,
                                 Configuration conf, Progressable reporter)
      throws IOException {
      super(in, comparator, keyClass, valClass, conf, reporter);
    }

    @Override
    public VALUE next() {
      reduceInputValueCounter.increment(1);
      return moveToNext();
    }
    
    protected VALUE moveToNext() {
      return super.next();
    }
    
    /**
     * 更新Reduce阶段进度并通知任务报告器
     */
    public void informReduceProgress() {
      reducePhase.set(super.in.getProgress().getProgress()); // update progress
      reporter.progress();
    }
  }

  /**
   * 支持跳过错误分组的Reduce值迭代器，用于错误容忍模式，
   * 根据配置跳过出问题的分组，避免整个Reduce任务失败
   * @param <KEY> 键类型
   * @param <VALUE> 值类型
   */
  private class SkippingReduceValuesIterator<KEY,VALUE> 
     extends ReduceValuesIterator<KEY,VALUE> {
     private SkipRangeIterator skipIt;
     private TaskUmbilicalProtocol umbilical;
     private Counters.Counter skipGroupCounter;
     private Counters.Counter skipRecCounter;
     private long grpIndex = -1;
     private Class<KEY> keyClass;
     private Class<VALUE> valClass;
     private SequenceFile.Writer skipWriter;
     private boolean toWriteSkipRecs;
     private boolean hasNext;
     private TaskReporter reporter;
     
     public SkippingReduceValuesIterator(RawKeyValueIterator in,
         RawComparator<KEY> comparator, Class<KEY> keyClass,
         Class<VALUE> valClass, Configuration conf, TaskReporter reporter,
         TaskUmbilicalProtocol umbilical) throws IOException {
       super(in, comparator, keyClass, valClass, conf, reporter);
       this.umbilical = umbilical;
       this.skipGroupCounter = 
         reporter.getCounter(TaskCounter.REDUCE_SKIPPED_GROUPS);
       this.skipRecCounter = 
         reporter.getCounter(TaskCounter.REDUCE_SKIPPED_RECORDS);
       this.toWriteSkipRecs = toWriteSkipRecs() &&  
         SkipBadRecords.getSkipOutputPath(conf)!=null;
       this.keyClass = keyClass;
       this.valClass = valClass;
       this.reporter = reporter;
       skipIt = getSkipRanges().skipRangeIterator();
       mayBeSkip();
     }
     
     public void nextKey() throws IOException {
       super.nextKey();
       mayBeSkip();
     }
     
     public boolean more() { 
       return super.more() && hasNext; 
     }
     
     /**
      * 根据跳过范围检查并跳过需要跳过的分组，统计跳过数量
      * @throws IOException IO异常
      */
     private void mayBeSkip() throws IOException {
       hasNext = skipIt.hasNext();
       if(!hasNext) {
         LOG.warn("Further groups got skipped.");
         return;
       }
       grpIndex++;
       long nextGrpIndex = skipIt.next();
       long skip = 0;
       long skipRec = 0;
       while(grpIndex<nextGrpIndex && super.more()) {
         while (hasNext()) {
           VALUE value = moveToNext();
           if(toWriteSkipRecs) {
             writeSkippedRec(getKey(), value);
           }
           skipRec++;
         }
         super.nextKey();
         grpIndex++;
         skip++;
       }
       
       // 所有需要跳过的范围处理完成后关闭跳过输出文件
       if(skip>0 && skipIt.skippedAllRanges() && skipWriter!=null) {
         skipWriter.close();
       }
       skipGroupCounter.increment(skip);
       skipRecCounter.increment(skipRec);
       reportNextRecordRange(umbilical, grpIndex);
     }
     
     @SuppressWarnings("unchecked")
     /**
      * 将跳过的记录写入跳过输出文件，便于后续分析
      * @param key 跳过记录的键
      * @param value 跳过记录的值
      * @throws IOException IO异常
      */
     private void writeSkippedRec(KEY key, VALUE value) throws IOException{
       if(skipWriter==null) {
         Path skipDir = SkipBadRecords.getSkipOutputPath(conf);
         Path skipFile = new Path(skipDir, getTaskID().toString());
         skipWriter = SequenceFile.createWriter(
               skipFile.getFileSystem(conf), conf, skipFile,
               keyClass, valClass, 
               CompressionType.BLOCK, reporter);
       }
       skipWriter.append(key, value);
     }
  }

  @Override
  @SuppressWarnings("unchecked")
  /**
   * Reduce任务主执行方法，完成整个Reduce阶段流程：混洗拉取、排序、用户reduce执行、输出
   * @param job 任务配置
   * @param umbilical 与ApplicationMaster通信的协议对象
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   * @throws ClassNotFoundException 类找不到异常
   */
  public void run(JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException, InterruptedException, ClassNotFoundException {
    job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());

    if (isMapOrReduce()) {
      // 注册三个执行阶段的进度对象
      copyPhase = getProgress().addPhase("copy");
      sortPhase  = getProgress().addPhase("sort");
      reducePhase = getProgress().addPhase("reduce");
    }
    // 启动与父服务通信的进度报告线程
    TaskReporter reporter = startReporter(umbilical);
    
    boolean useNewApi = job.getUseNewReducer();
    initialize(job, getJobID(), reporter, useNewApi);

    // 检查是否是作业清理任务，直接执行清理后退出
    if (jobCleanup) {
      runJobCleanupTask(umbilical, reporter);
      return;
    }
    // 检查是否是作业初始化任务，直接执行初始化后退出
    if (jobSetup) {
      runJobSetupTask(umbilical, reporter);
      return;
    }
    // 检查是否是任务清理任务，直接执行清理后退出
    if (taskCleanup) {
      runTaskCleanupTask(umbilical, reporter);
      return;
    }
    
    // 初始化压缩编解码器
    codec = initCodec();
    RawKeyValueIterator rIter = null;
    ShuffleConsumerPlugin shuffleConsumerPlugin = null;
    
    // 如果配置了Combiner，创建Combiner输出收集器
    Class combinerClass = conf.getCombinerClass();
    CombineOutputCollector combineCollector = 
      (null != combinerClass) ? 
     new CombineOutputCollector(reduceCombineOutputCounter, reporter, conf) : null;

    // 加载配置的Shuffle消费者插件类
    Class<? extends ShuffleConsumerPlugin> clazz =
          job.getClass(MRConfig.SHUFFLE_CONSUMER_PLUGIN, Shuffle.class, ShuffleConsumerPlugin.class);
					
    // 创建Shuffle消费者插件实例
    shuffleConsumerPlugin = ReflectionUtils.newInstance(clazz, job);
    LOG.info("Using ShuffleConsumerPlugin: " + shuffleConsumerPlugin);

    // 构造Shuffle消费者上下文，初始化插件
    ShuffleConsumerPlugin.Context shuffleContext = 
      new ShuffleConsumerPlugin.Context(getTaskID(), job, FileSystem.getLocal(job), umbilical, 
                  super.lDirAlloc, reporter, codec, 
                  combinerClass, combineCollector, 
                  spilledRecordsCounter, reduceCombineInputCounter,
                  shuffledMapsCounter,
                  reduceShuffleBytes, failedShuffleCounter,
                  mergedMapOutputsCounter,
                  taskStatus, copyPhase, sortPhase, this,
                  mapOutputFile, localMapFiles);
    shuffleConsumerPlugin.init(shuffleContext);

    // 执行Shuffle拉取和合并排序，得到排序后的键值对迭代器
    rIter = shuffleConsumerPlugin.run();

    // 清理磁盘上的Map输出文件引用
    mapOutputFilesOnDisk.clear();
    
    // 排序阶段完成，切换到Reduce阶段，更新任务状态到ApplicationMaster
    sortPhase.complete();
    setPhase(TaskStatus.Phase.REDUCE); 
    statusUpdate(umbilical);
    Class keyClass = job.getMapOutputKeyClass();
    Class valueClass = job.getMapOutputValueClass();
    RawComparator comparator = job.getOutputValueGroupingComparator();

    // 根据API版本调用对应Reducer执行逻辑
    if (useNewApi) {
      runNewReducer(job, um