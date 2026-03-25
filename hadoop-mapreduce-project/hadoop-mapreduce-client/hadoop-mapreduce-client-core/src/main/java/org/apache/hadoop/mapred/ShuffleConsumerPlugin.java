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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapred.Task.CombineOutputCollector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 文件说明：MapReduce Reduce任务Shuffle阶段消费插件接口
 * 
 * Shuffle消费插件接口定义，为Reduce任务提供Map输出拉取消费能力
 * 支持从内置ShuffleHandler或第三方辅助服务拉取Map输出文件（MOF）
 */
@InterfaceAudience.LimitedPrivate("mapreduce")
@InterfaceStability.Unstable
public interface ShuffleConsumerPlugin<K, V> {

  /**
   * 初始化插件，传入上下文环境配置
   * @param context 插件运行上下文，包含Reduce任务所有运行参数
   */
  public void init(Context<K, V> context);

  /**
   * 执行Shuffle拉取与合并流程，返回合并后的排序键值对迭代器
   * @return 合并排序后的键值对迭代器，供Reduce任务读取处理
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public RawKeyValueIterator run() throws IOException, InterruptedException;

  /**
   * 关闭插件，清理临时资源
   */
  public void close();

  /**
   * Shuffle消费插件上下文类，保存插件运行所需的全部上下文参数
   * 为插件提供Reduce任务运行环境信息与统计计数器
   */
  @InterfaceAudience.LimitedPrivate("mapreduce")
  @InterfaceStability.Unstable
  public static class Context<K,V> {
    private final org.apache.hadoop.mapreduce.TaskAttemptID reduceId;
    private final JobConf jobConf;
    private final FileSystem localFS;
    private final TaskUmbilicalProtocol umbilical;
    private final LocalDirAllocator localDirAllocator;
    private final Reporter reporter;
    private final CompressionCodec codec;
    private final Class<? extends Reducer> combinerClass;
    private final CombineOutputCollector<K, V> combineCollector;
    private final Counters.Counter spilledRecordsCounter;
    private final Counters.Counter reduceCombineInputCounter;
    private final Counters.Counter shuffledMapsCounter;
    private final Counters.Counter reduceShuffleBytes;
    private final Counters.Counter failedShuffleCounter;
    private final Counters.Counter mergedMapOutputsCounter;
    private final TaskStatus status;
    private final Progress copyPhase;
    private final Progress mergePhase;
    private final Task reduceTask;
    private final MapOutputFile mapOutputFile;
    private final Map<TaskAttemptID, MapOutputFile> localMapFiles;

    /**
     * 构造上下文对象，初始化所有参数
     * @param reduceId Reduce任务尝试ID
     * @param jobConf 作业配置对象
     * @param localFS 本地文件系统
     * @param umbilical 任务与TaskTracker通信协议
     * @param localDirAllocator 本地目录分配器，用于管理临时磁盘空间
     * @param reporter 任务进度报告器
     * @param codec Map输出压缩编解码器
     * @param combinerClass Combiner合并类
     * @param combineCollector Combiner输出收集器
     * @param spilledRecordsCounter 溢写记录计数器
     * @param reduceCombineInputCounter Combiner输入记录计数器
     * @param shuffledMapsCounter 完成Shuffle的Map任务计数器
     * @param reduceShuffleBytes Shuffle拉取字节数计数器
     * @param failedShuffleCounter 失败Shuffle拉取次数计数器
     * @param mergedMapOutputsCounter 合并Map输出数计数器
     * @param status 任务状态对象
     * @param copyPhase 拷贝阶段进度对象
     * @param mergePhase 合并阶段进度对象
     * @param reduceTask 当前Reduce任务对象
     * @param mapOutputFile Map输出文件管理对象
     * @param localMapFiles 本地Map输出文件映射表
     */
    public Context(org.apache.hadoop.mapreduce.TaskAttemptID reduceId,
                   JobConf jobConf, FileSystem localFS,
                   TaskUmbilicalProtocol umbilical,
                   LocalDirAllocator localDirAllocator,
                   Reporter reporter, CompressionCodec codec,
                   Class<? extends Reducer> combinerClass,
                   CombineOutputCollector<K,V> combineCollector,
                   Counters.Counter spilledRecordsCounter,
                   Counters.Counter reduceCombineInputCounter,
                   Counters.Counter shuffledMapsCounter,
                   Counters.Counter reduceShuffleBytes,
                   Counters.Counter failedShuffleCounter,
                   Counters.Counter mergedMapOutputsCounter,
                   TaskStatus status, Progress copyPhase, Progress mergePhase,
                   Task reduceTask, MapOutputFile mapOutputFile,
                   Map<TaskAttemptID, MapOutputFile> localMapFiles) {
      this.reduceId = reduceId;
      this.jobConf = jobConf;
      this.localFS = localFS;
      this. umbilical = umbilical;
      this.localDirAllocator = localDirAllocator;
      this.reporter = reporter;
      this.codec = codec;
      this.combinerClass = combinerClass;
      this.combineCollector = combineCollector;
      this.spilledRecordsCounter = spilledRecordsCounter;
      this.reduceCombineInputCounter = reduceCombineInputCounter;
      this.shuffledMapsCounter = shuffledMapsCounter;
      this.reduceShuffleBytes = reduceShuffleBytes;
      this.failedShuffleCounter = failedShuffleCounter;
      this.mergedMapOutputsCounter = mergedMapOutputsCounter;
      this.status = status;
      this.copyPhase = copyPhase;
      this.mergePhase = mergePhase;
      this.reduceTask = reduceTask;
      this.mapOutputFile = mapOutputFile;
      this.localMapFiles = localMapFiles;
    }

    public org.apache.hadoop.mapreduce.TaskAttemptID getReduceId() {
      return reduceId;
    }
    public JobConf getJobConf() {
      return jobConf;
    }
    public FileSystem getLocalFS() {
      return localFS;
    }
    public TaskUmbilicalProtocol getUmbilical() {
      return umbilical;
    }
    public LocalDirAllocator getLocalDirAllocator() {
      return localDirAllocator;
    }
    public Reporter getReporter() {
      return reporter;
    }
    public CompressionCodec getCodec() {
      return codec;
    }
    public Class<? extends Reducer> getCombinerClass() {
      return combinerClass;
    }
    public CombineOutputCollector<K, V> getCombineCollector() {
      return combineCollector;
    }
    public Counters.Counter getSpilledRecordsCounter() {
      return spilledRecordsCounter;
    }
    public Counters.Counter getReduceCombineInputCounter() {
      return reduceCombineInputCounter;
    }
    public Counters.Counter getShuffledMapsCounter() {
      return shuffledMapsCounter;
    }
    public Counters.Counter getReduceShuffleBytes() {
      return reduceShuffleBytes;
    }
    public Counters.Counter getFailedShuffleCounter() {
      return failedShuffleCounter;
    }
    public Counters.Counter getMergedMapOutputsCounter() {
      return mergedMapOutputsCounter;
    }
    public TaskStatus getStatus() {
      return status;
    }
    public Progress getCopyPhase() {
      return copyPhase;
    }
    public Progress getMergePhase() {
      return mergePhase;
    }
    public Task getReduceTask() {
      return reduceTask;
    }
    public MapOutputFile getMapOutputFile() {
      return mapOutputFile;
    }
    public Map<TaskAttemptID, MapOutputFile> getLocalMapFiles() {
      return localMapFiles;
    }
  } // end of public static class Context<K,V>

}