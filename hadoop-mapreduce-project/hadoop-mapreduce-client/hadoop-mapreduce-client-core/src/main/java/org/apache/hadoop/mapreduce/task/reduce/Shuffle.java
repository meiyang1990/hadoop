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
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapred.ShuffleConsumerPlugin;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progress;

/**
 * MapReduce Reduce阶段Shuffle过程默认实现，负责从各个Map任务拉取输出数据，
 * 进行合并排序后，为后续Reduce处理提供有序键值对迭代器，是Reduce阶段核心组件。
 * 实现了ShuffleConsumerPlugin接口和异常上报接口，协调事件拉取、数据拷贝、合并多个子阶段。
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
@SuppressWarnings({"unchecked", "rawtypes"})
public class Shuffle<K, V> implements ShuffleConsumerPlugin<K, V>,
    ExceptionReporter {
  private static final int PROGRESS_FREQUENCY = 2000;
  private static final int MAX_EVENTS_TO_FETCH = 10000;
  private static final int MIN_EVENTS_TO_FETCH = 100;
  private static final int MAX_RPC_OUTSTANDING_EVENTS = 3000000;
  
  private ShuffleConsumerPlugin.Context context;

  private TaskAttemptID reduceId;
  private JobConf jobConf;
  private Reporter reporter;
  private ShuffleClientMetrics metrics;
  private TaskUmbilicalProtocol umbilical;
  
  private ShuffleSchedulerImpl<K, V> scheduler;
  private MergeManager<K, V> merger;
  private Throwable throwable = null;
  private String throwingThreadName = null;
  private Progress copyPhase;
  private TaskStatus taskStatus;
  private Task reduceTask; //Used for status updates
  private Map<TaskAttemptID, MapOutputFile> localMapFiles;

  /**
   * 初始化Shuffle消费者组件，保存上下文并创建调度器和合并管理器。
   * @param context Shuffle消费者上下文，包含任务ID、配置、协议、计数器等核心信息
   */
  @Override
  public void init(ShuffleConsumerPlugin.Context context) {
    this.context = context;

    this.reduceId = context.getReduceId();
    this.jobConf = context.getJobConf();
    this.umbilical = context.getUmbilical();
    this.reporter = context.getReporter();
    this.metrics = ShuffleClientMetrics.create(context.getReduceId(),
        this.jobConf);
    this.copyPhase = context.getCopyPhase();
    this.taskStatus = context.getStatus();
    this.reduceTask = context.getReduceTask();
    this.localMapFiles = context.getLocalMapFiles();
    
    scheduler = new ShuffleSchedulerImpl<K, V>(jobConf, taskStatus, reduceId,
        this, copyPhase, context.getShuffledMapsCounter(),
        context.getReduceShuffleBytes(), context.getFailedShuffleCounter());
    merger = createMergeManager(context);
  }

  /**
   * 创建合并管理器实例，负责对拉取到的Map输出进行合并操作。
   * @param context Shuffle消费者上下文
   * @return 合并管理器实例
   */
  protected MergeManager<K, V> createMergeManager(
      ShuffleConsumerPlugin.Context context) {
    return new MergeManagerImpl<K, V>(reduceId, jobConf, context.getLocalFS(),
        context.getLocalDirAllocator(), reporter, context.getCodec(),
        context.getCombinerClass(), context.getCombineCollector(), 
        context.getSpilledRecordsCounter(),
        context.getReduceCombineInputCounter(),
        context.getMergedMapOutputsCounter(), this, context.getMergePhase(),
        context.getMapOutputFile());
  }

  /**
   * 执行完整Shuffle流程，包括拉取Map完成事件、并发拷贝Map输出、合并排序，最终返回有序键值对迭代器。
   * @return 合并排序完成后的原始键值对迭代器，供Reduce任务处理
   * @throws IOException 当IO操作或Shuffle过程发生异常时抛出
   * @throws InterruptedException 当线程被中断时抛出
   */
  @Override
  public RawKeyValueIterator run() throws IOException, InterruptedException {
    // 根据Reducer数量调整每次RPC拉取的最大事件数，避免AM端OOM（大量Reducer同时请求的惊群效应）
    int eventsPerReducer = Math.max(MIN_EVENTS_TO_FETCH,
        MAX_RPC_OUTSTANDING_EVENTS / jobConf.getNumReduceTasks());
    int maxEventsToFetch = Math.min(MAX_EVENTS_TO_FETCH, eventsPerReducer);

    // 启动Map完成事件拉取线程，从AM拉取已完成的Map任务输出位置信息
    final EventFetcher<K, V> eventFetcher =
        new EventFetcher<K, V>(reduceId, umbilical, scheduler, this,
            maxEventsToFetch);
    eventFetcher.start();
    
    // 启动Map输出数据拉取线程，计算需要的线程数
    boolean isLocal = localMapFiles != null;
    final int numFetchers = isLocal ? 1 :
        jobConf.getInt(MRJobConfig.SHUFFLE_PARALLEL_COPIES, 5);
    Fetcher<K, V>[] fetchers = new Fetcher[numFetchers];
    if (isLocal) {
      // 本地模式，创建本地拉取器直接读取本地Map输出
      fetchers[0] = new LocalFetcher<K, V>(jobConf, reduceId, scheduler,
          merger, reporter, metrics, this, reduceTask.getShuffleSecret(),
          localMapFiles);
      fetchers[0].start();
    } else {
      // 分布式模式，创建多个远程拉取线程并发拉取Map输出
      for (int i=0; i < numFetchers; ++i) {
        fetchers[i] = new Fetcher<K, V>(jobConf, reduceId, scheduler, merger,
                                       reporter, metrics, this, 
                                       reduceTask.getShuffleSecret());
        fetchers[i].start();
      }
    }
    
    // 等待Shuffle流程完成，定期上报进度
    while (!scheduler.waitUntilDone(PROGRESS_FREQUENCY)) {
      reporter.progress();
      
      // 检查是否有异常发生，有则抛出
      synchronized (this) {
        if (throwable != null) {
          throw new ShuffleError("error in shuffle in " + throwingThreadName,
                                 throwable);
        }
      }
    }

    // 停止事件拉取线程
    eventFetcher.shutDown();
    
    // 停止所有数据拉取线程
    for (Fetcher<K, V> fetcher : fetchers) {
      fetcher.shutDown();
    }
    
    // 关闭调度器
    scheduler.close();

    copyPhase.complete(); // 拷贝阶段完成
    taskStatus.setPhase(TaskStatus.Phase.SORT);
    // 更新任务状态到AM
    reduceTask.statusUpdate(umbilical);

    // 等待所有合并完成，获取最终合并后的键值对迭代器
    RawKeyValueIterator kvIter = null;
    try {
      kvIter = merger.close();
    } catch (Throwable e) {
      throw new ShuffleError("Error while doing final merge ", e);
    }

    // 最终异常检查
    synchronized (this) {
      if (throwable != null) {
        throw new ShuffleError("error in shuffle in " + throwingThreadName,
                               throwable);
      }
    }
    
    return kvIter;
  }

  /**
   * 关闭Shuffle消费者，释放资源，本实现无需额外清理。
   */
  @Override
  public void close(){
  }

  /**
   * 上报Shuffle过程中发生的异常，保存异常信息并唤醒等待线程。
   * @param t 发生的异常对象
   */
  public synchronized void reportException(Throwable t) {
    if (throwable == null) {
      throwable = t;
      throwingThreadName = Thread.currentThread().getName();
      // 唤醒调度器等待线程，使其立即发现异常
      synchronized (scheduler) {
        scheduler.notifyAll();
      }
    }
  }
  
  /**
   * Shuffle过程异常封装，用于包装Shuffle流程中发生的各类异常。
   */
  public static class ShuffleError extends IOException {
    private static final long serialVersionUID = 5753909320586607881L;

    ShuffleError(String msg, Throwable t) {
      super(msg, t);
    }
  }
}