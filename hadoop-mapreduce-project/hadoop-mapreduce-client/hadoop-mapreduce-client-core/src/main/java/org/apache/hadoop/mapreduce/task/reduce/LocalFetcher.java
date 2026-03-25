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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.crypto.SecretKey;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.IndexRecord;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SpillRecord;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.mapreduce.security.IntermediateEncryptedStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：Local模式下MapReduce Shuffle阶段的Map输出拉取器，供LocalJobRunner使用
 * 核心功能：从本地文件系统拉取同一作业中Map任务的输出，供给Reduce任务合并处理
 */
/**
 * LocalFetcher是LocalJobRunner本地运行模式下，用于从本地文件系统拉取Map输出的拉取器
 * 继承通用Fetcher基类，实现本地模式下的shuffle拉取逻辑
 */
class LocalFetcher<K,V> extends Fetcher<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFetcher.class);

  private static final MapHost LOCALHOST = new MapHost("local", "local");

  private JobConf job;
  private Map<TaskAttemptID, MapOutputFile> localMapFiles;

  /**
   * 构造LocalFetcher实例，初始化拉取所需的配置和依赖
   * @param job 作业配置对象
   * @param reduceId 当前Reduce任务尝试ID
   * @param scheduler Shuffle调度器实例
   * @param merger 合并管理器实例
   * @param reporter 任务进度上报器
   * @param metrics Shuffle客户端性能指标收集器
   * @param exceptionReporter 异常上报器
   * @param shuffleKey Shuffle加密密钥
   * @param localMapFiles 本地Map任务输出文件映射表，key为Map任务尝试ID，value为输出文件信息
   */
  public LocalFetcher(JobConf job, TaskAttemptID reduceId,
                 ShuffleSchedulerImpl<K, V> scheduler,
                 MergeManager<K,V> merger,
                 Reporter reporter, ShuffleClientMetrics metrics,
                 ExceptionReporter exceptionReporter,
                 SecretKey shuffleKey,
                 Map<TaskAttemptID, MapOutputFile> localMapFiles) {
    super(job, reduceId, scheduler, merger, reporter, metrics,
        exceptionReporter, shuffleKey);

    this.job = job;
    this.localMapFiles = localMapFiles;

    setName("localfetcher#" + id);
    setDaemon(true);
  }

  /**
   * LocalFetcher主工作方法，循环拉取所有Map任务输出直到完成
   */
  public void work() {
    // 初始化待拉取的Map任务列表
    Set<TaskAttemptID> maps = new HashSet<TaskAttemptID>();
    for (TaskAttemptID map : localMapFiles.keySet()) {
      maps.add(map);
    }

    while (maps.size() > 0) {
      try {
        // 如果合并已经在进行，等待资源释放
        merger.waitForResource();
        metrics.threadBusy();

        // 批量拉取尽可能多的Map输出
        doCopy(maps);
        metrics.threadFree();
      } catch (InterruptedException ie) {
      } catch (Throwable t) {
        exceptionReporter.reportException(t);
      }
    }
  }

  /**
   * 批量拉取Map输出核心方法，遍历待拉取列表逐个拉取
   * @param maps 待拉取的Map任务尝试ID集合
   * @throws IOException 本地文件读写异常时抛出
   */
  private void doCopy(Set<TaskAttemptID> maps) throws IOException {
    Iterator<TaskAttemptID> iter = maps.iterator();
    while (iter.hasNext()) {
      TaskAttemptID map = iter.next();
      LOG.debug("LocalFetcher " + id + " going to fetch: " + map);
      if (copyMapOutput(map)) {
        // 拉取成功，从待处理列表移除
        iter.remove();
      } else {
        // 资源不足需要等待，跳出循环重新等待合并资源
        break;
      }
    }
  }

  /**
   * 拉取单个Map任务的输出，并交给合并管理器处理
   * @param mapTaskId 目标Map任务尝试ID
   * @return 拉取成功返回true，需要等待资源返回false
   * @throws IOException 本地文件读写异常时抛出
   */
  private boolean copyMapOutput(TaskAttemptID mapTaskId) throws IOException {
    // 获取Map任务输出文件路径
    Path mapOutputFileName = localMapFiles.get(mapTaskId).getOutputFile();
    Path indexFileName = mapOutputFileName.suffix(".index");

    // 读取索引文件，获取当前Reduce分区的数据位置和大小
    SpillRecord sr = new SpillRecord(indexFileName, job);
    IndexRecord ir = sr.getIndex(reduce);

    long compressedLength = ir.partLength;
    long decompressedLength = ir.rawLength;

    // 减去加密填充字节长度
    compressedLength -= CryptoUtils.cryptoPadding(job);
    decompressedLength -= CryptoUtils.cryptoPadding(job);

    // 向合并管理器申请存储空间，决定是放内存还是磁盘
    MapOutput<K, V> mapOutput = merger.reserve(mapTaskId, decompressedLength,
        id);

    // 检查当前是否可以开始shuffle，资源不足返回WAIT
    if (mapOutput == null) {
      LOG.info("fetcher#" + id + " - MergeManager returned Status.WAIT ...");
      return false;
    }

    // 日志记录本次拉取信息
    LOG.info("localfetcher#" + id + " about to shuffle output of map " + 
             mapOutput.getMapId() + " decomp: " +
             decompressedLength + " len: " + compressedLength + " to " +
             mapOutput.getDescription());

    // 打开本地文件系统，读取Map输出文件
    FileSystem localFs = FileSystem.getLocal(job).getRaw();
    FSDataInputStream inStream = localFs.open(mapOutputFileName);
    try {
      // 跳转到当前Reduce分区的起始偏移
      inStream.seek(ir.startOffset);
      // 如果启用加密，包装为加密输入流
      inStream =
          IntermediateEncryptedStream.wrapIfNecessary(job, inStream,
              mapOutputFileName);
      // 将数据交给MapOutput进行shuffle处理
      mapOutput.shuffle(LOCALHOST, inStream, compressedLength,
          decompressedLength, metrics, reporter);
    } finally {
      // 关闭输入流
      IOUtils.cleanupWithLogger(LOG, inStream);
    }

    // 通知Shuffle调度器拷贝成功，更新状态
    scheduler.copySucceeded(mapTaskId, LOCALHOST, compressedLength, 0, 0,
        mapOutput);
    return true; // 拉取成功
  }
}