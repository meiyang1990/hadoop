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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.hadoop.fs.FileUtil.maybeIgnoreMissingDirectory;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;

/**
 * 文件输入路径块位置信息获取工具类，使用多线程并发获取指定输入路径下所有文件的块位置信息
 * 线程数量由配置参数"mapreduce.input.fileinputformat.list-status.num-threads"决定
 * 用于MapReduce输入阶段并行列举输入文件，提升大输入量下的列表获取性能
 */
@Private
public class LocatedFileStatusFetcher implements IOStatisticsSource {

  public static final Logger LOG =
      LoggerFactory.getLogger(LocatedFileStatusFetcher.class.getName());
  private final Path[] inputDirs;
  private final PathFilter inputFilter;
  private final Configuration conf;
  private final boolean recursive;
  private final boolean newApi;
  
  private final ExecutorService rawExec;
  private final ListeningExecutorService exec;
  private final BlockingQueue<List<FileStatus>> resultQueue;
  private final List<IOException> invalidInputErrors = new LinkedList<>();

  private final ProcessInitialInputPathCallback processInitialInputPathCallback = 
      new ProcessInitialInputPathCallback();
  private final ProcessInputDirCallback processInputDirCallback = 
      new ProcessInputDirCallback();

  private final AtomicInteger runningTasks = new AtomicInteger(0);

  private final ReentrantLock lock;
  private final Condition condition;

  private volatile Throwable unknownError;

  /**
   * 按需创建的IO统计信息：仅当文件系统返回统计信息时才收集
   */
  private IOStatisticsSnapshot iostats;

  /**
   * 构造LocatedFileStatusFetcher实例
   * newApi参数仅用于配置getFileStatuses()失败时抛出的异常类型，不改变算法逻辑
   * @param conf 作业配置对象
   * @param dirs 初始输入路径列表
   * @param recursive 是否递归遍历子路径
   * @param inputFilter 结果路径过滤器
   * @param newApi 是否使用mapreduce新API（决定异常类型）
   * @throws InterruptedException 线程中断异常
   * @throws IOException IO异常
   */
  public LocatedFileStatusFetcher(Configuration conf, Path[] dirs,
      boolean recursive, PathFilter inputFilter, boolean newApi)
      throws InterruptedException, IOException {
    // 从配置读取并行线程数，使用默认值兜底
    int numThreads = conf.getInt(FileInputFormat.LIST_STATUS_NUM_THREADS,
        FileInputFormat.DEFAULT_LIST_STATUS_NUM_THREADS);
    LOG.debug("Instantiated LocatedFileStatusFetcher with {} threads",
        numThreads);
    // 创建固定大小线程池，使用守护线程
    rawExec = HadoopExecutors.newFixedThreadPool(
        numThreads,
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("GetFileInfo #%d").build());
    // 包装为支持ListenableFuture的执行器
    exec = MoreExecutors.listeningDecorator(rawExec);
    resultQueue = new LinkedBlockingQueue<>();
    // 初始化成员变量
    this.conf = conf;
    this.inputDirs = dirs;
    this.recursive = recursive;
    this.inputFilter = inputFilter;
    this.newApi = newApi;
    this.lock = new ReentrantLock();
    this.condition = lock.newCondition();
  }

  /**
   * 启动获取流程并返回所有获取到的文件状态信息
   * @return 所有输入文件的FileStatus可迭代对象
   * @throws InterruptedException 等待结果时线程中断
   * @throws IOException IO失败或其他错误
   * @throws InvalidInputException 使用旧API时输入无效抛出
   * @throws org.apache.hadoop.mapreduce.lib.input.InvalidInputException 使用新API时输入无效抛出
   */
  public Iterable<FileStatus> getFileStatuses() throws InterruptedException,
      IOException {
    // 增加计数，避免第一个线程完成后其余线程尚未调度导致提前终止
    runningTasks.incrementAndGet();
    // 提交所有初始输入路径处理任务
    for (Path p : inputDirs) {
      LOG.debug("Queuing scan of directory {}", p);
      runningTasks.incrementAndGet();
      ListenableFuture<ProcessInitialInputPathCallable.Result> future = exec
          .submit(new ProcessInitialInputPathCallable(p, conf, inputFilter));
      Futures.addCallback(future, processInitialInputPathCallback,
          MoreExecutors.directExecutor());
    }

    // 减去初始增加的计数
    runningTasks.decrementAndGet();

    // 加锁等待所有任务完成
    lock.lock();
    try {
      LOG.debug("Waiting scan completion");
      // 仍有任务在运行且未出现错误时持续等待
      while (runningTasks.get() != 0 && unknownError == null) {
        condition.await();
      }
    } finally {
      lock.unlock();
      // 无论扫描完成还是出错，都关闭执行器
      LOG.debug("Scan complete: shutting down");
      this.exec.shutdownNow();
    }

    // 如果出现未知错误，按类型抛出对应异常
    if (this.unknownError != null) {
      LOG.debug("Scan failed", this.unknownError);
      if (this.unknownError instanceof Error) {
        throw (Error) this.unknownError;
      } else if (this.unknownError instanceof RuntimeException) {
        throw (RuntimeException) this.unknownError;
      } else if (this.unknownError instanceof IOException) {
        throw (IOException) this.unknownError;
      } else if (this.unknownError instanceof InterruptedException) {
        throw (InterruptedException) this.unknownError;
      } else {
        throw new IOException(this.unknownError);
      }
    }
    // 如果存在输入错误，按API版本抛出对应异常
    if (!this.invalidInputErrors.isEmpty()) {
      LOG.debug("Invalid Input Errors raised");
      for (IOException error : invalidInputErrors) {
        LOG.debug("Error", error);
      }
      if (this.newApi) {
        throw new org.apache.hadoop.mapreduce.lib.input.InvalidInputException(
            invalidInputErrors);
      } else {
        throw new InvalidInputException(invalidInputErrors);
      }
    }
    // 拼接所有结果返回
    return Iterables.concat(resultQueue);
  }

  /**
   * 注册输入配置错误，仅收集不立即抛出，最后统一返回
   * @param errors 输入错误列表
   */
  private void registerInvalidInputError(List<IOException> errors) {
    synchronized (this) {
      this.invalidInputErrors.addAll(errors);
    }
  }

  /**
   * 注册致命错误，如访问文件时的IOException、执行队列满等，会终止整个获取流程
   * @param t 抛出的错误/异常对象
   */
  private void registerError(Throwable t) {
    LOG.debug("Error", t);
    lock.lock();
    try {
      if (unknownError == null) {
        unknownError = t;
        condition.signal();
      }

    } finally {
      lock.unlock();
    }
  }

  /**
   * 减少正在运行任务计数，并检查是否所有任务完成，完成则唤醒等待主线程
   */
  private void decrementRunningAndCheckCompletion() {
    lock.lock();
    try {
      if (runningTasks.decrementAndGet() == 0) {
        condition.signal();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * 获取列举过程中收集的IO统计信息
   * @return 收集到的IO统计信息
   */
  @Override
  public synchronized IOStatistics getIOStatistics() {
    return iostats;
  }

  /**
   * 合并单个线程扫描得到的IO统计信息到整体统计
   * @param stats 单个线程的IO统计信息，可为null
   */
  private void addResultStatistics(IOStatistics stats) {
    if (stats != null) {
      // 按需创建IO统计对象
      synchronized (this) {
        LOG.debug("Adding IOStatistics: {}", stats);
        if (iostats == null) {
          // 第一次添加时创建快照
          iostats = snapshotIOStatistics(stats);
        } else {
          // 后续聚合统计信息
          iostats.aggregate(stats);
        }
      }
    }
  }

  @Override
  public String toString() {
    final IOStatistics ioStatistics = getIOStatistics();
    StringJoiner stringJoiner = new StringJoiner(", ",
        LocatedFileStatusFetcher.class.getSimpleName() + "[", "]");
    if (ioStatistics != null) {
      stringJoiner.add("IOStatistics=" + ioStatistics);
    }
    return stringJoiner.toString();
  }

  /**
   * 处理单个目录/文件的Callable任务，获取文件块位置信息，递归目录则添加新任务到队列
   */
  private static class ProcessInputDirCallable implements
      Callable<ProcessInputDirCallable.Result> {

    private final FileSystem fs;
    private final FileStatus fileStatus;
    private final boolean recursive;
    private final PathFilter inputFilter;

    ProcessInputDirCallable(FileSystem fs, FileStatus fileStatus,
        boolean recursive, PathFilter inputFilter) {
      this.fs = fs;
      this.fileStatus = fileStatus;
      this.recursive = recursive;
      this.inputFilter = inputFilter;
    }

    @Override
    public Result call() throws Exception {
      Result result = new Result();
      result.fs = fs;
      LOG.debug("ProcessInputDirCallable {}", fileStatus);
      try {
        // 当前是目录，遍历目录下所有文件
        if (fileStatus.isDirectory()) {
          RemoteIterator<LocatedFileStatus> iter = fs
              .listLocatedStatus(fileStatus.getPath());
          while (iter.hasNext()) {
            LocatedFileStatus stat = iter.next();
            // 过滤路径
            if (inputFilter.accept(stat.getPath())) {
              // 需要递归且当前是目录，添加到待递归目录列表
              if (recursive && stat.isDirectory()) {
                result.dirsNeedingRecursiveCalls.add(stat);
              } else {
                // 是文件，添加到结果列表，压缩状态节省空间
                result.locatedFileStatuses.add(org.apache.hadoop.mapreduce.lib.
                    input.FileInputFormat.shrinkStatus(stat));
              }
            }
          }
          // 收集迭代器返回的IO统计信息
          result.stats = retrieveIOStatistics(iter);
        } else {
          // 当前是文件，直接添加到结果
          result.locatedFileStatuses.add(fileStatus);
        }
      } catch (FileNotFoundException e) {
        // 根据配置决定是否忽略不存在的目录，不忽略则抛出异常
        maybeIgnoreMissingDirectory(fs, fileStatus.getPath(), e);
      }
      return result;
    }

    /**
     * ProcessInputDirCallable处理结果容器
     */
    private static class Result {
      private List<FileStatus> locatedFileStatuses = new LinkedList<>();
      private List<FileStatus> dirsNeedingRecursiveCalls = new LinkedList<>();
      private FileSystem fs;
      private IOStatistics stats;
    }
  }

  /**
   * ProcessInputDirCallable任务结果回调处理器，将结果放入结果队列，并提交新的递归目录处理任务
   */
  private class ProcessInputDirCallback implements
      FutureCallback<ProcessInputDirCallable.Result> {

    @Override
    public void onSuccess(ProcessInputDirCallable.Result result) {
      try {
        // 合并IO统计信息
        addResultStatistics(result.stats);
        // 将文件结果放入结果队列
        if (!result.locatedFileStatuses.isEmpty()) {
          resultQueue.add(result.locatedFileStatuses);
        }
        // 提交待递归目录的处理任务
        if (!result.dirsNeedingRecursiveCalls.isEmpty()) {
          for (FileStatus fileStatus : result.dirsNeedingRecursiveCalls) {
            LOG.debug("Queueing directory scan {}", fileStatus.getPath());
            runningTasks.incrementAndGet();
            ListenableFuture<ProcessInputDirCallable.Result> future = exec
                .submit(new ProcessInputDirCallable(result.fs, fileStatus,
                    recursive, inputFilter));
            Futures.addCallback(future, processInputDirCallback,
                MoreExecutors.directExecutor());
          }
        }
        // 减少任务计数，检查是否完成
        decrementRunningAndCheckCompletion();
      } catch (Throwable t) { // 回调本身出现错误
        registerError(t);
      }
    }

    @Override
    public void onFailure(Throwable t) {
      // 任务执行失败，注册致命错误终止整个流程
      registerError(t);
    }
  }


  /**
   * 处理初始输入路径的Callable任务，通过通配符匹配和路径过滤生成待处理文件列表
   */
  private static class ProcessInitialInputPathCallable implements
      Callable<ProcessInitialInputPathCallable.Result> {

    private final Path path;
    private final Configuration conf;
    private final PathFilter inputFilter;

    public ProcessInitialInputPathCallable(Path path, Configuration conf,
        PathFilter pathFilter) {
      this.path = path;
      this.conf = conf;
      this.inputFilter = pathFilter;
    }

    @Override
    public Result call() throws Exception {
      Result result = new Result();
      // 获取路径对应的文件系统
      FileSystem fs = path.getFileSystem(conf);
      result.fs = fs;
      LOG.debug("ProcessInitialInputPathCallable path {}", path);
      // 通配符匹配得到符合条件的文件状态
      FileStatus[] matches = fs.globStatus(path, inputFilter);
      // 匹配结果为空，添加输入错误
      if (matches == null) {
        result.addError(new IOException("Input path does not exist: " + path));
      } else if (matches.length == 0) {
        result.addError(new IOException("Input Pattern " + path
            + " matches 0 files"));
      } else {
        // 保存匹配到的结果
        result.matchedFileStatuses = matches;
      }
      return result;
    }

    /**
     * ProcessInitialInputPathCallable处理结果容器
     */
    private static class Result {
      private List<IOException> errors;
      private FileStatus[] matchedFileStatuses;
      private FileSystem fs;

      void addError(IOException ioe) {
        if (errors == null) {
          errors = new LinkedList<IOException>();
        }
        errors.add(ioe);
      }
    }
  }

  /**
   * ProcessInitialInputPathCallable任务结果回调处理器，处理初始路径匹配结果，提交后续文件处理任务
   */
  private class ProcessInitialInputPathCallback implements
      FutureCallback<ProcessInitialInputPathCallable.Result> {

    @Override
    public void onSuccess(ProcessInitialInputPathCallable.Result result) {
      try {
        // 注册输入错误
        if (result.errors != null) {
          registerInvalidInputError(result.errors);
        }
        // 遍历匹配到的结果，提交每个文件/目录处理任务