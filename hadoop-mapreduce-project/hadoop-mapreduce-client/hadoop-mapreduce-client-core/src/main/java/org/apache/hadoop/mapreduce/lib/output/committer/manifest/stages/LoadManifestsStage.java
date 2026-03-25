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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.EntryFileIO;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.LoadedManifestData;
import org.apache.hadoop.util.functional.TaskPool;

import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASK_DIRECTORY_COUNT_MEAN;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASK_FILE_COUNT_MEAN;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASK_MANIFEST_FILE_SIZE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_LOAD_ALL_MANIFESTS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_LOAD_MANIFESTS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.maybeAddIOStatistics;
import static org.apache.hadoop.util.functional.RemoteIterators.haltableRemoteIterator;

/**
 * 文件：加载作业尝试目录下所有任务清单文件的提交阶段
 * 调用时机：作业提交阶段执行
 * 核心功能：并行加载所有任务生成的清单文件，合并目录信息，将待提交文件写入本地序列文件
 * 内存优化：若开启修剪，会移除清单中的IO统计信息以降低内存占用
 */
public class LoadManifestsStage extends
    AbstractJobOrTaskStage<
        LoadManifestsStage.Arguments,
        LoadManifestsStage.Result> {

  private static final Logger LOG = LoggerFactory.getLogger(
      LoadManifestsStage.class);

  /**
   * 清单加载过程的汇总信息
   */
  private final SummaryInfo summaryInfo = new SummaryInfo();

  /**
   * 从所有清单中收集得到的目录条目映射，合并去重减少冗余
   */
  private final Map<String, DirEntry> directories = new ConcurrentHashMap<>();

  /**
   * 文件条目写入器，用于将待提交文件写入本地文件
   */
  private EntryFileIO.EntryWriter entryWriter;

  /**
   * 构造加载清单阶段实例
   * @param stageConfig 阶段配置信息
   */
  public LoadManifestsStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_LOAD_MANIFESTS, true);
  }

  /**
   * 执行加载所有任务清单的核心流程
   * @param arguments 阶段输入参数
   * @return 加载结果，包含汇总信息和处理后的清单数据
   * @throws IOException IO操作失败时抛出
   */
  @Override
  protected LoadManifestsStage.Result executeStage(
      final LoadManifestsStage.Arguments arguments) throws IOException {

    EntryFileIO entryFileIO = new EntryFileIO(getStageConfig().getConf());

    final Path manifestDir = getTaskManifestDir();
    LOG.info("{}: Executing Manifest Job Commit with manifests in {}",
        getName(),
        manifestDir);

    final Path entrySequenceData = arguments.getEntrySequenceData();

    // 初始化用于排队条目的写入器
    entryWriter = entryFileIO.launchEntryWriter(
            entryFileIO.createWriter(entrySequenceData),
            arguments.queueCapacity);

    try {

      // 列清单文件前同步文件系统元数据
      msync(manifestDir);

      // 获取所有已成功提交的任务清单文件，写入停止时会自动中断遍历
      final RemoteIterator<FileStatus> manifestFiles =
          haltableRemoteIterator(listManifests(),
              () -> entryWriter.isActive());

      // 并行处理所有清单文件
      processAllManifests(manifestFiles);
      maybeAddIOStatistics(getIOStatistics(), manifestFiles);

      LOG.info("{}: Summary of {} manifests loaded in {}: {}",
          getName(),
          summaryInfo.manifestCount,
          manifestDir,
          summaryInfo);

      // 正常关闭写入器
      entryWriter.close();

      // 如果写入过程出现异常，抛出异常
      entryWriter.maybeRaiseWriteException();

    } catch (EntryWriteException e) {
      // 写入过程发生错误
      // 先检查写入线程是否已有异常
      entryWriter.maybeRaiseWriteException();

      // 没有的话抛出工作线程捕获的异常
      throw e;
    } finally {
      // 再次关闭，正常关闭后此处为空操作；读取/解析/处理出错时会执行关闭清理
      entryWriter.close();
    }

    // 封装加载完成的清单数据：将目录集合转为ArrayList释放ConcurrentHashMap占用
    final LoadedManifestData loadedManifestData = new LoadedManifestData(
        new ArrayList<>(directories.values()),
        entrySequenceData,
        entryWriter.getCount());

    return new LoadManifestsStage.Result(summaryInfo, loadedManifestData);
  }

  /**
   * 并行加载处理所有清单文件
   * @param manifestFiles 清单文件状态迭代器
   * @throws IOException 加载/解析/排队失败时抛出
   */
  private void processAllManifests(
      final RemoteIterator<FileStatus> manifestFiles) throws IOException {

    trackDurationOfInvocation(getIOStatistics(), OP_LOAD_ALL_MANIFESTS, () ->
        TaskPool.foreach(manifestFiles)
            .executeWith(getIOProcessors())
            .stopOnFailure()
            .run(this::processOneManifest));
  }

  /**
   * 处理单个任务清单文件
   * @param status 清单文件状态
   * @throws IOException 加载/解析/排队失败时抛出
   */
  private void processOneManifest(FileStatus status)
      throws IOException {
    updateAuditContext(OP_LOAD_ALL_MANIFESTS);

    // 加载清单文件到内存
    TaskManifest manifest = fetchTaskManifest(status);
    progress();

    // 合并去重目录信息
    final int created = coalesceDirectories(manifest);
    final String attemptID = manifest.getTaskAttemptID();
    LOG.debug("{}: task attempt {} added {} directories",
        getName(), attemptID, created);

    // 更新汇总统计信息
    summaryInfo.add(manifest);

    // 清理清单中的额外数据，降低排队等待时的内存占用
    manifest.setIOStatistics(null);
    manifest.getExtraData().clear();

    // 将待提交文件条目写入队列
    final boolean enqueued = entryWriter.enqueue(manifest.getFilesToCommit());
    if (!enqueued) {
      LOG.warn("{}: Failed to write manifest for task {}",
          getName(), attemptID);
      throw new EntryWriteException(attemptID);
    }

  }

  /**
   * 合并清单中的目录信息并去重，处理后清空清单中的目录条目
   * 只有存在新目录需要添加时才会加锁，减少锁竞争
   * @param manifest 待处理的任务清单
   * @return 新增不重复目录的数量
   */
  @VisibleForTesting
  int coalesceDirectories(final TaskManifest manifest) {

    // 过滤出全局目录映射中不存在的目录
    final List<DirEntry> toCreate = manifest.getDestDirectories().stream()
        .filter(e -> !directories.containsKey(e))
        .collect(Collectors.toList());
    if (!toCreate.isEmpty()) {
      // 需要新增目录，加锁同步保证原子性，避免重复插入
      synchronized (directories) {
        toCreate.forEach(entry -> {
          directories.putIfAbsent(entry.getDir(), entry);
        });
      }
    }
    return toCreate.size();
  }

  /**
   * 加载并验证单个任务清单文件，为降低内存占用会清理统计和额外数据
   * @param status 清单文件状态
   * @return 加载完成的任务清单对象
   * @throws IOException 文件无效或加载失败时抛出
   */
  private TaskManifest fetchTaskManifest(FileStatus status)
      throws IOException {
    if (status.getLen() == 0 || !status.isFile()) {
      throw new PathIOException(status.getPath().toString(),
          "Not a valid manifest file; file status = " + status);
    }
    // 加载并验证清单
    final TaskManifest manifest = loadManifest(status);
    final String id = manifest.getTaskAttemptID();
    final int filecount = manifest.getFilesToCommit().size();
    final long size = manifest.getTotalFileSize();
    LOG.info("{}: Task Attempt {} file {}: File count: {}; data size={}",
        getName(), id, status.getPath(), filecount, size);

    // 记录统计样本，用于监控和诊断
    final IOStatisticsStore iostats = getIOStatistics();
    iostats.addSample(COMMITTER_TASK_MANIFEST_FILE_SIZE, status.getLen());
    iostats.addSample(COMMITTER_TASK_FILE_COUNT_MEAN, filecount);
    iostats.addSample(COMMITTER_TASK_DIRECTORY_COUNT_MEAN,
        manifest.getDestDirectories().size());
    return manifest;
  }

  /**
   * 加载清单阶段的输入参数封装
   */
  public static final class Arguments {
    /**
     * 本地存储文件条序列的文件
     */
    private final File entrySequenceFile;

    /**
     * 加载线程和写入线程之间的队列容量
     */
    private final int queueCapacity;

    /**
     * 构造参数实例
     * @param entrySequenceFile 本地存储条目的文件路径
     * @param queueCapacity 队列容量限制
     */
    public Arguments(
        final File entrySequenceFile,
        final int queueCapacity) {
      this.entrySequenceFile = entrySequenceFile;
      this.queueCapacity = queueCapacity;
    }

    private Path getEntrySequenceData() {
      return new Path(entrySequenceFile.toURI());

    }
  }

  /**
   * 加载清单阶段的输出结果封装
   */
  public static final class Result {
    private final SummaryInfo summary;

    /**
     * 传递给后续阶段的加载完成的清单数据
     */
    private final LoadedManifestData loadedManifestData;

    /**
     * 构造结果实例
     * @param summary 加载过程汇总信息
     * @param loadedManifestData 处理完成的清单数据
     */
    public Result(
        final SummaryInfo summary,
        final LoadedManifestData loadedManifestData) {
      this.summary = summary;
      this.loadedManifestData = loadedManifestData;
    }

    public SummaryInfo getSummary() {
      return summary;
    }

    public LoadedManifestData getLoadedManifestData() {
      return loadedManifestData;
    }
  }

  /**
   * 条目写入失败时抛出的异常
   */
  public static final class EntryWriteException extends IOException {

    private EntryWriteException(String taskId) {
      super("Failed to write manifest data for task "
          + taskId + "to local file");
    }
  }

  /**
   * 加载过程汇总信息，使用原子计数器保证线程安全
   */
  public static final class SummaryInfo implements IOStatisticsSource {

    /**
     * 聚合所有清单的IO统计信息
     */
    private final IOStatisticsSnapshot iostatistics = snapshotIOStatistics();

    /**
     * 所有加载成功的任务ID列表
     */
    private final List<String> taskIDs = new ArrayList<>();

    /**
     * 所有加载成功的任务尝试ID列表
     */
    private final List<String> taskAttemptIDs = new ArrayList<>();

    /**
     * 已加载的清单数量
     */
    private AtomicLong manifestCount = new AtomicLong();

    /**
     * 所有待提交文件总数
     */
    private AtomicLong fileCount = new AtomicLong();

    /**
     * 所有需要创建的目录总数（未去重，数值会比实际大）
     */
    private AtomicLong directoryCount = new AtomicLong();

    /**
     * 所有待提交文件的总大小
     */
    private AtomicLong totalFileSize = new AtomicLong();

    /**
     * 获取聚合后的IO统计信息
     * @return 聚合IO统计快照
     */
    @Override
    public IOStatisticsSnapshot getIOStatistics() {
      return iostatistics;
    }

    public long getFileCount() {
      return fileCount.get();
    }

    public long getDirectoryCount() {
      return directoryCount.get();
    }

    public long getTotalFileSize() {
      return totalFileSize.get();
    }

    public long getManifestCount() {
      return manifestCount.get();
    }

    public List<String> getTaskIDs() {
      return taskIDs;
    }

    public List<String> getTaskAttemptIDs() {
      return taskAttemptIDs;
    }

    /**
     * 同步添加单个清单的统计信息到汇总
     * @param manifest 已加载的任务清单
     */
    public synchronized void add(TaskManifest manifest) {
      manifestCount.incrementAndGet();
      iostatistics.aggregate(manifest.getIOStatistics());
      fileCount.addAndGet(manifest.getFilesToCommit().size());
      directoryCount.addAndGet(manifest.getDestDirectories().size());
      totalFileSize.addAndGet(manifest.getTotalFileSize());
      taskIDs.add(manifest.getTaskID());
      taskAttemptIDs.add(manifest.getTaskAttemptID());
    }

    /**
     * 生成汇总信息的字符串表示，不包含详细统计
     * @return 汇总信息字符串
     */
    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "SummaryInfo{");
      sb.append("manifestCount=").append(getManifestCount());
      sb.append(", fileCount=").append(getFileCount());
      sb.append(", directoryCount=").append(getDirectoryCount());
      sb.append(", totalFileSize=").append(
          byteCountToDisplaySize(getTotalFileSize()));
      sb.append('}');
      return sb.toString();
    }
  }
}