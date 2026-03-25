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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.EntryStatus;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASK_DIRECTORY_COUNT_MEAN;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASK_DIRECTORY_DEPTH_MEAN;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASK_FILE_COUNT_MEAN;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASK_FILE_SIZE_MEAN;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_SCAN_DIRECTORY;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.createTaskManifest;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.maybeAddIOStatistics;

/**
 * 扫描任务尝试输出目录树并构建任务清单文件的处理阶段。
 * 该阶段由任务提交器在任务执行完成后执行，用于收集所有需要提交的文件和目录信息。
 */
public final class TaskAttemptScanDirectoryStage
    extends AbstractJobOrTaskStage<Void, TaskManifest> {

  private static final Logger LOG = LoggerFactory.getLogger(
      TaskAttemptScanDirectoryStage.class);

  /**
   * 构造目录扫描处理阶段实例。
   * @param stageConfig 阶段配置信息
   */
  public TaskAttemptScanDirectoryStage(
      final StageConfig stageConfig) {
    super(true, stageConfig, OP_STAGE_TASK_SCAN_DIRECTORY, false);
  }

  /**
   * 执行目录扫描并构建任务清单文件。
   * @param arguments 无输入参数
   * @return 构建完成的任务清单文件，包含所有需要提交的文件和目录信息
   * @throws IOException 扫描目录或IO操作失败时抛出
   */
  @Override
  protected TaskManifest executeStage(final Void arguments)
      throws IOException {

    // 获取当前任务尝试的输出目录
    final Path taskAttemptDir = getRequiredTaskAttemptDir();
    // 创建空任务清单对象
    final TaskManifest manifest = createTaskManifest(getStageConfig());

    LOG.info("{}: scanning directory {}",
        getName(), taskAttemptDir);

    // 递归扫描整个目录树，收集文件和目录信息，返回目录最大深度
    final int depth = scanDirectoryTree(manifest,
        taskAttemptDir,
        getDestinationDir(),
        0, true);
    // 统计已收集文件的基本信息
    List<FileEntry> filesToCommit = manifest.getFilesToCommit();
    LongSummaryStatistics fileSummary = filesToCommit.stream()
        .mapToLong(FileEntry::getSize)
        .summaryStatistics();
    long fileDataSize = fileSummary.getSum();
    long fileCount = fileSummary.getCount();
    int dirCount = manifest.getDestDirectories().size();
    LOG.info("{}: directory {} contained {} file(s); data size {}",
        getName(),
        taskAttemptDir,
        fileCount,
        fileDataSize);
    LOG.info("{}: Directory count = {}; maximum depth {}",
        getName(),
        dirCount,
        depth);
    // 将当前任务输出结构统计信息存入IO统计，聚合后可用于分析作业结构和任务数据倾斜
    IOStatisticsStore iostats = getIOStatistics();
    iostats.addSample(COMMITTER_TASK_DIRECTORY_COUNT_MEAN, dirCount);
    iostats.addSample(COMMITTER_TASK_DIRECTORY_DEPTH_MEAN, depth);
    iostats.addSample(COMMITTER_TASK_FILE_COUNT_MEAN, fileCount);
    iostats.addSample(COMMITTER_TASK_FILE_SIZE_MEAN, fileDataSize);

    return manifest;
  }

  /**
   * 递归扫描目录树，收集所有需要提交的文件和目录信息到任务清单。
   * 先处理当前目录下的所有文件，再递归处理子目录，便于统计信息收集。
   * 暂未实现异步迭代获取等优化，因为该阶段不在关键路径上。
   * @param manifest 用于收集信息的任务清单对象
   * @param srcDir 当前需要扫描的源目录（任务尝试输出下的目录）
   * @param destDir 该目录对应的最终输出目标路径
   * @param depth 当前目录相对于任务尝试根目录的深度
   * @param parentDirExists 父目录在最终输出路径中是否存在
   * @return 当前目录树的最大深度
   * @throws IOException 目录列表或IO操作失败时抛出
   */
  private int scanDirectoryTree(
      TaskManifest manifest,
      Path srcDir,
      Path destDir,
      int depth,
      boolean parentDirExists) throws IOException {

    // 目录扫描可能很慢，更新任务进度避免超时
    progress();

    int maxDepth = 0;
    int files = 0;
    boolean dirExists = parentDirExists;
    List<FileStatus> subdirs = new ArrayList<>();
    try (DurationInfo ignored = new DurationInfo(LOG, false,
        "Task Attempt %s source dir %s, dest dir %s",
        getTaskAttemptId(), srcDir, destDir)) {

      // 获取目录的文件迭代器，不同文件系统可能同步或异步获取列表
      final RemoteIterator<FileStatus> listing = listStatusIterator(srcDir);

      // 利用异步列表获取的间隙，提前探测目标目录状态
      // 仅对根目录以外的目录添加目标目录条目
      if (depth > 0) {
        final EntryStatus status;
        if (parentDirExists) {
          // 探测目标目录是否已存在
          final FileStatus destDirStatus = getFileStatusOrNull(destDir);
          status = EntryStatus.toEntryStatus(destDirStatus);
          dirExists = destDirStatus != null;
        } else {
          // 父目录不存在，则当前目录必然不存在，无需探测直接标记
          status = EntryStatus.not_found;
        }
        // 将目录信息添加到任务清单
        manifest.addDirectory(DirEntry.dirEntry(
            destDir,
            status,
            depth));
      }

      // 遍历目录条目，此时异步文件系统会阻塞等待列表获取完成
      while (listing.hasNext()) {
        final FileStatus st = listing.next();
        if (st.isFile()) {
          // 普通文件，添加到待提交文件列表
          files++;
          final FileEntry entry = fileEntry(st, destDir);
          manifest.addFileToCommit(entry);
          LOG.debug("To rename: {}", entry);
        } else {
          if (st.isDirectory()) {
            // 子目录，暂存后续递归处理
            subdirs.add(st);
          } else {
            // 忽略其他类型的文件系统对象（例如符号链接）
            LOG.info("Ignoring FS object {}", st);
          }
        }
      }
      // 合并目录列表操作产生的IO统计信息
      maybeAddIOStatistics(getIOStatistics(), listing);
    }

    // 递归处理所有子目录
    LOG.debug("{}: Number of subdirectories under {} found: {}; file count {}",
        getName(), srcDir, subdirs.size(), files);

    for (FileStatus st : subdirs) {
      Path destSubDir = new Path(destDir, st.getPath().getName());
      final int d = scanDirectoryTree(manifest,
          st.getPath(),
          destSubDir,
          depth + 1,
          dirExists);
      maxDepth = Math.max(maxDepth, d);
    }

    return 1 + maxDepth;
  }

}