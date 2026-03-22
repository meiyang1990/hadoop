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
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.EntryFileIO;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.LoadedManifestData;
import org.apache.hadoop.util.functional.TaskPool;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_COMMIT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_RENAME_FILES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.createManifestOutcome;

/**
 * 文件重命名阶段，完成作业提交过程中所有任务输出文件从临时目录到最终输出目录的重命名操作。
 * 输入：
 * <ol>
 *   <li>{@link LoadManifestsStage}加载得到的{@link LoadedManifestData}</li>
 *   <li>{@link CreateOutputDirectoriesStage}创建完成的输出目录集合</li>
 * </ol>
 * 需要重命名的文件通过LoadedManifestData中记录的入口文件读取，采用增量方式逐个处理。
 * 如果配置了删除目标文件且目标父目录为本次新创建，则可以跳过删除已存在目标文件的操作。
 * 阶段返回汇总输出信息的作业成功清单文件，但不会在其中添加IO统计信息。
 */
public class RenameFilesStage extends
    AbstractJobOrTaskStage<
        Triple<LoadedManifestData, Set<Path>, Integer>,
        ManifestSuccessData> {

  private static final Logger LOG = LoggerFactory.getLogger(
      RenameFilesStage.class);

  /**
   * 已提交成功的文件列表。
   */
  private final List<FileEntry> filesCommitted = new ArrayList<>();

  /**
   * 已提交文件总大小。
   */
  private long totalFileSize = 0;

  /** 本次提交创建的所有输出目录集合 */
  private Set<Path> createdDirectories;

  /**
   * 构造RenameFilesStage实例。
   * @param stageConfig 阶段配置
   */
  public RenameFilesStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_RENAME_FILES, true);
  }

  /**
   * 获取已提交成功的文件列表。
   * 访问未做同步，调用方需保证线程安全。
   * @return 已提交文件列表的直接引用
   */
  public synchronized  List<FileEntry> getFilesCommitted() {
    return filesCommitted;
  }

  /**
   * 获取已提交文件总大小。
   * @return 总大小，大于等于0
   */
  public synchronized long getTotalFileSize() {
    return totalFileSize;
  }

  /**
   * 执行文件重命名阶段逻辑，完成所有任务输出文件的提交重命名。
   * @param args 三元组，包含(已加载清单数据、本次创建目录集合、结果文件保留的路径数量)
   * @return 作业提交成功数据对象
   * @throws IOException IO操作失败时抛出
   */
  @Override
  protected ManifestSuccessData executeStage(
      Triple<LoadedManifestData, Set<Path>, Integer> args)
      throws IOException {


    final LoadedManifestData manifestData = args.getLeft();
    createdDirectories = args.getMiddle();
    // 创建入口文件IO处理器
    final EntryFileIO entryFileIO = new EntryFileIO(getStageConfig().getConf());


    final ManifestSuccessData success = createManifestOutcome(getStageConfig(),
        OP_STAGE_JOB_COMMIT);

    LOG.info("{}: Executing Manifest Job Commit with {} files",
        getName(), manifestData.getFileCount());

    // 遍历读取入口文件中的所有文件条目
    try (SequenceFile.Reader reader = entryFileIO.createReader(
        manifestData.getEntrySequenceData())) {

      // 使用线程池并行处理每个文件的重命名提交
      TaskPool.foreach(entryFileIO.iterateOver(reader))
          .executeWith(getIOProcessors())
          .stopOnFailure()
          .run(this::commitOneFile);
    }

    // 获取已提交文件列表用于日志和结果输出
    List<FileEntry> committed = getFilesCommitted();
    LOG.info("{}: Files committed: {}. Total size {}",
        getName(), committed.size(), getTotalFileSize());

    // 抽取部分目标路径写入成功清单，满足简单测试需求，避免结果文件过大
    success.setFilenamePaths(
        committed
            .subList(0, Math.min(committed.size(), args.getRight()))
            .stream().map(FileEntry::getDestPath)
            .collect(Collectors.toList()));

    // 标记提交成功
    success.setSuccess(true);

    return success;
  }

  /**
   * 提交单个文件：执行重命名操作，成功后添加到已提交列表。
   * @param entry 待提交的文件条目
   * @throws IOException IO操作失败时抛出
   */
  private void commitOneFile(FileEntry entry) throws IOException {
    updateAuditContext(OP_STAGE_JOB_RENAME_FILES);

    // 向框架报告进度，避免超时
    progress();

    // 判断是否需要删除已存在的目标文件：如果父目录是本次新创建，目标文件肯定不存在，可以跳过删除
    final boolean deleteDest = getStageConfig().getDeleteTargetPaths()
        && !createdDirectories.contains(entry.getDestPath().getParent());
    // 执行文件提交重命名操作
    commitFile(entry, deleteDest);

    // 更新已提交列表和总大小，同步保证线程安全
    synchronized (this) {
      filesCommitted.add(entry);
      totalFileSize += entry.getSize();
    }

  }

}