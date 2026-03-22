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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.EntryStatus;
import org.apache.hadoop.util.functional.TaskPool;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_DELETE;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.measureDurationOfInvocation;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_CREATE_DIRECTORIES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_DELETE_FILE_UNDER_DESTINATION;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_MKDIRS_RETURNED_FALSE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_PREPARE_DIR_ANCESTORS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CREATE_TARGET_DIRS;
import static org.apache.hadoop.util.OperationDuration.humanTime;

/**
 * 文件级说明：Manifest提交器输出目录准备阶段，高效并行创建输出目录树。
 * 继承传统FileOutputCommitter的处理逻辑：在需要创建目录的路径如果存在文件，需要先删除该文件。
 * 合并所有任务分片的目录需求，去重后按目录层级并行处理，提升对象存储下的创建效率。
 * 输入是聚合后的所有待创建目录信息，且已经提前探测过目录存在性与状态，输出为创建结果。
 */
public class CreateOutputDirectoriesStage extends
    AbstractJobOrTaskStage<
        Collection<DirEntry>,
        CreateOutputDirectoriesStage.Result> {

  private static final Logger LOG = LoggerFactory.getLogger(
      CreateOutputDirectoriesStage.class);

  /**
   * 存储目录路径与对应状态的并发映射表。
   */
  private final Map<Path, DirMapState> dirMap = new ConcurrentHashMap<>();

  /**
   * 存储实际创建成功的目录列表，用于结果返回。
   */
  private final List<Path> createdDirectories = new ArrayList<>();

  /**
   * 构造创建输出目录阶段实例，初始化目录映射表。
   * @param stageConfig 阶段配置信息
   */
  public CreateOutputDirectoriesStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_CREATE_TARGET_DIRS, true);
    // 将根输出目录加入目录映射，作业初始化阶段已经创建完成
    dirMap.put(getDestinationDir(), DirMapState.dirWasCreated);
  }

  @Override
  /**
   * 执行创建输出目录树阶段主流程。
   * @param manifestDirs 所有聚合后的待创建目录条目
   * @return 创建结果，包含创建的目录集合与所有目录状态映射
   * @throws IOException IO操作异常
   */
  protected Result executeStage(
      final Collection<DirEntry> manifestDirs)
      throws IOException {

    final List<Path> directories = createAllDirectories(manifestDirs);
    LOG.info("{}: Created {} directories", getName(), directories.size());
    return new Result(new HashSet<>(directories), dirMap);
  }

  /**
   * 处理所有待创建目录，分类叶子目录与父目录，删除冲突文件，并行创建叶子目录。
   * @param manifestDirs 来自所有任务manifest的目录条目集合
   * @return 实际创建成功的目录路径列表
   * @throws IOException IO操作异常
   */
  private List<Path> createAllDirectories(final Collection<DirEntry> manifestDirs)
      throws IOException {

    // 保存叶子目录（最底层需要创建的目录）
    final Map<Path, DirEntry> leaves = new HashMap<>();
    // 保存父目录（不需要显式创建，创建叶子目录时会自动生成）
    final Map<Path, DirEntry> parents = new HashMap<>();
    // 保存路径存在文件、需要先删除才能创建目录的路径集合
    final Set<Path> filesToDelete = new HashSet<>();

    // 按目录层级排序，父目录在前，叶子目录在后
    List<DirEntry> destDirectories = new ArrayList<>(manifestDirs);

    Collections.sort(destDirectories, Comparator.comparingInt(DirEntry::getLevel));
    // 遍历所有目录条目进行分类整理
    for (DirEntry entry: destDirectories) {
      final Path path = entry.getDestPath();
      if (!leaves.containsKey(path)) {
        leaves.put(path, entry);

        // 如果当前路径是文件，加入待删除集合
        if (entry.getStatus() == EntryStatus.file) {
          filesToDelete.add(path);
        }
        final Path parent = path.getParent();
        if (parent != null && leaves.containsKey(parent)) {
          // 父目录已经在叶子集合中，需要移动到父目录集合，不再显式创建
          parents.put(parent, leaves.remove(parent));
        }
      }
    }

    // 分类完成，现在先删除所有冲突文件
    deleteFiles(filesToDelete);

    // 统计目录数量并输出日志
    final int createCount = leaves.size();
    LOG.info("Preparing {} directory/directories; {} parent dirs implicitly created."
            + " Files deleted: {}",
        createCount, parents.size(), filesToDelete.size());

    // 使用线程池并行创建所有叶子目录，统计操作耗时
    Duration d = measureDurationOfInvocation(getIOStatistics(), OP_CREATE_DIRECTORIES, () ->
        TaskPool.foreach(leaves.values())
            .executeWith(getIOProcessors(createCount))
            .onFailure(this::reportMkDirFailure)
            .stopOnFailure()
            .run(this::createOneDirectory));
    LOG.info("Time to prepare directories {}", humanTime(d.toMillis()));
    return createdDirectories;
  }

  /**
   * 记录创建失败次数的计数器。
   */
  private final AtomicInteger failureCount = new AtomicInteger();

  /**
   * 报告单个目录创建失败，记录日志并递增失败计数。
   * @param dirEntry 创建失败的目录条目
   * @param e 捕获到的异常
   */
  private void reportMkDirFailure(DirEntry dirEntry, Exception e) {
    Path path = dirEntry.getDestPath();
    final int count = failureCount.incrementAndGet();
    LOG.warn("{}: mkdir failure #{} Failed to create directory \"{}\": {}",
        getName(), count, path, e.toString());
    LOG.debug("{}: Full exception details",
        getName(), e);
  }

  /**
   * 并行删除所有需要创建目录位置上已存在的文件。
   * @param filesToDelete 需要删除的文件路径集合
   * @throws IOException IO操作异常
   */
  private void deleteFiles(final Set<Path> filesToDelete)
      throws IOException {

    final int size = filesToDelete.size();
    if (size == 0) {
      // 没有需要删除的文件，直接返回
      return;
    }
    LOG.info("{}: Directory entries containing files to delete: {}", getName(), size);
    // 使用线程池并行删除，统计操作耗时
    Duration d = measureDurationOfInvocation(getIOStatistics(),
        OP_PREPARE_DIR_ANCESTORS, () ->
            TaskPool.foreach(filesToDelete)
                .executeWith(getIOProcessors(size))
                .stopOnFailure()
                .run(dir -> {
                  updateAuditContext(OP_PREPARE_DIR_ANCESTORS);
                  deleteDirWithFile(dir);
                }));
    LOG.info("Time to delete files {}", humanTime(d.toMillis()));
  }

  /**
   * 删除指定路径上的文件，更新目录状态。
   * @param dir 需要删除文件的路径
   * @throws IOException IO操作异常
   */
  private void deleteDirWithFile(Path dir) throws IOException {
    // 上报作业进度
    progress();
    LOG.info("{}: Deleting file {}", getName(), dir);
    deleteFile(dir, OP_DELETE);
    // 更新目录状态为文件已删除
    addToDirectoryMap(dir, DirMapState.fileNowDeleted);
  }


  /**
   * 创建单个目录，根据探测结果处理不同情况，更新状态。
   * @param dirEntry 待创建目录条目
   * @throws PathIOException 多次尝试后仍无法创建目录
   * @throws IOException 其他IO异常
   */
  private void createOneDirectory(final DirEntry dirEntry) throws IOException {
    // 上报作业进度
    progress();
    final Path dir = dirEntry.getDestPath();
    updateAuditContext(OP_STAGE_JOB_CREATE_TARGET_DIRS);
    // 尝试创建目录，获取操作结果状态
    final DirMapState state = maybeCreateOneDirectory(dirEntry);
    switch (state) {
    case dirFoundInStore:
      addToDirectoryMap(dir, state);
      break;
    case dirWasCreated:
    case dirCreatedOnSecondAttempt:
      addCreatedDirectory(dir);
      addToDirectoryMap(dir, state);
      break;
    default:
      break;
    }

  }


  /**
   * 尝试高效健壮地创建单个目录，支持并发创建，处理创建失败的重试与恢复。
   * @param dirEntry 待创建目录条目
   * @return 操作结果状态，标识目录是已存在、本次创建还是重试创建
   * @throws PathIOException 多次尝试后仍无法创建目录
   * @throws IOException 其他IO异常
   */
  private DirMapState maybeCreateOneDirectory(DirEntry dirEntry) throws IOException {
    final EntryStatus status = dirEntry.getStatus();
    if (status == EntryStatus.dir) {
      // 已经存在目录，直接返回状态
      return DirMapState.dirFoundInStore;
    }
    // 目录已经在任务提交阶段创建完成
    if (status == EntryStatus.created_dir) {
      return DirMapState.dirWasCreated;
    }

    // 当前路径不存在目录：要么是文件已删除，要么是探测未找到，需要创建
    final Path path = dirEntry.getDestPath();

    LOG.info("Creating directory {}", path);

    try {
      if (mkdirs(path, false)) {
        // 第一次创建成功，直接返回
        return DirMapState.dirWasCreated;
      }
      // 创建返回false，统计计数
      getIOStatistics().incrementCounter(OP_MKDIRS_RETURNED_FALSE);

      LOG.info("{}: mkdirs({}) returned false, attempting to recover",
          getName(), path);
    } catch (IOException e) {
      // 创建抛出异常，可能是文件已存在等原因，记录日志进入恢复流程
      LOG.info("{}: mkdir({}) raised exception {}", getName(), path, e.toString());
      LOG.debug("{}: Mkdir stack", getName(), e);
    }

    // 恢复流程：检查文件系统当前状态，可能其他并发进程已经创建了目录
    FileStatus st = getFileStatusOrNull(path);
    if (st != null) {
      if (!st.isDirectory()) {
        // 路径存在但还是文件，删除该文件后重试创建
        LOG.info("{}: Deleting file where a directory should go: {}",
            getName(), st);
        deleteFile(path, OP_DELETE_FILE_UNDER_DESTINATION);
      } else {
        // 路径已经是目录，虽然创建失败但实际存在，直接返回
        LOG.warn("{}: Even though mkdirs({}) failed, there is now a directory there",
            getName(), path);
        return DirMapState.dirFoundInStore;
      }
    } else {
      // 路径不存在，mkdir失败但没有阻碍创建，继续重试
      LOG.warn("{}: Although mkdirs({}) returned false, there's nothing at that path to prevent it",
          getName(), path);

    }

    // 第二次尝试创建目录，失败则抛出异常
    if (!mkdirs(path, false)) {

      // 第二次创建仍然失败，增加统计计数
      getIOStatistics().incrementCounter(OP_MKDIRS_RETURNED_FALSE);

      // 校验目录必须存在，不存在则抛出异常
      directoryMustExist("Creating directory ", path);
    }

    // 第二次尝试成功，返回对应状态
    return DirMapState.dirCreatedOnSecondAttempt;

  }

  /**
   * 将创建成功的目录加入结果列表，线程安全。
   * @param dir 创建成功的目录路径
   */
  private void addCreatedDirectory(final Path dir) {
    synchronized (createdDirectories) {
      createdDirectories.add(dir);
    }
  }

  /**
   * 将目录状态加入映射表，如果已存在则不覆盖。
   * @param dir 目录路径
   * @param state 目录状态
   */
  private void addToDirectoryMap(final Path dir,
      DirMapState state) {
    if (!dirMap.containsKey(dir)) {
      dirMap.put(dir, state);
    }
  }


  /**
   * 创建输出目录阶段的结果类，封装创建结果信息。
   */
  public static final class Result {

    /** 创建成功的目录集合 */
    private final Set<Path> createdDirectories;

    /** 所有目录路径与对应状态的映射表 */
    private final Map<Path, DirMapState> dirMap;

    /**
     * 构造结果实例。
     * @param createdDirectories 创建成功的目录集合
     * @param dirMap 目录状态映射表
     */
    public Result(Set<Path> createdDirectories,
        Map<Path, DirMapState> dirMap) {
      this.createdDirectories = requireNonNull(createdDirectories);
      this.dirMap = requireNonNull(dirMap);
    }

    public Set<Path> getCreatedDirectories() {
      return createdDirectories;
    }

    public Map<Path, DirMapState> getDirMap() {
      return dirMap;
    }

    @Override
    public String toString() {
      return "Result{" +
          "directory count=" + createdDirectories.size() +
          '}';
    }
  }

  /**
   * 目录状态枚举，标识目录在准备阶段的处理结果。
   */
  public enum DirMapState {
    /** 目录已经在文件系统中存在 */
    dirFoundInStore,
    /** 目录已经在映射表中存在 */
    dirFoundInMap,
    /** 本次流程第一次尝试创建成功 */
    dirWasCreated,
    /** 第二次尝试才创建成功 */
    dirCreatedOnSecondAttempt,
    /** 原有文件已删除，可以创建目录 */
    fileNowDeleted,
    /** 祖先目录已经是目录或不存在 */
    ancestorWasDirOrMissing,
    /** 父路径不是文件，无需处理 */
    parentWasNotFile,
    /** 作为已创建目录的父目录，已隐式创建 */
    parentOfCreatedDir
  }

}