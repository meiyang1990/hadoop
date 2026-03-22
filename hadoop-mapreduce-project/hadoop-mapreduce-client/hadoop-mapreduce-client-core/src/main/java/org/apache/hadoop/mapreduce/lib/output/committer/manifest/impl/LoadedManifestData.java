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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl;

import java.io.File;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.LoadManifestsStage;

import static java.util.Objects.requireNonNull;

/**
 * 已加载的任务输出清单数据容器，保存LoadManifestsStage加载完成后输出的数据结构，
 * 供后续文件重命名提交阶段使用，包含需要创建的目录信息和待重命名文件的序列化数据。
 */
public final class LoadedManifestData {

  /**
   * 需要创建的目录条目集合。
   */
  private final Collection<DirEntry> directories;

  /**
   * 待重命名文件条目序列化文件路径，存储在本地文件系统，
   * 文件格式为SequenceFile，key为long类型，value为FileEntry
   */
  private final Path entrySequenceData;

  /**
   * 待重命名文件总数。
   */
  private final int fileCount;

  /**
   * 构造已加载清单数据对象。
   * @param directories 需要创建的目录条目集合
   * @param entrySequenceData 本地文件系统中待重命名文件序列化数据的路径
   * @param fileCount 待重命名文件总数
   */
  public LoadedManifestData(
      final Collection<DirEntry> directories,
      final Path entrySequenceData,
      final int fileCount) {
    this.directories = requireNonNull(directories);
    this.fileCount = fileCount;
    this.entrySequenceData = requireNonNull(entrySequenceData);
  }

  /**
   * 获取需要创建的目录条目集合。
   * @return 需要创建的目录集合
   */
  public Collection<DirEntry> getDirectories() {
    return directories;
  }

  /**
   * 获取待重命名文件总数。
   * @return 文件总数
   */
  public int getFileCount() {
    return fileCount;
  }

  /**
   * 获取待重命名文件序列化数据的路径。
   * @return 序列化文件路径
   */
  public Path getEntrySequenceData() {
    return entrySequenceData;
  }

  /**
   * 将序列化文件路径转换为Java本地File对象。
   * @return 序列化数据文件
   */
  public File getEntrySequenceFile() {
    return new File(entrySequenceData.toUri());
  }

  /**
   * 删除本地存储的序列化数据文件，作业提交完成后清理临时文件。
   * @return 删除操作是否成功
   */
  public boolean deleteEntrySequenceFile() {
    return getEntrySequenceFile().delete();
  }
}