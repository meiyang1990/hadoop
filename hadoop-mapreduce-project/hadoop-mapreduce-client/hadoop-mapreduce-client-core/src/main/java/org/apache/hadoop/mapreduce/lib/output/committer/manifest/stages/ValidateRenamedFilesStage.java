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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.EntryFileIO;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.OutputValidationException;
import org.apache.hadoop.util.functional.TaskPool;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_VALIDATE_OUTPUT;

/**
 * 文件清单提交器的输出验证阶段，负责扫描所有任务清单文件，
 * 验证所有已重命名到最终输出目录的文件信息与清单记录一致。
 * 每个文件并行执行文件状态校验，验证失败会抛出异常中断提交流程。
 * 最终返回所有验证通过的文件列表，供后续流程使用。
 */
public class ValidateRenamedFilesStage extends
    AbstractJobOrTaskStage<
        Path,
        List<FileEntry>> {

  private static final Logger LOG = LoggerFactory.getLogger(
      ValidateRenamedFilesStage.class);

  /**
   * 存储所有验证通过的已提交文件条目。
   */
  private List<FileEntry> filesCommitted = new ArrayList<>();

  /**
   * 构造验证重命名文件阶段实例。
   * @param stageConfig 阶段配置信息
   */
  public ValidateRenamedFilesStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_VALIDATE_OUTPUT, true);
  }

  /**
   * 线程安全地获取所有验证通过的已提交文件列表。
   * @return 验证通过的文件列表，可能为空
   */
  private synchronized List<FileEntry> getFilesCommitted() {
    return filesCommitted;
  }

  /**
   * 线程安全地添加验证通过的文件条目到已提交列表。
   * @param entry 验证通过的文件条目
   */
  private synchronized void addFileCommitted(FileEntry entry) {
    filesCommitted.add(entry);
  }

  /**
   * 执行验证阶段核心逻辑：读取任务输出清单文件，并行验证每个文件的路径、大小和ETag信息，
   * 收集所有验证通过的文件并返回。
   * @param entryFile 任务输出清单文件路径
   * @return 所有验证通过的文件条目列表
   * @throws IOException 读取清单文件或验证过程中出现IO异常
   */
  @Override
  protected List<FileEntry> executeStage(
      final Path entryFile)
      throws IOException {

    // 创建文件条目IO工具实例
    final EntryFileIO entryFileIO = new EntryFileIO(getStageConfig().getConf());

    // 自动关闭清单文件读取流
    try (SequenceFile.Reader reader = entryFileIO.createReader(entryFile)) {
      // 遍历清单中所有文件条目，使用并行线程池执行验证，遇到失败立即停止
      TaskPool.foreach(entryFileIO.iterateOver(reader))
          .executeWith(getIOProcessors())
          .stopOnFailure()
          .run(this::validateOneFile);

      // 返回所有验证通过的文件列表
      return getFilesCommitted();
    }
  }

  /**
   * 验证单个文件条目：验证文件是否存在、是否为文件类型、长度是否匹配，
   * 若存储系统支持ETag则额外验证ETag一致性。验证通过后添加到已提交列表。
   * @param entry 待验证的文件条目
   * @throws IOException IO操作异常
   * @throws OutputValidationException 验证不通过时抛出
   */
  private void validateOneFile(FileEntry entry) throws IOException {
    // 更新审计上下文，记录当前操作阶段
    updateAuditContext(OP_STAGE_JOB_VALIDATE_OUTPUT);

    // 上报任务进度，避免Hadoop认为任务超时
    progress();
    FileStatus destStatus;
    final Path sourcePath = entry.getSourcePath();
    Path destPath = entry.getDestPath();
    try {
      // 获取目标文件的文件状态
      destStatus = getFileStatus(destPath);

      // 验证目标路径确实是文件，不是目录
      if (!destStatus.isFile()) {
        throw new OutputValidationException(destPath,
            "Expected a file renamed from " + sourcePath
                + "; found " + destStatus);
      }
      final long sourceSize = entry.getSize();
      final long destSize = destStatus.getLen();

      // 获取清单中记录的源文件ETag
      final String sourceEtag = entry.getEtag();
      // 如果存储系统重命名后会保留ETag且清单记录了ETag，验证ETag一致性
      if (getOperations().storePreservesEtagsThroughRenames(destStatus.getPath())
          && isNotBlank(sourceEtag)) {
        final String destEtag = ManifestCommitterSupport.getEtag(destStatus);
        if (!sourceEtag.equals(destEtag)) {
          LOG.warn("Etag of dest file {}: {} does not match that of manifest entry {}",
              destPath, destStatus, entry);
          throw new OutputValidationException(destPath,
              String.format("Expected the file"
                      + " renamed from %s"
                      + " with etag %s and length %s"
                      + " but found a file with etag %s and length %d",
                  sourcePath,
                  sourceEtag,
                  sourceSize,
                  destEtag,
                  destSize));

        }
      }
      // 验证文件长度是否和清单记录一致
      if (destSize != sourceSize) {
        LOG.warn("Length of dest file {}: {} does not match that of manifest entry {}",
            destPath, destStatus, entry);
        throw new OutputValidationException(destPath,
            String.format("Expected the file"
                    + " renamed from %s"
                    + " with length %d"
                    + " but found a file of length %d",
                sourcePath,
                sourceSize,
                destSize));
      }

    } catch (FileNotFoundException e) {
      // 目标文件不存在，验证失败
      throw new OutputValidationException(destPath,
          "Expected a file, but it was not found", e);
    }
    // 所有验证通过，添加到已提交文件列表
    addFileCommitted(entry);
  }

}