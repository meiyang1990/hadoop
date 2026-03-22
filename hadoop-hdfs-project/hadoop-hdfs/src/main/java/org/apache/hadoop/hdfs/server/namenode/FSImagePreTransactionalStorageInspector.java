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
package org.apache.hadoop.hdfs.server.namenode;


import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.io.IOUtils;

/**
 * 文件功能说明：检查旧版本（HDFS-1073之前）事务前格式的FSImage存储目录，
 * 用于兼容升级前的HDFS元数据存储结构，识别并恢复旧格式的fsimage和edits文件。
 * 
 * 旧格式包含以下数据文件：
 *   - fsimage：镜像文件
 *   - fsimage.ckpt：检查点上传过程中的临时镜像文件
 *   - edits：编辑日志文件
 *   - edits.new：日志滚动过程中的临时编辑日志文件
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class FSImagePreTransactionalStorageInspector extends FSImageStorageInspector {
  private static final Logger LOG =
      LoggerFactory.getLogger(FSImagePreTransactionalStorageInspector.class);
  
  // 标记是否存在至少一个存储目录不包含最新的检查点时间
  private boolean hasOutOfDateStorageDirs = false;
  // 标记是否不存在任何previous目录，即升级是否已经完成
  private boolean isUpgradeFinalized = true;
  // 标记恢复完成后是否需要重新保存镜像
  private boolean needToSaveAfterRecovery = false;
  
  // 跟踪拥有最新检查点时间的镜像和编辑日志
  private long latestNameCheckpointTime = Long.MIN_VALUE;
  private long latestEditsCheckpointTime = Long.MIN_VALUE;
  private StorageDirectory latestNameSD = null;
  private StorageDirectory latestEditsSD = null;

  /** 用于检查所有存储目录的检查点时间是否一致 */
  final Set<Long> checkpointTimes = new HashSet<Long>();

  private final List<String> imageDirs = new ArrayList<String>();
  private final List<String> editsDirs = new ArrayList<String>();
  
  /**
   * 检查单个存储目录，收集检查点信息，更新最新检查点记录
   * @param sd 待检查的存储目录
   * @throws IOException 检查过程中的IO异常
   */
  @Override
  void inspectDirectory(StorageDirectory sd) throws IOException {
    // 检查存储目录是否刚格式化，没有版本文件
    if (!sd.getVersionFile().exists()) {
      hasOutOfDateStorageDirs = true;
      return;
    }
    
    boolean imageExists = false;
    boolean editsExists = false;
    
    // 判断当前存储目录是否包含镜像
    if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
      imageExists = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE).exists();        
      imageDirs.add(sd.getRoot().getCanonicalPath());
    }
    
    // 判断当前存储目录是否包含编辑日志
    if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
      editsExists = NNStorage.getStorageFile(sd, NameNodeFile.EDITS).exists();
      editsDirs.add(sd.getRoot().getCanonicalPath());
    }
    
    // 读取当前存储目录的检查点时间
    long checkpointTime = readCheckpointTime(sd);

    // 将检查点时间加入集合，用于后续一致性检查
    checkpointTimes.add(checkpointTime);
    
    // 如果当前目录是镜像目录且检查点时间更新，更新最新镜像记录
    if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE) && 
       (latestNameCheckpointTime < checkpointTime) && imageExists) {
      latestNameCheckpointTime = checkpointTime;
      latestNameSD = sd;
    }
    
    // 如果当前目录是编辑日志目录且检查点时间更新，更新最新编辑日志记录
    if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS) && 
         (latestEditsCheckpointTime < checkpointTime) && editsExists) {
      latestEditsCheckpointTime = checkpointTime;
      latestEditsSD = sd;
    }
    
    // 检查检查点时间是否有效，无效则标记目录过期
    if (checkpointTime <= 0L)
      hasOutOfDateStorageDirs = true;
    
    // 更新升级完成标记：只要有一个previous目录存在，升级就未完成
    isUpgradeFinalized = isUpgradeFinalized && !sd.getPreviousDir().exists();    
  }

  /**
   * 读取指定存储目录的检查点时间
   *
   * @param sd 待检查的存储目录
   * @return 如果文件存在且可读取，返回最后检查点时间；否则返回0L
   * @throws IOException 读取文件过程中的IO异常
   */
  static long readCheckpointTime(StorageDirectory sd) throws IOException {
    File timeFile = NNStorage.getStorageFile(sd, NameNodeFile.TIME);
    long timeStamp = 0L;
    if (timeFile.exists() && FileUtil.canRead(timeFile)) {
      DataInputStream in = new DataInputStream(
          Files.newInputStream(timeFile.toPath()));
      try {
        timeStamp = in.readLong();
        in.close();
        in = null;
      } finally {
        IOUtils.cleanupWithLogger(LOG, in);
      }
    }
    return timeStamp;
  }

  /**
   * 获取升级是否已完成的标记
   * @return true 如果升级已完成（没有previous目录），否则false
   */
  @Override
  boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }
    
  /**
   * 获取所有存储目录中最新的镜像文件列表
   * @return 包含最新镜像文件的列表
   * @throws IOException 找不到镜像或存储不一致时抛出异常
   */
  @Override
  List<FSImageFile> getLatestImages() throws IOException {
    // 检查是否找到有效的镜像和编辑日志目录
    if (latestNameSD == null)
      throw new IOException("Image file is not found in " + imageDirs);
    if (latestEditsSD == null)
      throw new IOException("Edits file is not found in " + editsDirs);
    
    // 检查镜像和编辑日志的检查点时间是否一致
    if (latestNameCheckpointTime > latestEditsCheckpointTime
        && latestNameSD != latestEditsSD
        && latestNameSD.getStorageDirType() == NameNodeDirType.IMAGE
        && latestEditsSD.getStorageDirType() == NameNodeDirType.EDITS) {
      // 罕见异常场景：NameNode分离存储镜像和编辑日志，在保存镜像后、清理编辑日志前崩溃
      LOG.error("This is a rare failure scenario!!!");
      LOG.error("Image checkpoint time " + latestNameCheckpointTime +
                " > edits checkpoint time " + latestEditsCheckpointTime);
      LOG.error("Name-node will treat the image as the latest state of " +
                "the namespace. Old edits will be discarded.");
    } else if (latestNameCheckpointTime != latestEditsCheckpointTime) {
      throw new IOException("Inconsistent storage detected, " +
                      "image and edits checkpoint times do not match. " +
                      "image checkpoint time = " + latestNameCheckpointTime +
                      "edits checkpoint time = " + latestEditsCheckpointTime);
    }

    // 执行崩溃恢复处理，返回是否需要保存
    needToSaveAfterRecovery = doRecovery();
    
    // 构造最新镜像文件对象并返回
    FSImageFile file = new FSImageFile(latestNameSD, 
        NNStorage.getStorageFile(latestNameSD, NameNodeFile.IMAGE),
        HdfsServerConstants.INVALID_TXID);
    LinkedList<FSImageFile> ret = new LinkedList<FSImageFile>();
    ret.add(file);
    return ret;
  }

  /**
   * 判断恢复完成后是否需要重新保存命名空间
   * @return true 需要保存，false 不需要保存
   */
  @Override
  boolean needToSave() {
    return hasOutOfDateStorageDirs ||
      checkpointTimes.size() != 1 ||
      latestNameCheckpointTime > latestEditsCheckpointTime ||
      needToSaveAfterRecovery;
  }
  
  /**
   * 执行崩溃恢复，处理检查点过程中未完成的临时文件
   * @return true 恢复后需要重新保存镜像，false 不需要保存
   * @throws IOException 恢复过程中删除或重命名文件失败时抛出异常
   */
  boolean doRecovery() throws IOException {
    LOG.debug(
        "Performing recovery in "+ latestNameSD + " and " + latestEditsSD);
      
    boolean needToSave = false;
    File curFile =
      NNStorage.getStorageFile(latestNameSD, NameNodeFile.IMAGE);
    File ckptFile =
      NNStorage.getStorageFile(latestNameSD, NameNodeFile.IMAGE_NEW);
    
    // 处理检查点过程中崩溃的情况，存在未完成的新镜像文件
    if (ckptFile.exists()) {
      needToSave = true;
      if (NNStorage.getStorageFile(latestEditsSD, NameNodeFile.EDITS_NEW)
          .exists()) {
        // 如果编辑日志新文件还存在，说明检查点未完成，删除不完整的新镜像
        if (!ckptFile.delete()) {
          throw new IOException("Unable to delete " + ckptFile);
        }
      } else {
        // 如果编辑日志已经完成滚动，说明检查点已大部分完成，将新镜像重命名为正式镜像
        // 此处处理Windows上renameTo覆盖失败问题：先删除目标文件再重命名
        if (!ckptFile.renameTo(curFile)) {
          if (!curFile.delete())
            LOG.warn("Unable to delete dir " + curFile + " before rename");
          if (!ckptFile.renameTo(curFile)) {
            throw new IOException("Unable to rename " + ckptFile +
                                  " to " + curFile);
          }
        }
      }
    }
    return needToSave;
  }
  
  /**
   * 获取指定存储目录中的所有编辑日志文件（包含EDITS和存在的EDITS_NEW）
   * @param sd 待获取的存储目录
   * @return 编辑日志文件列表
   */
  static List<File> getEditsInStorageDir(StorageDirectory sd) {
    ArrayList<File> files = new ArrayList<File>();
    File edits = NNStorage.getStorageFile(sd, NameNodeFile.EDITS);
    assert edits.exists() : "Expected edits file at " + edits;
    files.add(edits);
    File editsNew = NNStorage.getStorageFile(sd, NameNodeFile.EDITS_NEW);
    if (editsNew.exists()) {
      files.add(editsNew);
    }
    return files;
  }
  
  /**
   * 获取最新的编辑日志文件列表
   * @return 最新编辑日志文件列表，如果镜像更新则返回空列表
   */
  private List<File> getLatestEditsFiles() {
    if (latestNameCheckpointTime > latestEditsCheckpointTime) {
      // 镜像已经比编辑日志新，不需要加载旧编辑日志
      LOG.debug(
          "Name checkpoint time is newer than edits, not loading edits.");
      return Collections.emptyList();
    }
    
    return getEditsInStorageDir(latestEditsSD);
  }
  
  /**
   * 获取最大可见事务ID，旧格式不支持事务ID，返回0
   * @return 固定返回0L
   */
  @Override
  long getMaxSeenTxId() {
    return 0L;
  }

  /**
   * 静态工具方法，检查旧格式存储并获取所有编辑日志输入流
   * @param storage NameNode存储对象
   * @return 编辑日志输入流可迭代对象
   * @throws IOException 检查或读取过程中IO异常
   */
  static Iterable<EditLogInputStream> getEditLogStreams(NNStorage storage)
      throws IOException {
    FSImagePreTransactionalStorageInspector inspector 
      = new FSImagePreTransactionalStorageInspector();
    storage.inspectStorageDirs(inspector);

    List<EditLogInputStream> editStreams = new ArrayList<EditLogInputStream>();
    for (File f : inspector.getLatestEditsFiles()) {
      editStreams.add(new EditLogFileInputStream(f));
    }
    return editStreams;
  }
}