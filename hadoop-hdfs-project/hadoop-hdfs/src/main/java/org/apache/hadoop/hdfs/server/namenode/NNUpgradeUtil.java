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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;

import org.apache.hadoop.util.Preconditions;

/**
 * NameNode版本升级/回滚工具类，提供HDFS元数据版本升级、确认升级、回滚的核心工具方法
 */
public abstract class NNUpgradeUtil {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(NNUpgradeUtil.class);
  
  /**
   * 检查当前存储目录是否支持回滚到指定版本
   * @param sd 目标存储目录
   * @param storage 当前版本存储信息
   * @param prevStorage 上一版本存储信息
   * @param targetLayoutVersion 期望回滚到的目标布局版本
   * @return 如果支持回滚返回true，否则返回false
   * @throws IOException 读取存储信息时发生IO异常
   */
  static boolean canRollBack(StorageDirectory sd, StorageInfo storage,
      StorageInfo prevStorage, int targetLayoutVersion) throws IOException {
    File prevDir = sd.getPreviousDir();
    if (!prevDir.exists()) {  // use current directory then
      LOG.info("Storage directory " + sd.getRoot()
               + " does not contain previous fs state.");
      // 读取并校验和其他目录的一致性
      storage.readProperties(sd);
      return false;
    }

    // 读取并校验上一版本目录的一致性
    prevStorage.readPreviousVersionProperties(sd);

    if (prevStorage.getLayoutVersion() != targetLayoutVersion) {
      throw new IOException(
        "Cannot rollback to storage version " +
        prevStorage.getLayoutVersion() +
        " using this version of the NameNode, which uses storage version " +
        targetLayoutVersion + ". " +
        "Please use the previous version of HDFS to perform the rollback.");
    }
    
    return true;
  }

  /**
   * 完成版本升级确认，删除升级前的旧版本数据，执行后无法再回滚升级
   * @param sd 需要确认升级的存储目录
   * @throws IOException 处理目录/文件时发生IO异常
   */
  static void doFinalize(StorageDirectory sd) throws IOException {
    File prevDir = sd.getPreviousDir();
    if (!prevDir.exists()) { // already discarded
      LOG.info("Directory " + prevDir + " does not exist.");
      LOG.info("Finalize upgrade for " + sd.getRoot()+ " is not required.");
      return;
    }
    LOG.info("Finalizing upgrade of storage directory " + sd.getRoot());
    Preconditions.checkState(sd.getCurrentDir().exists(),
        "Current directory must exist.");
    final File tmpDir = sd.getFinalizedTmp();
    // 先将旧版本目录重命名为临时目录，再删除
    NNStorage.rename(prevDir, tmpDir);
    NNStorage.deleteDir(tmpDir);
    LOG.info("Finalize upgrade for " + sd.getRoot()+ " is complete.");
  }
  
  /**
   * 执行升级前的准备工作，所有存储目录准备成功后才会开始实际升级
   * 将当前目录重命名为previous.tmp，创建新的空当前目录，并硬链接 edits 文件到新目录
   * @param conf 配置对象，用于创建输出流
   * @param sd 需要准备升级的存储目录
   * @throws IOException 执行目录操作/文件链接时发生IO异常
   */
  static void doPreUpgrade(Configuration conf, StorageDirectory sd)
      throws IOException {
    LOG.info("Starting upgrade of storage directory " + sd.getRoot());

    // 将当前目录重命名为临时目录
    renameCurToTmp(sd);

    final Path curDir = sd.getCurrentDir().toPath();
    final Path tmpDir = sd.getPreviousTmp().toPath();

    Files.walkFileTree(tmpDir,
      /* 不跟随符号链接 */ Collections.<FileVisitOption>emptySet(),
        1, new SimpleFileVisitor<Path>() {

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {

            String name = file.getFileName().toString();

            if (Files.isRegularFile(file)
                && name.startsWith(NNStorage.NameNodeFile.EDITS.getName())) {
              // 对edits文件创建硬链接到新的当前目录，避免复制节省空间
              Path newFile = curDir.resolve(name);
              Files.createLink(newFile, file);
            }

            return super.visitFile(file, attrs);
          }
        }
    );
  }

  /**
   * 将当前已有目录重命名为previous.tmp，新建空的当前目录，用于升级准备
   * @param sd 目标存储目录
   * @throws IOException 目录重命名/创建时发生IO异常，或者前置检查不通过
   */
  public static void renameCurToTmp(StorageDirectory sd) throws IOException {
    File curDir = sd.getCurrentDir();
    File prevDir = sd.getPreviousDir();
    final File tmpDir = sd.getPreviousTmp();

    Preconditions.checkState(curDir.exists(),
        "Current directory must exist for preupgrade.");
    Preconditions.checkState(!prevDir.exists(),
        "Previous directory must not exist for preupgrade.");
    Preconditions.checkState(!tmpDir.exists(),
        "Previous.tmp directory must not exist for preupgrade."
            + "Consider restarting for recovery.");

    // 将当前目录重命名为临时目录
    NNStorage.rename(curDir, tmpDir);

    if (!curDir.mkdir()) {
      throw new IOException("Cannot create directory " + curDir);
    }
  }
  
  /**
   * 执行实际的版本升级，写入新版本存储信息，将临时目录重命名为previous目录保留回滚能力
   * @param sd 目标存储目录
   * @param storage 升级后的新版本存储信息
   * @throws IOException 写入信息/重命名目录时发生IO异常
   */
  public static void doUpgrade(StorageDirectory sd, Storage storage)
      throws IOException {
    LOG.info("Performing upgrade of storage directory " + sd.getRoot());
    try {
      // 写入版本信息文件，fsimage生成不会自动生成版本文件
      storage.writeProperties(sd);

      File prevDir = sd.getPreviousDir();
      File tmpDir = sd.getPreviousTmp();
      Preconditions.checkState(!prevDir.exists(),
          "previous directory must not exist for upgrade.");
      Preconditions.checkState(tmpDir.exists(),
          "previous.tmp directory must exist for upgrade.");

      // 将升级前临时目录重命名为previous，保留用于回滚
      NNStorage.rename(tmpDir, prevDir);
    } catch (IOException ioe) {
      LOG.error("Unable to rename temp to previous for " + sd.getRoot(), ioe);
      throw ioe;
    }
  }

  /**
   * 执行版本回滚，删除当前新版本目录，将旧版本目录重命名为当前目录恢复状态
   * @param sd 需要回滚的存储目录
   * @throws IOException 目录操作时发生IO异常，或者前置检查不通过
   */
  static void doRollBack(StorageDirectory sd)
      throws IOException {
    File prevDir = sd.getPreviousDir();
    if (!prevDir.exists()) {
      return;
    }

    File tmpDir = sd.getRemovedTmp();
    Preconditions.checkState(!tmpDir.exists(),
        "removed.tmp directory must not exist for rollback."
            + "Consider restarting for recovery.");
    // 将当前新版本目录重命名为临时目录
    File curDir = sd.getCurrentDir();
    Preconditions.checkState(curDir.exists(),
        "Current directory must exist for rollback.");

    NNStorage.rename(curDir, tmpDir);
    // 将旧版本目录重命名为当前目录，完成回滚
    NNStorage.rename(prevDir, curDir);

    // 删除存储新版本的临时目录
    NNStorage.deleteDir(tmpDir);
    LOG.info("Rollback of " + sd.getRoot() + " is complete.");
  }
  
}