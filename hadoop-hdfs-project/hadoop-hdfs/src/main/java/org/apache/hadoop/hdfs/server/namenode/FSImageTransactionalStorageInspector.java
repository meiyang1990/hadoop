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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 事务式FSImage存储检查器，负责检查NameNode存储目录中的FSImage文件，
 * 提取最新检查点信息和事务ID，用于NameNode启动时加载最新镜像。
 * 支持多存储目录的检查，能够找出所有目录中最新的FSImage文件。
 */
class FSImageTransactionalStorageInspector extends FSImageStorageInspector {
  public static final Logger LOG = LoggerFactory.getLogger(
    FSImageTransactionalStorageInspector.class);

  private boolean needToSave = false;
  private boolean isUpgradeFinalized = true;
  
  final List<FSImageFile> foundImages = new ArrayList<FSImageFile>();
  private long maxSeenTxId = 0;
  
  private final List<Pattern> namePatterns = Lists.newArrayList();

  /**
   * 默认构造函数，仅检查IMAGE类型文件。
   */
  FSImageTransactionalStorageInspector() {
    this(EnumSet.of(NameNodeFile.IMAGE));
  }

  /**
   * 构造函数，指定需要检查的NameNode文件类型集合。
   * @param nnfs 需要检查的NameNode文件类型集合
   */
  FSImageTransactionalStorageInspector(EnumSet<NameNodeFile> nnfs) {
    for (NameNodeFile nnf : nnfs) {
      Pattern pattern = Pattern.compile(nnf.getName() + "_(\\d+)");
      namePatterns.add(pattern);
    }
  }

  /**
   * 匹配文件名是否符合预期格式，提取事务ID。
   * @param name 文件名
   * @return 匹配结果，包含提取的事务ID，不匹配返回null
   */
  private Matcher matchPattern(String name) {
    for (Pattern p : namePatterns) {
      Matcher m = p.matcher(name);
      if (m.matches()) {
        return m;
      }
    }
    return null;
  }

  /**
   * 检查指定存储目录，收集其中的FSImage文件和元数据信息。
   * @param sd 待检查的存储目录
   * @throws IOException 检查过程中IO异常
   */
  @Override
  public void inspectDirectory(StorageDirectory sd) throws IOException {
    // 检查目录是否刚刚格式化，无版本文件说明目录为空
    if (!sd.getVersionFile().exists()) {
      LOG.info("No version file in " + sd.getRoot());
      needToSave |= true;
      return;
    }
    
    // 读取seen_txid文件，获取该目录记录的最大事务ID
    try {
      maxSeenTxId = Math.max(maxSeenTxId, NNStorage.readTransactionIdFile(sd));
    } catch (IOException ioe) {
      LOG.warn("Unable to determine the max transaction ID seen by " + sd, ioe);
      return;
    }

    File currentDir = sd.getCurrentDir();
    File filesInStorage[];
    // 列出当前目录下所有文件
    try {
      filesInStorage = FileUtil.listFiles(currentDir);
    } catch (IOException ioe) {
      LOG.warn("Unable to inspect storage directory " + currentDir,
          ioe);
      return;
    }

    // 遍历所有文件，匹配FSImage格式
    for (File f : filesInStorage) {
      LOG.debug("Checking file " + f);
      String name = f.getName();
      
      // 匹配文件名格式，提取事务ID
      Matcher imageMatch = this.matchPattern(name);
      if (imageMatch != null) {
        // 仅在IMAGE类型目录中处理镜像文件
        if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
          try {
            long txid = Long.parseLong(imageMatch.group(1));
            foundImages.add(new FSImageFile(sd, f, txid));
          } catch (NumberFormatException nfe) {
            LOG.error("Image file " + f + " has improperly formatted " +
                      "transaction ID");
            // 跳过格式错误的文件
          }
        } else {
          LOG.warn("Found image file at " + f + " but storage directory is " +
                   "not configured to contain images.");
        }
      }
    }
    
    // 更新升级是否已完成标记：所有目录都不存在previous目录才表示升级已完成
    isUpgradeFinalized = isUpgradeFinalized && !sd.getPreviousDir().exists();
  }

  /**
   * 获取升级是否已完成状态。
   * @return 所有存储目录升级是否都已完成
   */
  @Override
  public boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }
  
  /**
   * 获取所有存储目录中最新的FSImage文件集合。
   * 多目录下会返回所有目录中拥有相同最大事务ID的FSImage文件。
   * @return 最新FSImage文件列表
   * @throws FileNotFoundException 未找到任何有效FSImage文件
   */
  @Override
  List<FSImageFile> getLatestImages() throws IOException {
    LinkedList<FSImageFile> ret = new LinkedList<FSImageFile>();
    for (FSImageFile img : foundImages) {
      if (ret.isEmpty()) {
        ret.add(img);
      } else {
        FSImageFile cur = ret.getFirst();
        if (cur.txId == img.txId) {
          ret.add(img);
        } else if (cur.txId < img.txId) {
          ret.clear();
          ret.add(img);
        }
      }
    }
    if (ret.isEmpty()) {
      throw new FileNotFoundException("No valid image files found");
    }
    return ret;
  }
  
  /**
   * 获取检查过程中找到的所有FSImage文件。
   * @return 不可变的所有找到的FSImage文件列表
   */
  public List<FSImageFile> getFoundImages() {
    return ImmutableList.copyOf(foundImages);
  }
  
  /**
   * 判断是否需要保存新的FSImage镜像。
   * @return 是否需要保存新镜像
   */
  @Override
  public boolean needToSave() {
    return needToSave;
  }

  /**
   * 获取所有存储目录中记录的最大事务ID。
   * @return 最大事务ID
   */
  @Override
  long getMaxSeenTxId() {
    return maxSeenTxId;
  }
}