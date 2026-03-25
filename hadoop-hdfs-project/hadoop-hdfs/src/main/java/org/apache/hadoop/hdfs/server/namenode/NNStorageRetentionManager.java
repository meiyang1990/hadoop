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
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector.FSImageFile;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;

/**
 * 文件级注释：NameNode存储目录的检查点和编辑日志保留策略管理器，负责清理过期的存储文件
 * 
 * NNStorageRetentionManager负责检查NameNode的存储目录，并对检查点和编辑日志执行保留策略。
 * 它将实际的文件删除操作委托给StoragePurger实现，删除器可以选择直接删除文件，或将文件备份到其他存储供后续分析。
 */
public class NNStorageRetentionManager {
  
  private final int numCheckpointsToRetain;
  private final long numExtraEditsToRetain;
  private final int maxExtraEditsSegmentsToRetain;
  private static final Logger LOG = LoggerFactory.getLogger(
      NNStorageRetentionManager.class);
  private final NNStorage storage;
  private final StoragePurger purger;
  private final LogsPurgeable purgeableLogs;

  /**
   * 构造函数，从配置加载保留参数，初始化保留管理器
   * @param conf Hadoop配置对象
   * @param storage NameNode存储对象
   * @param purgeableLogs 可清理的编辑日志接口
   * @param purger 存储文件删除器
   */
  public NNStorageRetentionManager(
      Configuration conf,
      NNStorage storage,
      LogsPurgeable purgeableLogs,
      StoragePurger purger) {
    // 从配置读取需要保留的检查点数量
    this.numCheckpointsToRetain = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_KEY,
        DFSConfigKeys.DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_DEFAULT);
    // 从配置读取需要额外保留的编辑日志事务数
    this.numExtraEditsToRetain = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY,
        DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_DEFAULT);
    // 从配置读取最多额外保留的编辑日志段数量
    this.maxExtraEditsSegmentsToRetain = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_EXTRA_EDITS_SEGMENTS_RETAINED_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_EXTRA_EDITS_SEGMENTS_RETAINED_DEFAULT);
    // 参数校验：必须至少保留一个检查点
    Preconditions.checkArgument(numCheckpointsToRetain > 0,
        "Must retain at least one checkpoint");
    // 参数校验：额外保留的编辑日志数不能为负
    Preconditions.checkArgument(numExtraEditsToRetain >= 0,
        DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY +
        " must not be negative");
    // 参数校验：最大额外保留的编辑日志段数不能为负
    Preconditions.checkArgument(maxExtraEditsSegmentsToRetain >= 0,
        DFSConfigKeys.DFS_NAMENODE_MAX_EXTRA_EDITS_SEGMENTS_RETAINED_KEY +
        " must not be negative");
    
    this.storage = storage;
    this.purgeableLogs = purgeableLogs;
    this.purger = purger;
  }
  
  /**
   * 构造函数，使用默认的删除器实现（直接删除文件）
   * @param conf Hadoop配置对象
   * @param storage NameNode存储对象
   * @param purgeableLogs 可清理的编辑日志接口
   */
  public NNStorageRetentionManager(Configuration conf, NNStorage storage,
      LogsPurgeable purgeableLogs) {
    this(conf, storage, purgeableLogs, new DeletionStoragePurger());
  }

  /**
   * 清理指定类型检查点，删除所有事务ID大于指定值的检查点
   * @param nnf 要清理的NameNode文件类型
   * @throws IOException 检查存储目录时抛出IO异常
   */
  void purgeCheckpoints(NameNodeFile nnf) throws IOException {
    purgeCheckpoinsAfter(nnf, -1);
  }

  /**
   * 清理指定类型且事务ID大于阈值的检查点
   * @param nnf 要清理的NameNode文件类型
   * @param fromTxId 事务ID阈值，删除大于该值的检查点
   * @throws IOException 检查存储目录时抛出IO异常
   */
  void purgeCheckpoinsAfter(NameNodeFile nnf, long fromTxId)
      throws IOException {
    // 创建检查点存储检查器
    FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector(EnumSet.of(nnf));
    // 遍历所有存储目录收集检查点信息
    storage.inspectStorageDirs(inspector);
    // 删除所有事务ID大于阈值的检查点
    for (FSImageFile image : inspector.getFoundImages()) {
      if (image.getCheckpointTxId() > fromTxId) {
        purger.purgeImage(image);
      }
    }
  }

  /**
   * 根据保留策略清理过期检查点和旧编辑日志
   * @param nnf 要处理的NameNode文件类型
   * @throws IOException IO操作异常
   */
  void purgeOldStorage(NameNodeFile nnf) throws IOException {
    // 创建存储检查器检查指定类型的镜像文件
    FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector(EnumSet.of(nnf));
    // 遍历存储目录收集镜像信息
    storage.inspectStorageDirs(inspector);

    // 计算需要保留的最旧检查点事务ID
    long minImageTxId = getImageTxIdToRetain(inspector);
    // 清理所有事务ID小于该值的旧检查点
    purgeCheckpointsOlderThan(inspector, minImageTxId);
    
    // 回滚镜像不需要清理编辑日志，直接返回
    if (nnf == NameNodeFile.IMAGE_ROLLBACK) {
      // do not purge edits for IMAGE_ROLLBACK.
      return;
    }

    // 如果我们保留fsimage_N，那么所有大于N的事务都需要保留
    // 可以删除小于N+1的所有日志，因为fsimage_N已经包含了到N为止的所有状态
    // 额外保留一定数量的旧日志，为HA场景下备用节点同步提供缓冲

    // 计算需要保留的最小事务ID
    long minimumRequiredTxId = minImageTxId + 1;
    // 计算可以清理的最大事务ID：最小需要保留的事务ID减去额外保留的事务数
    long purgeLogsFrom = Math.max(0, minimumRequiredTxId - numExtraEditsToRetain);
    
    // 获取所有从purgeLogsFrom开始的编辑日志输入流
    ArrayList<EditLogInputStream> editLogs = new ArrayList<EditLogInputStream>();
    purgeableLogs.selectInputStreams(editLogs, purgeLogsFrom, false, false);
    // 按起始事务ID、结束事务ID对编辑日志排序
    Collections.sort(editLogs, new Comparator<EditLogInputStream>() {
      @Override
      public int compare(EditLogInputStream a, EditLogInputStream b) {
        return ComparisonChain.start()
            .compare(a.getFirstTxId(), b.getFirstTxId())
            .compare(a.getLastTxId(), b.getLastTxId())
            .result();
      }
    });

    // 移除所有必须保留的编辑日志（起始事务ID大于等于最小需要保留的事务ID）
    while (editLogs.size() > 0 &&
        editLogs.get(editLogs.size() - 1).getFirstTxId() >= minimumRequiredTxId) {
      editLogs.remove(editLogs.size() - 1);
    }
    
    // 如果保留的日志段数量超过配置上限，调整清理起点，删除最旧的多余日志段
    while (editLogs.size() > maxExtraEditsSegmentsToRetain) {
      purgeLogsFrom = editLogs.get(0).getLastTxId() + 1;
      editLogs.remove(0);
    }
    
    // 校验：确保不会清理需要保留的日志
    if (purgeLogsFrom > minimumRequiredTxId) {
      throw new AssertionError("Should not purge more edits than required to "
          + "restore: " + purgeLogsFrom + " should be <= "
          + minimumRequiredTxId);
    }
    
    // 清理所有早于purgeLogsFrom的编辑日志
    purgeableLogs.purgeLogsOlderThan(purgeLogsFrom);
  }
  
  /**
   * 清理所有事务ID小于阈值的旧检查点
   * @param inspector 检查结果对象，包含所有找到的检查点
   * @param minTxId 最小需要保留的事务ID，小于该值会被清理
   */
  private void purgeCheckpointsOlderThan(
      FSImageTransactionalStorageInspector inspector,
      long minTxId) {
    for (FSImageFile image : inspector.getFoundImages()) {
      if (image.getCheckpointTxId() < minTxId) {
        purger.purgeImage(image);
      }
    }
  }

  /**
   * 计算需要保留的最旧检查点的事务ID
   * @param inspector 已完成存储检查的检查器对象
   * @return 需要保留的最旧检查点事务ID
   */
  private long getImageTxIdToRetain(
      FSImageTransactionalStorageInspector inspector) {

    final List<FSImageFile> images = inspector.getFoundImages();
    // 没有找到任何镜像，返回0
    if (images.isEmpty()) {
      return 0L;
    }

    // 将检查点事务ID按从大到小排序
    TreeSet<Long> imageTxIds = new TreeSet<>(Collections.reverseOrder());
    for (FSImageFile image : images) {
      imageTxIds.add(image.getCheckpointTxId());
    }

    // 转换为列表，计算需要保留的最旧检查点事务ID
    List<Long> imageTxIdsList = Lists.newArrayList(imageTxIds);
    int toRetain = Math.min(numCheckpointsToRetain, imageTxIdsList.size());
    long minTxId = imageTxIdsList.get(toRetain - 1);
    LOG.info("Going to retain {} images with txid >= {}", toRetain, minTxId);
    return minTxId;
  }

  /**
   * 存储文件清理器接口，负责处理过期检查点和编辑日志的清理
   */
  interface StoragePurger {
    /**
     * 清理编辑日志文件
     * @param log 要清理的编辑日志对象
     */
    void purgeLog(EditLogFile log);
    /**
     * 清理检查点镜像文件
     * @param image 要清理的检查点镜像对象
     */
    void purgeImage(FSImageFile image);
    /**
     * 标记不完整的进行中编辑日志为过期
     * @param log 要标记的编辑日志对象
     */
    void markStale(EditLogFile log);
  }
  
  /**
   * 直接删除实现的存储清理器，直接删除过期文件
   */
  static class DeletionStoragePurger implements StoragePurger {
    @Override
    public void purgeLog(EditLogFile log) {
      LOG.info("Purging old edit log {}", log);
      deleteOrWarn(log.getFile());
    }

    @Override
    public void purgeImage(FSImageFile image) {
      LOG.info("Purging old image {}", image);
      deleteOrWarn(image.getFile());
      // 同时删除对应的MD5校验文件
      deleteOrWarn(MD5FileUtils.getDigestFileForFile(image.getFile()));
    }

    /**
     * 删除文件，删除失败仅记录警告不抛出异常，下次会再次尝试删除
     * @param file 要删除的文件
     */
    private static void deleteOrWarn(File file) {
      if (!file.delete()) {
        // It's OK if we fail to delete something -- we'll catch it
        // next time we swing through this directory.
        LOG.warn("Could not delete {}", file);
      }      
    }

    @Override
    public void markStale(EditLogFile log){
      try {
        // 将不完整的进行中编辑日志重命名为stale后缀
        log.moveAsideStaleInprogressFile();
      } catch (IOException e) {
        // It is ok to just log the rename failure and go on, we will try next
        // time just as with deletions.
        LOG.warn("Could not mark {} as stale", log, e);
      }
    }
  }

  /**
   * 清理过期的离线镜像查看器（OIV）旧镜像文件，保留最新的配置数量
   * @param dir OIV镜像存储目录路径
   * @param txid 不使用，当前实现按文件数量保留
   */
  void purgeOldLegacyOIVImages(String dir, long txid) {
    File oivImageDir = new File(dir);
    final String oivImagePrefix = NameNodeFile.IMAGE_LEGACY_OIV.getName();
    String filesInStorage[];

    // 过滤出符合OIV镜像命名规则的文件
    filesInStorage = oivImageDir.list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.matches(oivImagePrefix + "_(\\d+)");
      }
    });

    // 文件数量小于等于需要保留的数量，不需要清理
    if (filesInStorage != null
        && filesInStorage.length <= numCheckpointsToRetain) {
      return;
    }

    // 从文件名提取事务ID，排序
    TreeSet<Long> sortedTxIds = new TreeSet<Long>();
    if (filesInStorage != null) {
      for (String fName : filesInStorage) {
        // 从文件名提取事务ID
        long fTxId;
        try {
          fTxId = Long.parseLong(fName.substring(oivImagePrefix.length() + 1));
        } catch (NumberFormatException nfe) {
          // 文件名格式错误，跳过并记录警告
          LOG.warn("Invalid file name. Skipping " + fName);
          continue;
        }
        sortedTxIds.add(Long.valueOf(fTxId));
      }
    }

    // 计算需要删除的文件数量，删除最旧的多余文件
    int numFilesToDelete = sortedTxIds.size() - numCheckpointsToRetain;
    Iterator<Long> iter = sortedTxIds.iterator();
    while (numFilesToDelete > 0 && iter.hasNext()) {
      long txIdVal = iter.next().longValue();
      String fileName = NNStorage.getLegacyOIVImageFileName(txIdVal);
      LOG.info("Deleting " + fileName);
      File fileToDelete = new File(oivImageDir, fileName);
      if (!fileToDelete.delete()) {
        // 删除失败仅记录警告，下次再尝试
        LOG.warn("Failed to delete image file: " + fileToDelete);
      }
      numFilesToDelete--;
    }
  }
}