// 这个文件已经全部加上中文注释
package org.apache.hadoop.hdfs.server.sps;
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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.sps.FileCollector;
import org.apache.hadoop.hdfs.server.namenode.sps.ItemInfo;
import org.apache.hadoop.hdfs.server.namenode.sps.SPSService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件路径收集器，用于存储策略满足器(SPS)，递归扫描指定路径收集需要处理的文件。
 * 对于目录会递归遍历所有子项，对于文件直接提交给SPS服务处理。
 * 外部SPS服务使用该类完成待处理文件的路径扫描与收集。
 */
@InterfaceAudience.Private
public class ExternalSPSFilePathCollector implements FileCollector {
  public static final Logger LOG =
      LoggerFactory.getLogger(ExternalSPSFilePathCollector.class);
  private DistributedFileSystem dfs;
  private SPSService service;
  private int maxQueueLimitToScan;

  /**
   * 构造外部SPS文件路径收集器，初始化DFS连接和队列限制
   * @param service SPS服务实例，用于提交收集到的文件
   */
  public ExternalSPSFilePathCollector(SPSService service) {
    this.service = service;
    this.maxQueueLimitToScan = service.getConf().getInt(
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_QUEUE_LIMIT_KEY,
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_QUEUE_LIMIT_DEFAULT);
    try {
      // TODO: probably we could get this dfs from external context? but this is
      // too specific to external.
      dfs = getFS(service.getConf());
    } catch (IOException e) {
      LOG.error("Unable to get the filesystem. Make sure Namenode running and "
          + "configured namenode address is correct.", e);
    }
  }

  /**
   * 根据配置获取HDFS分布式文件系统实例
   * @param conf Hadoop配置对象
   * @return 分布式文件系统实例
   * @throws IOException 获取文件系统失败时抛出异常
   */
  private DistributedFileSystem getFS(Configuration conf) throws IOException {
    return (DistributedFileSystem) FileSystem
        .get(FileSystem.getDefaultUri(conf), conf);
  }

  /**
   * 递归扫描指定路径，将所有文件添加到SPS服务等待处理，返回扫描到的待处理文件总数。
   * @param startID 起始路径ID
   * @param childPath 当前扫描的路径字符串
   * @return 当前路径下扫描到的待处理文件总数
   */
  private long processPath(Long startID, String childPath) {
    long pendingWorkCount = 0; // to be satisfied file counter
    for (byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME;;) {
      final DirectoryListing children;
      try {
        // 批量获取目录下的子项
        children = dfs.getClient().listPaths(childPath,
            lastReturnedName, false);
      } catch (IOException e) {
        LOG.warn("Failed to list directory " + childPath
            + ". Ignore the directory and continue.", e);
        return pendingWorkCount;
      }
      if (children == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("The scanning start dir/sub dir " + childPath
              + " does not have childrens.");
        }
        return pendingWorkCount;
      }

      // 遍历当前批次的所有子项
      for (HdfsFileStatus child : children.getPartialListing()) {
        if (child.isFile()) {
          // 文件添加到SPS处理队列
          service.addFileToProcess(new ItemInfo(startID, child.getFileId()),
              false);
          // 检查队列容量，满了则阻塞等待
          checkProcessingQueuesFree();
          pendingWorkCount++; // increment to be satisfied file count
        } else {
          // 构造子目录完整路径
          String childFullPathName = child.getFullName(childPath);
          if (child.isDirectory()) {
            if (!childFullPathName.endsWith(Path.SEPARATOR)) {
              childFullPathName = childFullPathName + Path.SEPARATOR;
            }
            // 递归扫描子目录
            pendingWorkCount += processPath(startID, childFullPathName);
          }
        }
      }

      // 还有更多子项，继续下一批遍历
      if (children.hasMore()) {
        lastReturnedName = children.getLastName();
      } else {
        // 当前目录遍历完成，返回结果
        return pendingWorkCount;
      }
    }
  }

  /**
   * 检查SPS处理队列剩余容量，如果队列满了则阻塞等待直到有空闲空间
   */
  private void checkProcessingQueuesFree() {
    int remainingCapacity = remainingCapacity();
    // wait for queue to be free
    while (remainingCapacity <= 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Waiting for storageMovementNeeded queue to be free!");
      }
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      remainingCapacity = remainingCapacity();
    }
  }

  /**
   * 计算并返回SPS处理队列的剩余可用容量
   * @return 剩余可用容量
   */
  public int remainingCapacity() {
    int size = service.processingQueueSize();
    int remainingSize = 0;
    if (size < maxQueueLimitToScan) {
      remainingSize = maxQueueLimitToScan - size;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("SPS processing Q -> maximum capacity:{}, current size:{},"
          + " remaining size:{}", maxQueueLimitToScan, size, remainingSize);
    }
    return remainingSize;
  }

  /**
   * 根据路径ID扫描并收集该路径下所有需要满足存储策略的文件，实现FileCollector接口
   * @param pathId 需要扫描的路径ID
   * @throws IOException 文件系统操作失败时抛出异常
   */
  @Override
  public void scanAndCollectFiles(long pathId) throws IOException {
    if (dfs == null) {
      dfs = getFS(service.getConf());
    }
    // 根据路径ID构造路径对象
    Path filePath = DFSUtilClient.makePathFromFileId(pathId);
    // 递归扫描路径，统计待处理文件数量
    long pendingSatisfyItemsCount = processPath(pathId, filePath.toString());
    // Check whether the given path contains any item to be tracked
    // or the no to be satisfied paths. In case of empty list, add the given
    // inodeId to the 'pendingWorkForDirectory' with empty list so that later
    // SPSPathIdProcessor#run function will remove the SPS hint considering that
    // this path is already satisfied the storage policy.
    // 没有待处理文件，将路径标记为已满足，移除SPS提示
    if (pendingSatisfyItemsCount <= 0) {
      LOG.debug("There is no pending items to satisfy the given path "
          + "inodeId:{}", pathId);
      service.addAllFilesToProcess(pathId, new ArrayList<>(), true);
    } else {
      // 标记路径扫描完成，等待后续处理
      service.markScanCompletedForPath(pathId);
    }
  }

}