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

package org.apache.hadoop.hdfs.server.sps;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.sps.BlockMoveTaskHandler;
import org.apache.hadoop.hdfs.server.namenode.sps.BlockMovementListener;
import org.apache.hadoop.hdfs.server.namenode.sps.Context;
import org.apache.hadoop.hdfs.server.namenode.sps.FileCollector;
import org.apache.hadoop.hdfs.server.namenode.sps.SPSService;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier.DatanodeMap;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier.DatanodeWithStorage;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.sps.metrics.ExternalSPSBeanMetrics;
import org.apache.hadoop.net.NetworkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：外部存储策略满足器（SPS）上下文，负责连接NameNode，为外置SPS服务从NameNode获取所需的运行信息
 * 该类实现Context接口，作为外置独立部署SPS服务的上下文环境，封装了与NameNode的交互逻辑
 */
@InterfaceAudience.Private
public class ExternalSPSContext implements Context {
  public static final Logger LOG = LoggerFactory
      .getLogger(ExternalSPSContext.class);
  // 外部SPS服务实例
  private final SPSService service;
  // NameNode连接器，负责与NameNode通信
  private final NameNodeConnector nnc;
  // 默认块存储策略套件，用于查询存储策略信息
  private final BlockStoragePolicySuite createDefaultSuite =
      BlockStoragePolicySuite.createDefaultSuite();
  // 文件收集器，用于收集需要满足存储策略的文件
  private final FileCollector fileCollector;
  // 块移动任务处理器，处理外部SPS发起的块移动任务
  private final BlockMoveTaskHandler externalHandler;
  // 块移动事件监听器
  private final BlockMovementListener blkMovementListener;
  // 外部SPS服务指标统计
  private ExternalSPSBeanMetrics spsBeanMetrics;

  /**
   * 构造外部SPS上下文实例，初始化各核心组件
   * @param service 外部SPS服务实例
   * @param nnc NameNode连接器，用于与NameNode通信
   */
  public ExternalSPSContext(SPSService service, NameNodeConnector nnc) {
    this.service = service;
    this.nnc = nnc;
    this.fileCollector = new ExternalSPSFilePathCollector(service);
    this.externalHandler = new ExternalSPSBlockMoveTaskHandler(
        service.getConf(), nnc, service);
    this.blkMovementListener = new ExternalBlockMovementListener();
  }

  @Override
  /**
   * 检查外部SPS服务是否正在运行
   * @return true表示服务正在运行，false表示服务已停止
   */
  public boolean isRunning() {
    return service.isRunning();
  }

  @Override
  /**
   * 检查NameNode是否处于安全模式
   * @return true表示NameNode在安全模式，false表示不在安全模式
   */
  public boolean isInSafeMode() {
    try {
      return nnc != null ? nnc.getDistributedFileSystem().isInSafeMode()
          : false;
    } catch (IOException e) {
      LOG.warn("Exception while creating Namenode Connector..", e);
      return false;
    }
  }

  @Override
  /**
   * 根据当前DataNode信息构建集群网络拓扑
   * @param datanodeMap 包含所有目标DataNode信息的映射
   * @return 构建完成的集群网络拓扑对象
   */
  public NetworkTopology getNetworkTopology(DatanodeMap datanodeMap) {
    // create network topology.
    NetworkTopology cluster = NetworkTopology.getInstance(service.getConf());
    List<DatanodeWithStorage> targets = datanodeMap.getTargets();
    for (DatanodeWithStorage node : targets) {
      cluster.add(node.getDatanodeInfo());
    }
    return cluster;
  }

  @Override
  /**
   * 检查指定文件ID对应的文件是否存在于HDFS
   * @param path 文件ID
   * @return true表示文件存在，false表示文件不存在或获取异常
   */
  public boolean isFileExist(long path) {
    Path filePath = DFSUtilClient.makePathFromFileId(path);
    try {
      return nnc.getDistributedFileSystem().exists(filePath);
    } catch (IllegalArgumentException | IOException e) {
      LOG.warn("Exception while getting file is for the given path:{}",
          filePath, e);
    }
    return false;
  }

  @Override
  /**
   * 根据策略ID获取对应的存储策略
   * @param policyId 存储策略ID
   * @return 对应ID的存储策略
   */
  public BlockStoragePolicy getStoragePolicy(byte policyId) {
    return createDefaultSuite.getPolicy(policyId);
  }

  @Override
  /**
   * 删除指定inode上的SPS处理提示XAttr
   * @param inodeId 文件inode ID
   * @throws IOException 删除操作异常时抛出
   */
  public void removeSPSHint(long inodeId) throws IOException {
    Path filePath = DFSUtilClient.makePathFromFileId(inodeId);
    try {
      nnc.getDistributedFileSystem().removeXAttr(filePath,
          HdfsServerConstants.XATTR_SATISFY_STORAGE_POLICY);
    } catch (IOException e) {
      List<String> listXAttrs = nnc.getDistributedFileSystem()
          .listXAttrs(filePath);
      // 提示已经被删除时，忽略异常不报错
      if (!listXAttrs
          .contains(HdfsServerConstants.XATTR_SATISFY_STORAGE_POLICY)) {
        LOG.info("SPS hint already removed for the inodeId:{}."
            + " Ignoring exception:{}", inodeId, e.getMessage());
      }
    }
  }

  @Override
  /**
   * 获取当前集群中存活DataNode的数量
   * @return 存活DataNode数量，获取异常时返回0
   */
  public int getNumLiveDataNodes() {
    try {
      return nnc.getDistributedFileSystem()
          .getDataNodeStats(DatanodeReportType.LIVE).length;
    } catch (IOException e) {
      LOG.warn("Exception while getting number of live datanodes.", e);
    }
    return 0;
  }

  @Override
  /**
   * 根据文件ID获取文件的详细信息（包含块位置信息）
   * @param path 文件ID
   * @return 文件状态信息，文件不存在时返回null
   * @throws IOException 访问NameNode异常时抛出
   */
  public HdfsFileStatus getFileInfo(long path) throws IOException {
    HdfsLocatedFileStatus fileInfo = null;
    try {
      Path filePath = DFSUtilClient.makePathFromFileId(path);
      fileInfo = nnc.getDistributedFileSystem().getClient()
          .getLocatedFileInfo(filePath.toString(), false);
    } catch (FileNotFoundException e) {
      LOG.debug("Path:{} doesn't exists!", path, e);
    }
    return fileInfo;
  }

  @Override
  /**
   * 获取所有存活DataNode的存储报告
   * @return 存活DataNode的存储报告数组
   * @throws IOException 从NameNode获取信息失败时抛出
   */
  public DatanodeStorageReport[] getLiveDatanodeStorageReport()
      throws IOException {
    return nnc.getLiveDatanodeStorageReport();
  }

  @Override
  /**
   * 从NameNode获取下一个需要处理的SPS文件ID
   * @return 下一个待处理文件ID，获取失败时返回null
   */
  public Long getNextSPSPath() {
    try {
      return nnc.getNNProtocolConnection().getNextSPSPath();
    } catch (IOException e) {
      LOG.warn("Exception while getting next sps path id from Namenode.", e);
      return null;
    }
  }

  @Override
  /**
   * 扫描并收集指定路径下需要满足存储策略的文件
   * @param path 需要扫描的文件ID
   * @throws IOException 扫描过程IO异常时抛出
   * @throws InterruptedException 扫描被中断时抛出
   */
  public void scanAndCollectFiles(long path)
      throws IOException, InterruptedException {
    fileCollector.scanAndCollectFiles(path);
  }

  @Override
  /**
   * 提交块移动任务到任务处理器处理
   * @param blkMovingInfo 待移动块的信息
   * @throws IOException 提交任务IO异常时抛出
   */
  public void submitMoveTask(BlockMovingInfo blkMovingInfo) throws IOException {
    externalHandler.submitMoveTask(blkMovingInfo);
  }

  @Override
  /**
   * 通知块移动尝试完成事件给监听器
   * @param moveAttemptFinishedBlks 已经完成移动尝试的块数组
   */
  public void notifyMovementTriedBlocks(Block[] moveAttemptFinishedBlks) {
    // External listener if it is plugged-in
    if (blkMovementListener != null) {
      blkMovementListener.notifyMovementTriedBlocks(moveAttemptFinishedBlks);
    }
  }

  /**
   * 块移动事件监听器实现，用于外部SPS记录已尝试移动的块信息
   */
  private static class ExternalBlockMovementListener
      implements BlockMovementListener {

    // 保存实际尝试移动的块列表
    private List<Block> actualBlockMovements = new ArrayList<>();

    @Override
    /**
     * 处理块移动尝试完成通知，将完成的块添加到列表并打印日志
     * @param moveAttemptFinishedBlks 已经完成移动尝试的块数组
     */
    public void notifyMovementTriedBlocks(Block[] moveAttemptFinishedBlks) {
      for (Block block : moveAttemptFinishedBlks) {
        actualBlockMovements.add(block);
      }
      LOG.info("Movement attempted blocks", actualBlockMovements);
    }
  }

  /**
   * 初始化外部SPS的指标统计
   * @param sps 存储策略满足器实例
   */
  public void initMetrics(StoragePolicySatisfier sps) {
    spsBeanMetrics = new ExternalSPSBeanMetrics(sps);
  }

  /**
   * 关闭外部SPS的指标统计，释放资源
   */
  public void closeMetrics() {
    spsBeanMetrics.close();
  }

  @VisibleForTesting
  /**
   * 获取外部SPS的指标统计实例，仅供测试使用
   * @return 外部SPS指标统计实例
   */
  public ExternalSPSBeanMetrics getSpsBeanMetrics() {
    return spsBeanMetrics;
  }
}