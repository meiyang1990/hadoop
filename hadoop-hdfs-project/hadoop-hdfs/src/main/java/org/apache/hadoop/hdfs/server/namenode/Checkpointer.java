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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.thirdparty.com.google.common.math.LongMath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件所属模块：HDFS ->  Namenode 核心服务
 * 检查点守护线程，负责周期性对HDFS元数据生成检查点，供BackupNode/SecondaryNameNode使用
 * 
 * 核心职责：定期触发检查点，满足两个触发条件之一即可：
 * 1. 距离上一次检查点超过配置的时间间隔
 * 2. 未检查点的事务数超过配置阈值
 * 
 * 工作流程：周期性唤醒检查，满足条件则从主NameNode拉取最新元数据生成检查点，回传给主NameNode
 */
class Checkpointer extends Daemon {
  public static final Logger LOG =
      LoggerFactory.getLogger(Checkpointer.class.getName());

  // 所属BackupNode实例
  private final BackupNode backupNode;
  // 线程运行标志，volatile保证多线程可见性
  volatile boolean shouldRun;

  // Checkpointer自身HTTP服务绑定地址，用于解决IP别名问题
  private String infoBindAddress;

  // 检查点配置，从配置文件加载
  private CheckpointConf checkpointConf;
  // Hadoop配置对象
  private final Configuration conf;

  /**
   * 获取当前BackupNode的镜像对象
   * @return BackupImage实例
   */
  private BackupImage getFSImage() {
    return (BackupImage)backupNode.getFSImage();
  }

  /**
   * 获取主NameNode的RPC代理对象，用于和主NameNode通信
   * @return 主NameNode的NamenodeProtocol代理
   */
  private NamenodeProtocol getRemoteNamenodeProxy(){
    return backupNode.namenode;
  }

  /**
   * 构造Checkpointer，初始化与主NameNode的连接
   * @param conf Hadoop配置
   * @param bnNode 所属BackupNode实例
   * @throws IOException 初始化失败时抛出
   */
  Checkpointer(Configuration conf, BackupNode bnNode)  throws IOException {
    this.conf = conf;
    this.backupNode = bnNode;
    try {
      initialize(conf);
    } catch(IOException e) {
      LOG.warn("Checkpointer got exception", e);
      shutdown();
      throw e;
    }
  }

  /**
   * 初始化检查点相关配置和状态
   * @param conf Hadoop配置
   * @throws IOException 初始化失败时抛出
   */
  private void initialize(Configuration conf) throws IOException {
    // 设置线程运行标志
    shouldRun = true;

    // 从配置加载检查点参数
    checkpointConf = new CheckpointConf(conf);

    // 提取HTTP地址中的主机名，避免IP别名问题
    String fullInfoAddr = conf.get(DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY, 
                                   DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT);
    infoBindAddress = fullInfoAddr.substring(0, fullInfoAddr.indexOf(":"));

    LOG.info("Checkpoint Period : " +
             checkpointConf.getPeriod() + " secs " +
             "(" + checkpointConf.getPeriod()/60 + " min)");
    LOG.info("Transactions count is  : " +
             checkpointConf.getTxnCount() +
             ", to trigger checkpoint");
  }

  /**
   * 关闭检查点线程，停止BackupNode服务
   */
  void shutdown() {
    shouldRun = false;
    backupNode.stop();
  }

  /**
   * Checkpointer主线程工作循环，周期性检查是否需要触发检查点
   */
  @Override
  public void work() {
    // 编辑日志检查间隔，取检查间隔和检查点周期的较小值
    long periodMSec = checkpointConf.getCheckPeriod() * 1000;
    // 强制检查点周期，无论事务数多少，到点必定触发检查点
    long checkpointPeriodMSec = checkpointConf.getPeriod() * 1000;

    long lastCheckpointTime = 0;
    long lastEditLogCheckTime =0;
    // 如果启动时不需要立即检查点，初始化上次检查点时间为当前时间
    if (!backupNode.shouldCheckpointAtStartup()) {
      lastCheckpointTime = monotonicNow();
    }
    while(shouldRun) {
      try {
        long now = monotonicNow();
        boolean shouldCheckpoint = false;
        // 达到强制检查点时间，触发检查点
        if(now >= lastCheckpointTime + checkpointPeriodMSec) {
          shouldCheckpoint = true;
        } 
        // 达到检查编辑日志的时间，检查未检查点事务数是否达标
        else if(now >= lastEditLogCheckTime + periodMSec) {
          long txns = countUncheckpointedTxns();
          lastEditLogCheckTime = now;
          // 未检查点事务数超过阈值，触发检查点
          if(txns >= checkpointConf.getTxnCount())
            shouldCheckpoint = true;
        }
        // 触发检查点流程，更新上次检查点时间
        if(shouldCheckpoint) {
          doCheckpoint();
          lastCheckpointTime = now;
          lastEditLogCheckTime = now;
        }
      } catch(IOException e) {
        LOG.error("Exception in doCheckpoint: ", e);
      } catch(Throwable e) {
        LOG.error("Throwable Exception in doCheckpoint: ", e);
        shutdown();
        break;
      }
      // 使用最大公约数作为休眠间隔，保证能够准时触发周期性检查
      try {
        Thread.sleep(LongMath.gcd(periodMSec, checkpointPeriodMSec));
      } catch(InterruptedException ie) {
        // do nothing
      }
    }
  }

  /**
   * 计算主NameNode上未检查点的事务数
   * @return 未检查点事务数量
   * @throws IOException RPC调用失败时抛出
   */
  private long countUncheckpointedTxns() throws IOException {
    long curTxId = getRemoteNamenodeProxy().getTransactionID();
    long uncheckpointedTxns = curTxId -
      getFSImage().getStorage().getMostRecentCheckpointTxId();
    assert uncheckpointedTxns >= 0;
    return uncheckpointedTxns;
  }

  /**
   * 执行完整检查点流程，从主NameNode拉取元数据生成新检查点
   * @throws IOException 检查点流程中任意步骤失败抛出
   */
  void doCheckpoint() throws IOException {
    BackupImage bnImage = getFSImage();
    NNStorage bnStorage = bnImage.getStorage();

    long startTime = monotonicNow();
    // 冻结命名空间，等待下次滚动编辑日志
    bnImage.freezeNamespaceAtNextRoll();
    
    // 向主NameNode发起检查点开始请求
    NamenodeCommand cmd = 
      getRemoteNamenodeProxy().startCheckpoint(backupNode.getRegistration());
    CheckpointCommand cpCmd = null;
    // 根据主NameNode返回的命令类型处理
    switch(cmd.getAction()) {
      case NamenodeProtocol.ACT_SHUTDOWN:
        // 主NameNode要求关闭本节点
        shutdown();
        throw new IOException("Name-node " + backupNode.nnRpcAddress
                                           + " requested shutdown.");
      case NamenodeProtocol.ACT_CHECKPOINT:
        // 开始执行检查点，转换命令类型
        cpCmd = (CheckpointCommand)cmd;
        break;
      default:
        throw new IOException("Unsupported NamenodeCommand: "+cmd.getAction());
    }

    // 等待命名空间冻结完成
    bnImage.waitUntilNamespaceFrozen();
    
    // 获取主NameNode检查点签名，用于校验一致性
    CheckpointSignature sig = cpCmd.getSignature();

    // 校验本节点和主NameNode存储信息一致，避免连接到错误节点
    sig.validateStorageInfo(bnImage);

    long lastApplied = bnImage.getLastAppliedTxId();
    LOG.debug("Doing checkpoint. Last applied: " + lastApplied);
    // 从主NameNode获取从上次检查点之后的编辑日志清单
    RemoteEditLogManifest manifest =
      getRemoteNamenodeProxy().getEditLogManifest(bnImage.getLastAppliedTxId() + 1);

    boolean needReloadImage = false;
    // 如果存在需要拉取的编辑日志
    if (!manifest.getLogs().isEmpty()) {
      RemoteEditLog firstRemoteLog = manifest.getLogs().get(0);
      // 本节点缺少起始事务日志，无法仅通过日志增量更新，需要下载完整新镜像
      if (firstRemoteLog.getStartTxId() > lastApplied + 1) {
        LOG.info("Unable to roll forward using only logs. Downloading " +
            "image with txid " + sig.mostRecentCheckpointTxId);
        // 下载主NameNode的最新FsImage到本地存储
        MD5Hash downloadedHash = TransferFsImage.downloadImageToStorage(
            backupNode.nnHttpAddress, sig.mostRecentCheckpointTxId, bnStorage,
            true, false);
        // 保存摘要并重命名镜像文件，完成下载
        bnImage.saveDigestAndRenameCheckpointImage(NameNodeFile.IMAGE,
            sig.mostRecentCheckpointTxId, downloadedHash);
        // 更新最后应用事务ID为镜像的事务ID
        lastApplied = sig.mostRecentCheckpointTxId;
        // 标记需要重新加载镜像到内存
        needReloadImage = true;
      }

      // 再次校验，仍然缺少日志则抛出异常
      if (firstRemoteLog.getStartTxId() > lastApplied + 1) {
        throw new IOException("No logs to roll forward from " + lastApplied);
      }
  
      // 依次下载所有需要的编辑日志文件到本地存储
      for (RemoteEditLog log : manifest.getLogs()) {
        TransferFsImage.downloadEditsToStorage(
            backupNode.nnHttpAddress, log, bnStorage);
      }

      // 如果下载了新镜像，加载镜像到内存命名空间
      if(needReloadImage) {
        LOG.info("Loading image with txid " + sig.mostRecentCheckpointTxId);
        // 获取命名空间全局写锁，保证元数据一致性
        backupNode.namesystem.writeLock(RwLockMode.GLOBAL);
        try {
          // 找到下载好的镜像文件
          File file = bnStorage.findImageFile(NameNodeFile.IMAGE,
              sig.mostRecentCheckpointTxId);
          // 从镜像文件重新加载命名空间
          bnImage.reloadFromImageFile(file, backupNode.getNamesystem());
        } finally {
          // 释放全局写锁
          backupNode.namesystem.writeUnlock(
              RwLockMode.GLOBAL, "doCheckpointByBackupNode");
        }
      }
      // 应用所有下载的编辑日志，将元数据向前滚动到最新状态
      rollForwardByApplyingLogs(manifest, bnImage, backupNode.getNamesystem());
    }
    
    // 获取检查点完成后的最新事务ID
    long txid = bnImage.getLastAppliedTxId();
    
    // 获取命名空间全局写锁，保存新检查点
    backupNode.namesystem.writeLock(RwLockMode.GLOBAL);
    try {
      // 标记命名空间已完成加载
      backupNode.namesystem.setImageLoaded();
      // 如果命名空间已有块信息，更新块管理器的块总数统计
      if(backupNode.namesystem.getBlocksTotal() > 0) {
        long completeBlocksTotal =
            backupNode.namesystem.getCompleteBlocksTotal();
        backupNode.namesystem.getBlockManager().setBlockTotal(
            completeBlocksTotal);
      }
      // 将最新元数据保存为检查点写入所有存储目录
      bnImage.saveFSImageInAllDirs(backupNode.getNamesystem(), txid);
      // 如果不是滚动升级阶段，更新存储版本号
      if (!backupNode.namenode.isRollingUpgrade()) {
        bnImage.updateStorageVersion();
      }
    } finally {
      // 释放全局写锁
      backupNode.namesystem.writeUnlock(RwLockMode.GLOBAL, "doCheckpoint");
    }

    // 如果主NameNode需要获取新检查点镜像，上传镜像回主NameNode
    if(cpCmd.needToReturnImage()) {
      TransferFsImage.uploadImageFromStorage(backupNode.nnHttpAddress, conf,
          bnStorage, NameNodeFile.IMAGE, txid);
    }

    // 通知主NameNode检查点完成
    getRemoteNamenodeProxy().endCheckpoint(backupNode.getRegistration(), sig);

    // 如果是BackupNode，合并日志缓存到本地journal
    if (backupNode.getRole() == NamenodeRole.BACKUP) {
      bnImage.convergeJournalSpool();
    }
    // 更新节点注册信息，保持和主NameNode信息一致
    backupNode.setRegistration(); // keep registration up to date
    
    // 输出检查点完成日志，统计耗时和镜像大小
    long imageSize = bnImage.getStorage().getFsImageName(txid).length();
    LOG.info("Checkpoint completed in "
        + (monotonicNow() - startTime)/1000 + " seconds."
        + " New Image Size: " + imageSize);
  }

  /**
   * 获取Checkpointer的HTTP服务监听地址URL
   * @return 完整URL对象
   */
  private URL getImageListenAddress() {
    InetSocketAddress httpSocAddr = backupNode.getHttpAddress();
    int httpPort = httpSocAddr.getPort();
    try {
      return new URL(DFSUtil.getHttpClientScheme(conf) + "://" + infoBindAddress + ":" + httpPort);
    } catch (MalformedURLException e) {
      // Unreachable
      throw new RuntimeException(e);
    }
  }

  /**
   * 通过应用下载的编辑日志，将元数据向前滚动到最新状态
   * @param manifest 远程编辑日志清单
   * @param dstImage 目标FsImage对象
   * @param dstNamesystem 目标命名空间对象
   * @throws IOException 加载日志或应用失败时抛出
   */
  static void rollForwardByApplyingLogs(
      RemoteEditLogManifest manifest,
      FSImage dstImage,
      FSNamesystem dstNamesystem) throws IOException {
    NNStorage dstStorage = dstImage.getStorage();
  
    List<EditLogInputStream> editsStreams = Lists.newArrayList();
    // 遍历所有需要应用的编辑日志
    for (RemoteEditLog log : manifest.getLogs()) {
      // 只应用事务ID大于当前最后应用ID的日志
      if (log.getEndTxId() > dstImage.getLastAppliedTxId()) {
        // 从本地存储找到已下载的编辑日志文件
        File f = dstStorage.findFinalizedEditsFile(
            log.getStartTxId(), log.getEndTxId());
        // 创建日志输入流，添加到待加载列表
        editsStreams.add(new EditLogFileInputStream(f, log.getStartTxId(), 
                                                    log.getEndTxId(), true));
      }
    }
    LOG.info("Checkpointer about to load edits from " +
        editsStreams.size() + " stream(s).");
    // 批量加载并应用所有编辑日志到目标命名空间，完成元数据更新
    dstImage.loadEdits(editsStreams, dstNamesystem);
  }
}