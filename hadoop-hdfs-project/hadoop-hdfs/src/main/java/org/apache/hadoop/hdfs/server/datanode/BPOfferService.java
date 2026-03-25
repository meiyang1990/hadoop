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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeStatus;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.server.protocol.BlockECReconstructionCommand.BlockECReconstructionInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Sets;

import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 文件概要：数据节点（DataNode）端单个块池/命名空间的服务管理类，负责维护该命名空间下所有NameNode（主备）的心跳通信，
 * 管理对应BPServiceActor实例，跟踪当前活跃NameNode状态，处理来自NameNode的命令并分发任务给对应Actor。
 * 在HA集群中，负责协调主备切换，识别脑裂场景，保证只处理活跃NameNode的命令。
 */
@InterfaceAudience.Private
class BPOfferService {
  static final Logger LOG = DataNode.LOG;
  
  /**
   * 当前服务注册的命名空间信息，握手第一阶段完成后赋值
   */
  NamespaceInfo bpNSInfo;

  /**
   * 当前块池的数据节点注册信息，握手第二阶段完成后赋值
   */
  volatile DatanodeRegistration bpRegistration;

  private final String nameserviceId;
  private volatile String bpId;
  private final DataNode dn;

  /**
   * 当前指向活跃NameNode的BPServiceActor引用，所有NameNode都处于备用状态时可为null。
   * 非null时一定是{@link #bpServices}列表中的成员
   */
  private volatile BPServiceActor bpServiceToActive = null;
  
  /**
   * 当前命名服务下所有NameNode对应的Actor列表，无论其处于活跃还是备用状态
   */
  private final List<BPServiceActor> bpServices =
    new CopyOnWriteArrayList<BPServiceActor>();

  /**
   * 记录声称自己是活跃的NameNode发送的最新事务ID，用于识别脑裂场景：
   * 旧的活跃NameNode仍然声称自己活跃，但事务ID低于最新值，此时会被拒绝。参见HDFS-2627
   */
  private long lastActiveClaimTxId = -1;

  private final ReentrantReadWriteLock mReadWriteLock =
      new ReentrantReadWriteLock();
  private final Lock mReadLock  = mReadWriteLock.readLock();
  private final Lock mWriteLock = mReadWriteLock.writeLock();

  /**
   * 获取读锁，保护共享状态的并发访问
   */
  void readLock() {
    mReadLock.lock();
  }

  /**
   * 释放读锁
   */
  void readUnlock() {
    mReadLock.unlock();
  }

  /**
   * 获取写锁，用于修改共享状态
   */
  void writeLock() {
    mWriteLock.lock();
  }

  /**
   * 释放写锁
   */
  void writeUnlock() {
    mWriteLock.unlock();
  }

  /**
   * 构造方法，为指定命名服务创建BPOfferService，初始化所有NameNode对应的Actor
   * @param nameserviceId 命名服务ID
   * @param nnIds NameNode ID列表
   * @param nnAddrs NameNode地址列表
   * @param lifelineNnAddrs NameNode生命线地址列表
   * @param dn 所属DataNode实例
   */
  BPOfferService(
      final String nameserviceId, List<String> nnIds,
      List<InetSocketAddress> nnAddrs,
      List<InetSocketAddress> lifelineNnAddrs,
      DataNode dn) {
    Preconditions.checkArgument(!nnAddrs.isEmpty(),
        "Must pass at least one NN.");
    Preconditions.checkArgument(nnAddrs.size() == lifelineNnAddrs.size(),
        "Must pass same number of NN addresses and lifeline addresses.");
    this.nameserviceId = nameserviceId;
    this.dn = dn;

    for (int i = 0; i < nnAddrs.size(); ++i) {
      this.bpServices.add(new BPServiceActor(nameserviceId, nnIds.get(i),
          nnAddrs.get(i), lifelineNnAddrs.get(i), this));
    }
  }

  /**
   * 刷新NameNode地址列表，添加新增的NameNode，停止并移除已删除的NameNode
   * @param serviceId 命名服务ID
   * @param nnIds 新的NameNode ID列表
   * @param addrs 新的NameNode地址列表
   * @param lifelineAddrs 新的NameNode生命线地址列表
   * @throws IOException 刷新过程中的IO异常
   */
  void refreshNNList(String serviceId, List<String> nnIds,
      ArrayList<InetSocketAddress> addrs,
      ArrayList<InetSocketAddress> lifelineAddrs) throws IOException {
    Set<InetSocketAddress> oldAddrs = new HashSet<>();
    for (BPServiceActor actor : bpServices) {
      oldAddrs.add(actor.getNNSocketAddress());
    }
    Set<InetSocketAddress> newAddrs = new HashSet<>(addrs);
    
    // 处理新增的NameNode
    Set<InetSocketAddress> addedNNs = Sets.difference(newAddrs, oldAddrs);
    for (InetSocketAddress addedNN : addedNNs) {
      BPServiceActor actor = new BPServiceActor(serviceId,
          nnIds.get(addrs.indexOf(addedNN)), addedNN,
          lifelineAddrs.get(addrs.indexOf(addedNN)), this);
      actor.start();
      bpServices.add(actor);
    }

    // 处理移除的NameNode
    Set<InetSocketAddress> removedNNs = Sets.difference(oldAddrs, newAddrs);
    for (InetSocketAddress removedNN : removedNNs) {
      for (BPServiceActor actor : bpServices) {
        if (actor.getNNSocketAddress().equals(removedNN)) {
          actor.stop();
          shutdownActor(actor);
          break;
        }
      }
    }
  }

  /**
   * 检查当前服务是否已经至少向一个NameNode完成注册
   * @return true表示已完成初始化注册，false表示未完成
   */
  boolean isInitialized() {
    return bpRegistration != null;
  }
  
  /**
   * 检查是否至少有一个Actor线程正在和NameNode通信
   * @return true表示至少有一个Actor存活，false表示全部停止
   */
  boolean isAlive() {
    for (BPServiceActor actor : bpServices) {
      if (actor.isAlive()) {
        return true;
      }
    }
    return false;
  }

  /**
   * 获取当前服务对应的命名服务ID
   * @return 命名服务ID，可为null
   */
  String getNameserviceId() {
    return nameserviceId;
  }

  /**
   * 获取当前块池ID，注册未完成时加读锁查询，已完成时直接返回缓存值避免锁竞争
   * @param quiet 是否静默，未注册时不输出警告日志
   * @return 块池ID，未注册时返回null
   */
  String getBlockPoolId(boolean quiet) {
    // 注册完成后直接返回缓存ID，避免锁竞争
    String id = bpId;
    if (id != null) {
      return id;
    }
    // 注入故障，用于测试锁竞争场景
    DataNodeFaultInjector.get().delayWhenOfferServiceHoldLock();
    readLock();
    try {
      if (bpNSInfo != null) {
        return bpNSInfo.getBlockPoolID();
      } else {
        if (!quiet) {
          LOG.warn("Block pool ID needed, but service not yet registered with "
              + "NN, trace:", new Exception());
        }
        return null;
      }
    } finally {
      readUnlock();
    }
  }

  /**
   * 获取当前块池ID，默认非静默模式
   * @return 块池ID，未注册时返回null
   */
  String getBlockPoolId() {
    return getBlockPoolId(false);
  }

  /**
   * 检查是否已经获取到块池ID
   * @return true表示已获取，false表示未获取
   */
  boolean hasBlockPoolId() {
    return getBlockPoolId(true) != null;
  }

  /**
   * 获取当前命名空间信息，加读锁保证并发安全
   * @return 命名空间信息
   */
  NamespaceInfo getNamespaceInfo() {
    readLock();
    try {
      return bpNSInfo;
    } finally {
      readUnlock();
    }
  }

  /**
   * 设置命名空间信息，仅用于测试
   * @param nsInfo 新的命名空间信息
   * @return 旧的命名空间信息
   * @throws IOException 命名空间信息不匹配时抛出异常
   */
  @VisibleForTesting
  NamespaceInfo setNamespaceInfo(NamespaceInfo nsInfo) throws IOException {
    writeLock();
    try {
      NamespaceInfo old = bpNSInfo;
      if (bpNSInfo != null && nsInfo != null) {
        // 校验块池ID、命名空间ID、集群ID一致性，避免接入不同集群的NameNode
        checkNSEquality(bpNSInfo.getBlockPoolID(), nsInfo.getBlockPoolID(),
            "Blockpool ID");
        checkNSEquality(bpNSInfo.getNamespaceID(), nsInfo.getNamespaceID(),
            "Namespace ID");
        checkNSEquality(bpNSInfo.getClusterID(), nsInfo.getClusterID(),
            "Cluster ID");
      }
      bpNSInfo = nsInfo;
      // 缓存块池ID，实现无锁访问
      bpId = (nsInfo != null) ? nsInfo.getBlockPoolID() : null;
      return old;
    } finally {
      writeUnlock();
    }
  }

  @Override
  public String toString() {
    readLock();
    try {
      if (bpNSInfo == null) {
        // 尚未连接到NameNode，未知块池ID
        String datanodeUuid = dn.getDatanodeUuid();

        if (datanodeUuid == null || datanodeUuid.isEmpty()) {
          datanodeUuid = "unassigned";
        }
        return "Block pool <registering> (Datanode Uuid " + datanodeUuid + ")";
      } else {
        return "Block pool " + getBlockPoolId() +
            " (Datanode Uuid " + dn.getDatanodeUuid() +
            ")";
      }
    } finally {
      readUnlock();
    }
  }
  
  /**
   * 向所有NameNode报告损坏块，将报告任务加入所有Actor的队列
   * @param block 损坏的扩展块
   * @param storageUuid 块所在存储的UUID
   * @param storageType 存储类型
   */
  void reportBadBlocks(ExtendedBlock block,
                       String storageUuid, StorageType storageType) {
    checkBlock(block);
    ReportBadBlockAction rbbAction = new ReportBadBlockAction(block, storageUuid, storageType);
    for (BPServiceActor actor : bpServices) {
      actor.bpThreadEnqueue(rbbAction);
    }
  }
  
  /*
   * 通知NameNode块接收完成可能需要较长时间，这里不等待通知完成就直接返回客户端成功，
   * 异步通知NameNode
   */
  /**
   * 通知所有NameNode已接收完成一个新块
   * @param block 已接收的块
   * @param delHint 删除提示
   * @param storageUuid 块所在存储UUID
   * @param isOnTransientStorage 块是否在临时存储上
   */
  void notifyNamenodeReceivedBlock(ExtendedBlock block, String delHint,
      String storageUuid, boolean isOnTransientStorage) {
    notifyNamenodeBlock(block, BlockStatus.RECEIVED_BLOCK, delHint,
        storageUuid, isOnTransientStorage);
  }

  /**
   * 通知所有NameNode正在接收一个块
   * @param block 正在接收的块
   * @param storageUuid 存储UUID
   */
  void notifyNamenodeReceivingBlock(ExtendedBlock block, String storageUuid) {
    notifyNamenodeBlock(block, BlockStatus.RECEIVING_BLOCK, null, storageUuid,
        false);
  }

  /**
   * 通知所有NameNode已删除一个块
   * @param block 已删除的块
   * @param storageUuid 存储UUID
   */
  void notifyNamenodeDeletedBlock(ExtendedBlock block, String storageUuid) {
    notifyNamenodeBlock(block, BlockStatus.DELETED_BLOCK, null, storageUuid,
        false);
  }

  /**
   * 通用方法：向所有NameNode通知块状态变更，通过增量块报告管理器处理
   * @param block 变更的块
   * @param status 块状态（接收中/已接收/已删除）
   * @param delHint 删除提示
   * @param storageUuid 存储UUID
   * @param isOnTransientStorage 是否在临时存储
   */
  private void notifyNamenodeBlock(ExtendedBlock block, BlockStatus status,
      String delHint, String storageUuid, boolean isOnTransientStorage) {
    checkBlock(block);
    final ReceivedDeletedBlockInfo info = new ReceivedDeletedBlockInfo(
        block.getLocalBlock(), status, delHint);
    final DatanodeStorage storage = dn.getFSDataset().getStorage(storageUuid);
    if (storage == null) {
      LOG.warn("Trying to add RDBI for null storage UUID {}. Trace: {}", storageUuid,
          Joiner.on("\n").join(Thread.currentThread().getStackTrace()));
      getDataNode().getMetrics().incrNullStorageBlockReports();
      return;
    }

    for (BPServiceActor actor : bpServices) {
      actor.getIbrManager().notifyNamenodeBlock(info, storage,
          isOnTransientStorage);
    }
  }

  /**
   * 校验块属于当前块池，参数不为空
   * @param block 待校验的扩展块
   */
  private void checkBlock(ExtendedBlock block) {
    Preconditions.checkArgument(block != null,
        "block is null");
    final String bpId = getBlockPoolId();
    Preconditions.checkArgument(block.getBlockPoolId().equals(bpId),
        "block belongs to BP %s instead of BP %s",
        block.getBlockPoolId(), bpId);
  }

  /**
   * 启动该块池服务下所有Actor线程，仅由BlockPoolManager调用
   */
  //This must be called only by blockPoolManager
  void start() {
    for (BPServiceActor actor : bpServices) {
      actor.start();
    }
  }
  
  /**
   * 停止该块池服务下所有Actor线程，仅由BlockPoolManager调用
   */
  //This must be called only by blockPoolManager.
  void stop() {
    for (BPServiceActor actor : bpServices) {
      actor.stop();
    }
  }
  
  /**
   * 等待所有Actor线程退出，仅由BlockPoolManager调用
   */
  //This must be called only by blockPoolManager
  void join() {
    for (BPServiceActor actor : bpServices) {
      actor.join();
    }
  }

  /**
   * 获取所属的DataNode实例
   * @return DataNode实例
   */
  DataNode getDataNode() {
    return dn;
  }

  /**
   * BPServiceActor握手完成后调用，验证并设置命名空间信息，保证所有NameNode属于同一集群，
   * 第一个完成握手的Actor会触发DataNode初始化该块