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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeStatus;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocolPB.DatanodeLifelineProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager.LockLevel;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.InvalidBlockReportLeaseException;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;

/**
 * 每个NameNode对应一个线程，负责数据节点与NameNode之间的核心交互，包括：
 * <ul>
 * <li> 与NameNode完成注册前握手 </li>
 * <li> 向NameNode注册数据节点 </li>
 * <li> 定期向NameNode发送心跳 </li>
 * <li> 处理NameNode返回的指令 </li>
 * </ul>
 * 在HA集群中，每个数据节点会为Active和Standby NameNode分别创建一个该类的实例。
 */
@InterfaceAudience.Private
class BPServiceActor implements Runnable {
  
  static final Logger LOG = DataNode.LOG;
  final InetSocketAddress nnAddr;
  HAServiceState state;

  final BPOfferService bpos;
  
  volatile long lastCacheReport = 0;
  private final Scheduler scheduler;

  Thread bpThread;
  DatanodeProtocolClientSideTranslatorPB bpNamenode;

  /**
   * 定义Actor当前的运行状态。
   */
  enum RunningState {
    CONNECTING, INIT_FAILED, RUNNING, EXITED, FAILED;
  }

  private String serviceId = null;
  private String nnId = null;
  private volatile RunningState runningState = RunningState.CONNECTING;
  private volatile boolean shouldServiceRun = true;
  private volatile boolean isSlownode = false;
  private final DataNode dn;
  private final DNConf dnConf;
  private long prevBlockReportId;
  private long fullBlockReportLeaseId;
  private final SortedSet<Integer> blockReportSizes =
      Collections.synchronizedSortedSet(new TreeSet<>());
  private final int maxDataLength;

  private final IncrementalBlockReportManager ibrManager;

  private DatanodeRegistration bpRegistration;
  final LinkedList<BPServiceActorAction> bpThreadQueue 
      = new LinkedList<BPServiceActorAction>();
  private final CommandProcessingThread commandProcessingThread;

  /**
   * 构造方法，创建针对指定NameNode的BPServiceActor实例。
   * @param serviceId 服务ID
   * @param nnId NameNode ID
   * @param nnAddr NameNode地址
   * @param lifelineNnAddr 生命线服务地址
   * @param bpos 所属的块池服务
   */
  BPServiceActor(String serviceId, String nnId, InetSocketAddress nnAddr,
      InetSocketAddress lifelineNnAddr, BPOfferService bpos) {
    this.bpos = bpos;
    this.dn = bpos.getDataNode();
    this.nnAddr = nnAddr;
    this.lifelineSender = lifelineNnAddr != null ?
        new LifelineSender(lifelineNnAddr) : null;
    this.initialRegistrationComplete = lifelineNnAddr != null ?
        new CountDownLatch(1) : null;
    this.dnConf = dn.getDnConf();
    this.ibrManager = new IncrementalBlockReportManager(
        dnConf.ibrInterval,
        dn.getMetrics());
    prevBlockReportId = ThreadLocalRandom.current().nextLong();
    fullBlockReportLeaseId = 0;
    scheduler = new Scheduler(dnConf.heartBeatInterval,
        dnConf.getLifelineIntervalMs(), dnConf.blockReportInterval,
        dnConf.outliersReportIntervalMs);
    // get the value of maxDataLength.
    this.maxDataLength = dnConf.getMaxDataLength();
    if (serviceId != null) {
      this.serviceId = serviceId;
    }
    if (nnId != null) {
      this.nnId = nnId;
    }
    commandProcessingThread = new CommandProcessingThread(this);
    commandProcessingThread.start();
  }

  /**
   * 获取当前Actor的数据节点注册信息。
   * @return 数据节点注册信息
   */
  public DatanodeRegistration getBpRegistration() {
    return bpRegistration;
  }

  /**
   * 获取增量块报告管理器。
   * @return 增量块报告管理器实例
   */
  IncrementalBlockReportManager getIbrManager() {
    return ibrManager;
  }

  /**
   * 检查当前Actor是否处于存活运行状态。
   * @return 如果Actor正在运行或连接中返回true，否则返回false
   */
  boolean isAlive() {
    if (!shouldServiceRun || !bpThread.isAlive()) {
      return false;
    }
    return runningState == BPServiceActor.RunningState.RUNNING
        || runningState == BPServiceActor.RunningState.CONNECTING;
  }

  /**
   * 获取当前Actor运行状态的字符串描述。
   * @return 运行状态字符串
   */
  String getRunningState() {
    return runningState.toString();
  }

  @Override
  public String toString() {
    return bpos.toString() + " service to " + nnAddr;
  }
  
  /**
   * 获取NameNode的socket地址。
   * @return NameNode地址
   */
  InetSocketAddress getNNSocketAddress() {
    return nnAddr;
  }

  private String getNameNodeAddress() {
    return NetUtils.getHostPortString(getNNSocketAddress());
  }

  /**
   * 获取当前Actor的监控信息Map，用于WebUI展示。
   * @return 包含Actor运行信息的Map
   */
  Map<String, String> getActorInfoMap() {
    final Map<String, String> info = new HashMap<String, String>();
    info.put("NamenodeAddress", getNameNodeAddress());
    info.put("NamenodeHaState", state != null ? state.toString() : "Unknown");
    info.put("BlockPoolID", bpos.getBlockPoolId());
    info.put("ActorState", getRunningState());
    info.put("LastHeartbeat",
        String.valueOf(getScheduler().getLastHearbeatTime()));
    info.put("LastHeartbeatResponseTime",
        String.valueOf(getScheduler().getLastHeartbeatResponseTime()));
    info.put("LastBlockReport",
        String.valueOf(getScheduler().getLastBlockReportTime()));
    info.put("maxBlockReportSize", String.valueOf(getMaxBlockReportSize()));
    info.put("maxDataLength", String.valueOf(maxDataLength));
    info.put("isSlownode", String.valueOf(isSlownode));
    return info;
  }

  private final CountDownLatch initialRegistrationComplete;
  private final LifelineSender lifelineSender;

  /**
   * 单元测试用，注入NameNode代理对象。
   */
  @VisibleForTesting
  void setNameNode(DatanodeProtocolClientSideTranslatorPB dnProtocol) {
    bpNamenode = dnProtocol;
  }

  @VisibleForTesting
  String getNnId() {
    return nnId;
  }

  @VisibleForTesting
  DatanodeProtocolClientSideTranslatorPB getNameNodeProxy() {
    return bpNamenode;
  }

  /**
   * 单元测试用，注入生命线NameNode代理对象。
   */
  @VisibleForTesting
  void setLifelineNameNode(
      DatanodeLifelineProtocolClientSideTranslatorPB dnLifelineProtocol) {
    lifelineSender.lifelineNamenode = dnLifelineProtocol;
  }

  @VisibleForTesting
  DatanodeLifelineProtocolClientSideTranslatorPB getLifelineNameNodeProxy() {
    return lifelineSender.lifelineNamenode;
  }

  /**
   * 与NameNode进行握手第一阶段，获取NameNode的命名空间和版本信息。
   * 会自动重试直到NameNode响应或数据节点关闭。
   * @return NameNode的命名空间信息
   * @throws IOExceptions 当获取失败或DN关闭时抛出异常
   */
  @VisibleForTesting
  NamespaceInfo retrieveNamespaceInfo() throws IOException {
    NamespaceInfo nsInfo = null;
    while (shouldRun()) {
      try {
        nsInfo = bpNamenode.versionRequest();
        LOG.debug("{} received versionRequest response: {}", this, nsInfo);
        break;
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.warn("Problem connecting to server: " + nnAddr);
      } catch(IOException e ) {  // namenode is not available
        LOG.warn("Problem connecting to server: " + nnAddr);
      }
      
      // try again in a second
      sleepAndLogInterrupts(5000, "requesting version info from NN");
    }
    
    if (nsInfo != null) {
      checkNNVersion(nsInfo);
    } else {
      throw new IOException("DN shut down before block pool connected");
    }
    return nsInfo;
  }

  /**
   * 检查NameNode版本是否满足DataNode最低要求。
   * @param nsInfo NameNode命名空间信息
   * @throws IncorrectVersionException 当版本不满足要求时抛出异常
   */
  private void checkNNVersion(NamespaceInfo nsInfo)
      throws IncorrectVersionException {
    // build and layout versions should match
    String nnVersion = nsInfo.getSoftwareVersion();
    String minimumNameNodeVersion = dnConf.getMinimumNameNodeVersion();
    if (VersionUtil.compareVersions(nnVersion, minimumNameNodeVersion) < 0) {
      IncorrectVersionException ive = new IncorrectVersionException(
          minimumNameNodeVersion, nnVersion, "NameNode", "DataNode");
      LOG.warn(ive.getMessage());
      throw ive;
    }
    String dnVersion = VersionInfo.getVersion();
    if (!nnVersion.equals(dnVersion)) {
      LOG.info("Reported NameNode version '" + nnVersion + "' does not match " +
          "DataNode version '" + dnVersion + "' but is within acceptable " +
          "limits. Note: This is normal during a rolling upgrade.");
    }
  }

  /**
   * 连接NameNode并完成完整握手流程。
   * @throws IOException 握手过程中发生IO异常
   */
  private void connectToNNAndHandshake() throws IOException {
    // 获取NameNode代理
    bpNamenode = dn.connectToNN(nnAddr);

    // 握手第一阶段：获取命名空间信息
    NamespaceInfo nsInfo = retrieveNamespaceInfo();

    // 初始化块池锁
    dn.getDataSetLockManager().addLock(LockLevel.BLOCK_POOl,
        nsInfo.getBlockPoolID());

    // 验证版本信息并在DN中初始化该块池（如果是第一个连接）
    bpos.verifyAndSetNamespaceInfo(this, nsInfo);
    state = nsInfo.getState();

    /* 获取命名空间信息后重新设置线程名 */
    this.bpThread.setName(formatThreadName("heartbeating", nnAddr));

    // 握手第二阶段：向NameNode注册
    register(nsInfo);
  }


  /**
   * 单元测试用，立即触发一次全量块报告。
   */
  @VisibleForTesting
  void triggerBlockReportForTests() {
    synchronized (ibrManager) {
      scheduler.scheduleHeartbeat();
      long oldBlockReportTime = scheduler.getNextBlockReportTime();
      scheduler.forceFullBlockReportNow();
      ibrManager.notifyAll();
      while (oldBlockReportTime == scheduler.getNextBlockReportTime()) {
        try {
          ibrManager.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }
  
  @VisibleForTesting
  void triggerHeartbeatForTests() {
    synchronized (ibrManager) {
      final long nextHeartbeatTime = scheduler.scheduleHeartbeat();
      ibrManager.notifyAll();
      while (nextHeartbeatTime - scheduler.nextHeartbeatTime >= 0) {
        try {
          ibrManager.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }

  private int getMaxBlockReportSize() {
    int maxBlockReportSize = 0;
    if (!blockReportSizes.isEmpty()) {
      maxBlockReportSize = blockReportSizes.last();
    }
    return maxBlockReportSize;
  }

  /**
   * 生成唯一的块报告ID，避免与NameNode的0值冲突。
   * @return 唯一的块报告ID
   */
  private long generateUniqueBlockReportId() {
    // 首次初始化块报告ID
    // 注意NameNode使用0表示未初始化，因此我们不能发送0值
    prevBlockReportId++;
    while (prevBlockReportId == 0) {
      prevBlockReportId = ThreadLocalRandom.current().nextLong();
    }
    return prevBlockReportId;
  }

  /**
   * 向NameNode发送全量块报告，报告本节点存储的所有块信息。
   * @param fullBrLeaseId 全量块报告租约ID
   * @return NameNode返回的指令列表，可能为null
   * @throws IOException 发送块报告过程中发生IO异常
   */
  List<DatanodeCommand> blockReport(long fullBrLeaseId) throws IOException {
    final ArrayList<DatanodeCommand> cmds = new ArrayList<DatanodeCommand>();

    // 发送块报告前先刷新所有增量块报告，避免状态不一致
    ibrManager.sendIBRs(bpNamenode, bpRegistration,
        bpos.getBlockPoolId(), getRpcMetricSuffix());

    long brCreateStartTime = monotonicNow();
    Map<DatanodeStorage, BlockListAsLongs> perVolumeBlockLists =
        dn.getFSDataset().getBlockReports(bpos.getBlockPoolId());

    // 将块报告