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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URL;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.JournalProtocolService;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.ha.HAState;
import org.apache.hadoop.hdfs.server.protocol.FenceResponse;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.protobuf.BlockingService;

/**
 * 备份节点（BackupNode），负责同步主NameNode的元数据并生成检查点
 * <p>
 * Backup node可以扮演两种角色：
 * <ol>
 * <li>{@link NamenodeRole#CHECKPOINT} 检查点节点定期从活跃主节点下载镜像和编辑日志，合并生成新检查点，再将新镜像上传回主节点。</li>
 * <li>{@link NamenodeRole#BACKUP} 备份节点实时保持命名空间与主节点同步，只需要定期将命名空间保存到本地磁盘即可生成检查点。</li>
 * </ol>
 * 本类是HDFS元数据备份与检查点机制的核心服务实现，为主NameNode提供元数据冗余备份能力。
 */
@InterfaceAudience.Private
@Metrics(context="dfs")
public class BackupNode extends NameNode {
  private static final String BN_ADDRESS_NAME_KEY = DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY;
  private static final String BN_ADDRESS_DEFAULT = DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_DEFAULT;
  private static final String BN_HTTP_ADDRESS_NAME_KEY = DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY;
  private static final String BN_HTTP_ADDRESS_DEFAULT = DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT;
  private static final String BN_SERVICE_RPC_ADDRESS_KEY = DFSConfigKeys.DFS_NAMENODE_BACKUP_SERVICE_RPC_ADDRESS_KEY;
  private static final float  BN_SAFEMODE_THRESHOLD_PCT_DEFAULT = 1.5f;
  private static final int    BN_SAFEMODE_EXTENSION_DEFAULT = Integer.MAX_VALUE;

  /** 活跃主NameNode的RPC代理 */
  NamenodeProtocol namenode;
  /** 活跃主NameNode的RPC地址 */
  String nnRpcAddress;
  /** 活跃主NameNode的HTTP地址 */
  URL nnHttpAddress;
  /** 检查点管理器，负责定期触发检查点流程 */
  Checkpointer checkpointManager;
  
  /**
   * 构造BackupNode实例
   * @param conf Hadoop配置对象
   * @param role BackupNode角色（CHECKPOINT/BACKUP）
   * @throws IOException 初始化失败时抛出IO异常
   */
  BackupNode(Configuration conf, NamenodeRole role) throws IOException {
    super(conf, role);
  }

  /////////////////////////////////////////////////////
  // Common NameNode methods implementation for backup node.
  /////////////////////////////////////////////////////
  @Override // NameNode
  protected InetSocketAddress getRpcServerAddress(Configuration conf) {
    String addr = conf.getTrimmed(BN_ADDRESS_NAME_KEY, BN_ADDRESS_DEFAULT);
    return NetUtils.createSocketAddr(addr);
  }
  
  @Override
  protected InetSocketAddress getServiceRpcServerAddress(Configuration conf) {
    String addr = conf.getTrimmed(BN_SERVICE_RPC_ADDRESS_KEY);
    if (addr == null || addr.isEmpty()) {
      return null;
    }
    return NetUtils.createSocketAddr(addr);
  }

  @Override // NameNode
  protected void setRpcServerAddress(Configuration conf,
      InetSocketAddress addr) {
    conf.set(BN_ADDRESS_NAME_KEY, NetUtils.getHostPortString(addr));
  }
  
  @Override // Namenode
  protected void setRpcServiceServerAddress(Configuration conf,
      InetSocketAddress addr) {
    conf.set(BN_SERVICE_RPC_ADDRESS_KEY, NetUtils.getHostPortString(addr));
  }

  @Override // NameNode
  protected InetSocketAddress getHttpServerAddress(Configuration conf) {
    assert getNameNodeAddress() != null : "rpcAddress should be calculated first";
    String addr = conf.getTrimmed(BN_HTTP_ADDRESS_NAME_KEY, BN_HTTP_ADDRESS_DEFAULT);
    return NetUtils.createSocketAddr(addr);
  }

  @Override // NameNode
  protected void loadNamesystem(Configuration conf) throws IOException {
    // 覆盖配置：BackupNode始终保持安全模式，阈值设为超过100%确保不会自动退出安全模式
    conf.setFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY,
                                BN_SAFEMODE_THRESHOLD_PCT_DEFAULT);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY,
                                BN_SAFEMODE_EXTENSION_DEFAULT);
    // 创建BackupImage实例，管理备份节点的元数据存储
    BackupImage bnImage = new BackupImage(conf);
    // 初始化FSNamesystem，加载命名空间
    this.namesystem = new FSNamesystem(conf, bnImage);
    // 备份节点不需要做配额检查
    namesystem.dir.disableQuotaChecks();
    bnImage.setNamesystem(namesystem);
    // 恢复并读取现有元数据
    bnImage.recoverCreateRead();
  }

  @Override // NameNode
  protected void initialize(Configuration conf) throws IOException {
    // 由于同步竞态条件，BackupNode不支持异步日志写入，强制关闭
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_EDITS_ASYNC_LOGGING, false);

    // 回收站在BackupNode上禁用，如果未来切换为活跃节点会重新启用
    conf.setLong(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, 
                 CommonConfigurationKeys.FS_TRASH_INTERVAL_DEFAULT);
    // 与主NameNode握手，获取命名空间信息
    NamespaceInfo nsInfo = handshake(conf);
    super.initialize(conf);
    // 设置块池ID
    namesystem.setBlockPoolId(nsInfo.getBlockPoolID());

    if (false == namesystem.isInSafeMode()) {
      // 强制进入安全模式，BackupNode始终保持安全模式
      namesystem.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    }

    // BackupNode永远不需要做租约恢复，因此将租约硬限设为永不过期
    namesystem.leaseManager.setLeasePeriod(
        HdfsConstants.LEASE_SOFTLIMIT_PERIOD, Long.MAX_VALUE);

    // 向活跃主NameNode注册当前BackupNode
    registerWith(nsInfo);
    // RPC服务启动后再启动检查点守护线程
    runCheckpointDaemon(conf);
    InetSocketAddress addr = getHttpAddress();
    if (addr != null) {
      conf.set(BN_HTTP_ADDRESS_NAME_KEY, NetUtils.getHostPortString(getHttpAddress()));
    }
  }

  @Override
  protected NameNodeRpcServer createRpcServer(Configuration conf)
      throws IOException {
    return new BackupNodeRpcServer(conf, this);
  }

  @Override // NameNode
  public void stop() {
    stop(true);
  }
  
  @VisibleForTesting
  void stop(boolean reportError) {
   
    if(checkpointManager != null) {
      // 阻止新检查点启动，已开始的检查点会继续执行完成
      checkpointManager.shouldRun = false;
    }
    
    // reportError用于测试场景，模拟BackupNode异常退出不通知主节点
    if (reportError && namenode != null && getRegistration() != null) {
      // 通知主节点将本节点从备份流列表移除
      try {
        namenode.errorReport(getRegistration(), NamenodeProtocol.FATAL,
            "Shutting down.");
      } catch(IOException e) {
        LOG.error("Failed to report to name-node.", e);
      }
    }
    // 停止RPC客户端代理
    if (namenode != null) {
      RPC.stopProxy(namenode);
    }
    namenode = null;
    // 停止检查点管理器线程
    if(checkpointManager != null) {
      checkpointManager.interrupt();
      checkpointManager = null;
    }

    // 终止当前日志段，避免正常关闭导致错误
    if (namesystem != null) {
      getFSImage().getEditLog().abortCurrentLogSegment();
    }

    // 调用父类停止基础服务
    super.stop();
  }
  
  /* @Override */// NameNode
  public boolean setSafeMode(SafeModeAction action)
      throws IOException {
    throw new UnsupportedActionException("setSafeMode");
  }
  
  /**
   * BackupNode的RPC服务端实现，实现JournalProtocol接收主节点发送的编辑日志
   */
  static class BackupNodeRpcServer extends NameNodeRpcServer implements
      JournalProtocol {
    /**
     * 构造BackupNode RPC服务端
     * @param conf Hadoop配置对象
     * @param nn BackupNode实例
     * @throws IOException 初始化失败时抛出IO异常
     */
    private BackupNodeRpcServer(Configuration conf, BackupNode nn)
        throws IOException {
      super(conf, nn);
      JournalProtocolServerSideTranslatorPB journalProtocolTranslator = 
          new JournalProtocolServerSideTranslatorPB(this);
      BlockingService service = JournalProtocolService
          .newReflectiveBlockingService(journalProtocolTranslator);
      DFSUtil.addInternalPBProtocol(conf, JournalProtocolPB.class, service,
          this.clientRpcServer);
    }
    
    /** 
     * 校验来自主节点的日志请求，验证命名空间ID和集群ID一致性
     */
    private void verifyJournalRequest(JournalInfo journalInfo)
        throws IOException {
      verifyLayoutVersion(journalInfo.getLayoutVersion());
      String errorMsg = null;
      int expectedNamespaceID = namesystem.getNamespaceInfo().getNamespaceID();
      if (journalInfo.getNamespaceId() != expectedNamespaceID) {
        errorMsg = "Invalid namespaceID in journal request - expected " + expectedNamespaceID
            + " actual " + journalInfo.getNamespaceId();
        LOG.warn(errorMsg);
        throw new UnregisteredNodeException(journalInfo);
      } 
      if (!journalInfo.getClusterId().equals(namesystem.getClusterId())) {
        errorMsg = "Invalid clusterId in journal request - expected "
            + journalInfo.getClusterId() + " actual " + namesystem.getClusterId();
        LOG.warn(errorMsg);
        throw new UnregisteredNodeException(journalInfo);
      }
    }

    /////////////////////////////////////////////////////
    // JournalProtocol implementation for backup node.
    /////////////////////////////////////////////////////
    @Override
    public void startLogSegment(JournalInfo journalInfo, long epoch,
        long txid) throws IOException {
      namesystem.checkOperation(OperationCategory.JOURNAL);
      verifyJournalRequest(journalInfo);
      getBNImage().namenodeStartedLogSegment(txid);
    }
    
    @Override
    public void journal(JournalInfo journalInfo, long epoch, long firstTxId,
        int numTxns, byte[] records) throws IOException {
      namesystem.checkOperation(OperationCategory.JOURNAL);
      verifyJournalRequest(journalInfo);
      getBNImage().journal(firstTxId, numTxns, records);
    }

    private BackupImage getBNImage() {
      return (BackupImage)nn.getFSImage();
    }

    @Override
    public FenceResponse fence(JournalInfo journalInfo, long epoch,
        String fencerInfo) throws IOException {
      LOG.info("Fenced by " + fencerInfo + " with epoch " + epoch);
      throw new UnsupportedOperationException(
          "BackupNode does not support fence");
    }
  }
  
  //////////////////////////////////////////////////////
  
  /**
   * 判断启动时是否需要执行检查点
   * @return 启动时需要执行检查点返回true，否则返回false
   */
  boolean shouldCheckpointAtStartup() {
    FSImage fsImage = getFSImage();
    if(isRole(NamenodeRole.CHECKPOINT)) {
      assert fsImage.getStorage().getNumStorageDirs() > 0;
      // 检查点节点如果没有版本文件说明是第一次启动，需要执行检查点
      return ! fsImage.getStorage().getStorageDir(0).getVersionFile().exists();
    }
    
    // 备份节点启动时总是需要执行检查点，确保和主节点命名空间同步
    return true;
  }

  /**
   * 和主NameNode建立连接并完成握手，版本验证，获取命名空间信息
   * @param conf Hadoop配置对象
   * @return 主节点返回的命名空间信息
   * @throws IOException 握手失败时抛出IO异常
   */
  private NamespaceInfo handshake(Configuration conf) throws IOException {
    // 连接主NameNode
    InetSocketAddress nnAddress = NameNode.getServiceAddress(conf, true);
    // 创建非HA模式下的主NameNode代理
    this.namenode = NameNodeProxies.createNonHAProxy(conf, nnAddress,
        NamenodeProtocol.class, UserGroupInformation.getCurrentUser(),
        true).getProxy();
    this.nnRpcAddress = NetUtils.getHostPortString(nnAddress);
    this.nnHttpAddress = DFSUtil.getInfoServer(nnAddress, conf,
        DFSUtil.getHttpClientScheme(conf)).toURL();
    // 从主节点获取版本和ID信息
    NamespaceInfo nsInfo = null;
    while(!isStopRequested()) {
      try {
        nsInfo = handshake(namenode);
        break;
      } catch(SocketTimeoutException e) {  // 主节点忙，重试连接
        LOG.info("Problem connecting to server: " + nnAddress);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          LOG.warn("Encountered exception ", e);
        }
      }
    }
    return nsInfo;
  }

  /**
   * 启动检查点守护线程
   * @param conf Hadoop配置对象
   * @throws IOException 启动失败时抛出IO异常
   */
  private void runCheckpointDaemon(Configuration conf) throws IOException {
    checkpointManager = new Checkpointer(conf, this);
    checkpointManager.start();
  }

  /**
   * 触发一次检查点流程，供测试使用
   * @throws IOException 检查点执行失败抛出IO异常
   */
  void doCheckpoint() throws IOException {
    checkpointManager.doCheckpoint();
  }

  /**
   * 向活跃主NameNode注册当前BackupNode
   * @param nsInfo 命名空间信息
   * @throws IOException 注册失败抛出IO异常
   */
  private void registerWith(NamespaceInfo nsInfo) throws IOException {
    BackupImage bnImage = (BackupImage)getFSImage();
    NNStorage storage = bnImage.getStorage();
    // 验证命名空间ID
    if (storage.getNamespaceID() == 0) { // 新备份存储，初始化存储信息
      storage.setStorageInfo(nsInfo);
      storage.setBlockPoolID(nsInfo.getBlockPoolID());
      storage.setClusterID(nsInfo.getClusterID());
    } else { // 已有存储，验证信息一致性
      nsInfo.validateStorage(storage);
    }
    // 初始化编辑日志
    bnImage.initEditLog(StartupOption.REGULAR);
    setRegistration();
    NamenodeRegistration nnReg = null;
    while(!isStopRequested()) {
      try {
        // 向主节点注册当前下级NameNode
        nnReg = namenode.registerSubordinate