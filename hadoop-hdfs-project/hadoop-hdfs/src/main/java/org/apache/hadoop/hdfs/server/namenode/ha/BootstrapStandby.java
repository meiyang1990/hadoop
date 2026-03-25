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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNUpgradeUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.tools.DFSHAAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;

/**
 * 备⽤NameNode存储目录引导工具，通过从活动NameNode拷贝最新命名空间快照初始化备⽤节点存储。
 * 用于首次配置HDFS高可用(HA)集群时，初始化备⽤NameNode的元数据存储。
 */
@InterfaceAudience.Private
public class BootstrapStandby implements Tool, Configurable {
  private static final Logger LOG =
      LoggerFactory.getLogger(BootstrapStandby.class);
  private String nsId;
  private String nnId;
  private List<RemoteNameNodeInfo> remoteNNs;

  private Collection<URI> dirsToFormat;
  private List<URI> editUrisToFormat;
  private List<URI> sharedEditsUris;
  private Configuration conf;
  
  private boolean force = false;
  private boolean interactive = true;
  private boolean skipSharedEditsCheck = false;

  private boolean inMemoryAliasMapEnabled;
  private String aliasMapPath;

  // 退出/返回码定义
  static final int ERR_CODE_FAILED_CONNECT = 2;
  static final int ERR_CODE_INVALID_VERSION = 3;
  // 跳过4 - 旧版本使用过，现已不再返回该码
  static final int ERR_CODE_ALREADY_FORMATTED = 5;
  static final int ERR_CODE_LOGS_UNAVAILABLE = 6; 

  @Override
  public int run(String[] args) throws Exception {
    parseArgs(args);
    // 禁用RPC尾部日志机制引导备⽤节点，该场景下RPC尾部效率较低；参考HDFS-14806
    conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, false);
    parseConfAndFindOtherNN();
    NameNode.checkAllowFormat(conf);

    InetSocketAddress myAddr = DFSUtilClient.getNNAddress(conf);
    SecurityUtil.login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY,
        DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, myAddr.getHostName());

    // 使用登录用户身份执行实际引导流程
    return SecurityUtil.doAsLoginUserOrFatal(new PrivilegedAction<Integer>() {
      @Override
      public Integer run() {
        try {
          return doRun();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }
  
  private void parseArgs(String[] args) {
    for (String arg : args) {
      if ("-force".equals(arg)) {
        force = true;
      } else if ("-nonInteractive".equals(arg)) {
        interactive = false;
      } else if ("-skipSharedEditsCheck".equals(arg)) {
        skipSharedEditsCheck = true;
      } else {
        printUsage();
        throw new HadoopIllegalArgumentException(
            "Illegal argument: " + arg);
      }
    }
  }

  private void printUsage() {
    System.out.println("Usage: " + this.getClass().getSimpleName() +
        " [-force] [-nonInteractive] [-skipSharedEditsCheck]\n"
        + "\t-force: formats if the name directory exists.\n"
        + "\t-nonInteractive: formats aborts if the name directory exists,\n"
        + "\tunless -force option is specified.\n"
        + "\t-skipSharedEditsCheck: skips edits check which ensures that\n"
        + "\twe have enough edits already in the shared directory to start\n"
        + "\tup from the last checkpoint on the active.");
  }

  /**
   * 创建远程NameNode的NamenodeProtocol代理对象
   * @param otherIpcAddr 远程NameNode的IPC地址
   * @return 代理对象
   * @throws IOException 创建代理失败时抛出IO异常
   */
  private NamenodeProtocol createNNProtocolProxy(InetSocketAddress otherIpcAddr)
      throws IOException {
    return NameNodeProxies.createNonHAProxy(getConf(),
        otherIpcAddr, NamenodeProtocol.class,
        UserGroupInformation.getLoginUser(), true)
        .getProxy();
  }
  
  /**
   * 执行备⽤NameNode存储引导的实际业务逻辑
   * @return 执行结果返回码，0表示成功，非0表示对应错误
   * @throws IOException 执行过程中IO异常
   */
  private int doRun() throws IOException {
    // 查找可用的活动NameNode
    NamenodeProtocol proxy = null;
    NamespaceInfo nsInfo = null;
    boolean isUpgradeFinalized = false;
    boolean isRollingUpgrade = false;
    RemoteNameNodeInfo proxyInfo = null;
    // 遍历所有远程NameNode，查找可连接获取命名空间信息的节点
    for (int i = 0; i < remoteNNs.size(); i++) {
      proxyInfo = remoteNNs.get(i);
      InetSocketAddress otherIpcAddress = proxyInfo.getIpcAddress();
      proxy = createNNProtocolProxy(otherIpcAddress);
      try {
        // 从任意可用活动NameNode获取命名空间信息
        // 如果刚格式化主NameNode并准备引导其他节点，只会连接到该单个节点
        // 如果集群已经运行，后续新增或替换故障NameNode，则会从集群任意可用节点引导
        nsInfo = getProxyNamespaceInfo(proxy);
        isUpgradeFinalized = proxy.isUpgradeFinalized();
        isRollingUpgrade = proxy.isRollingUpgrade();
        break;
      } catch (IOException ioe) {
        LOG.warn("Unable to fetch namespace information from remote NN at " + otherIpcAddress
            + ": " + ioe.getMessage());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Full exception trace", ioe);
        }
      }
    }

    // 所有远程NameNode都连接失败
    if (nsInfo == null) {
      LOG.error(
          "Unable to fetch namespace information from any remote NN. Possible NameNodes: "
              + remoteNNs);
      return ERR_CODE_FAILED_CONNECT;
    }

    // 检查布局版本兼容性
    if (!checkLayoutVersion(nsInfo, isRollingUpgrade)) {
      if(isRollingUpgrade) {
        LOG.error("Layout version on remote node in rolling upgrade ({}, {})"
            + " is not compatible based on minimum compatible version ({})",
            nsInfo.getLayoutVersion(), proxyInfo.getIpcAddress(),
            HdfsServerConstants.MINIMUM_COMPATIBLE_NAMENODE_LAYOUT_VERSION);
      } else {
        LOG.error("Layout version on remote node ({}) does not match this "
            + "node's service layout version ({})", nsInfo.getLayoutVersion(),
            nsInfo.getServiceLayoutVersion());
      }
      return ERR_CODE_INVALID_VERSION;
    }

    // 打印引导信息预览
    System.out.println(
        "=====================================================\n" +
        "About to bootstrap Standby ID " + nnId + " from:\n" +
        "           Nameservice ID: " + nsId + "\n" +
        "        Other Namenode ID: " + proxyInfo.getNameNodeID() + "\n" +
        "  Other NN's HTTP address: " + proxyInfo.getHttpAddress() + "\n" +
        "  Other NN's IPC  address: " + proxyInfo.getIpcAddress() + "\n" +
        "             Namespace ID: " + nsInfo.getNamespaceID() + "\n" +
        "            Block pool ID: " + nsInfo.getBlockPoolID() + "\n" +
        "               Cluster ID: " + nsInfo.getClusterID() + "\n" +
        "           Layout version: " + nsInfo.getLayoutVersion() + "\n" +
        "   Service Layout version: " + nsInfo.getServiceLayoutVersion() + "\n" +
        "       isUpgradeFinalized: " + isUpgradeFinalized + "\n" +
        "         isRollingUpgrade: " + isRollingUpgrade + "\n" +
        "=====================================================");

    // 创建存储对象
    NNStorage storage = new NNStorage(conf, dirsToFormat, editUrisToFormat);

    // 远程NameNode处于升级未完成状态，当前备⽤节点也需要准备升级目录
    if (!isUpgradeFinalized) {
      LOG.info("The active NameNode is in Upgrade. " +
          "Prepare the upgrade for the standby NameNode as well.");
      if (!doPreUpgrade(storage, nsInfo)) {
        return ERR_CODE_ALREADY_FORMATTED;
      }
    } else if (!format(storage, nsInfo, isRollingUpgrade)) { // 提示用户格式化存储
      return ERR_CODE_ALREADY_FORMATTED;
    }

    // 从活动NameNode下载fsimage
    int download = downloadImage(storage, proxy, proxyInfo, isRollingUpgrade);
    if (download != 0) {
      return download;
    }

    // 完成升级流程：重命名previous.tmp为previous
    if (!isUpgradeFinalized) {
      doUpgrade(storage);
    }

    // 如果启用内存别名映射，则引导下载别名映射数据
    if (inMemoryAliasMapEnabled) {
      return formatAndDownloadAliasMap(aliasMapPath, proxyInfo);
    } else {
      LOG.info("Skipping InMemoryAliasMap bootstrap as it was not configured");
    }
    return 0;
  }

  @VisibleForTesting
  public NamespaceInfo getProxyNamespaceInfo(NamenodeProtocol proxy)
      throws IOException {
    return proxy.versionRequest();
  }

  /**
   * 遍历所有存储目录，检查是否需要格式化，在用户允许的情况下执行格式化
   * @param storage NN存储对象
   * @param nsInfo 命名空间信息
   * @param isRollingUpgrade 是否处于滚动升级
   * @return 格式化完成返回true，否则返回false
   * @throws IOException 格式化过程IO异常
   */
  private boolean format(NNStorage storage, NamespaceInfo nsInfo,
      boolean isRollingUpgrade) throws IOException {
    // 在清除数据前与用户确认
    if (!Storage.confirmFormat(storage.dirIterable(null), force, interactive)) {
      storage.close();
      return false;
    } else {
      // 执行格式化，写入VERSION文件
      storage.format(nsInfo, isRollingUpgrade);
      return true;
    }
  }

  /**
   * HA升级场景下的备⽤节点预升级处理：远程活动节点处于升级状态时，备⽤节点也需要创建previous目录，
   * 将当前目录重命名为previous.tmp，使得备⽤节点启动时能识别集群处于升级状态。
   */
  private boolean doPreUpgrade(NNStorage storage, NamespaceInfo nsInfo)
      throws IOException {
    boolean isFormatted = false;
    Map<StorageDirectory, StorageState> dataDirStates =
        new HashMap<>();
    try {
      isFormatted = FSImage.recoverStorageDirs(StartupOption.UPGRADE, storage,
          dataDirStates);
      if (dataDirStates.values().contains(StorageState.NOT_FORMATTED)) {
        // recoverStorageDirs只要存在一个已格式化目录就会返回true
        isFormatted = false;
        System.err.println("The original storage directory is not formatted.");
      }
    } catch (InconsistentFSStateException e) {
      // 存储处于不一致状态
      LOG.warn("The storage directory is in an inconsistent state", e);
    } finally {
      storage.unlockAll();
    }

    // 如果存储未格式化或状态不一致，执行格式化
    // 即使这里使用新版本软件格式化，在HA场景下备⽤节点也可以通过bootstrapStandby回滚，不会有问题
    if (!isFormatted && !format(storage, nsInfo, false)) {
      return false;
    }

    // 确保不存在已有的previous目录
    FSImage.checkUpgrade(storage);
    // 对每个目录执行预升级处理
    for (Iterator<StorageDirectory> it = storage.dirIterator(false);
         it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        NNUpgradeUtil.renameCurToTmp(sd);
      } catch (IOException e) {
        LOG.error("Failed to move aside pre-upgrade storage " +
            "in image directory " + sd.getRoot(), e);
        throw e;
      }
    }
    storage.setStorageInfo(nsInfo);
    storage.setBlockPoolID(nsInfo.getBlockPoolID());
    return true;
  }

  /**
   * 完成升级流程，对每个存储目录执行升级操作
   */
  private void doUpgrade(NNStorage storage) throws IOException {
    for (Iterator<StorageDirectory> it = storage.dirIterator(false);
         it.hasNext();) {
      StorageDirectory sd = it.next();
      NNUpgradeUtil.doUpgrade(sd, storage);
    }
  }

  /**
   * 从远程活动NameNode下载fsimage到本地存储
   * @param storage 本地NN存储对象
   * @param proxy 远程NameNode代理
   * @param proxyInfo 远程NameNode信息
   * @param isRollingUpgrade 是否处于滚动升级
   * @return 下载结果返回码，0表示成功
   * @throws IOException 下载过程IO异常
   */
  private int downloadImage(NNStorage storage, NamenodeProtocol proxy, RemoteNameNodeInfo proxyInfo,
        boolean isRollingUpgrade)
      throws IOException {
    // 从远程获取最新检查点事务ID和当前事务ID
    final long imageTxId = proxy.getMostRecentCheckpointTxId();
    final long curTxId = proxy.getTransactionID();

    // 滚动升级场景下，额外下载回滚用fsimage
    if (isRollingUpgrade) {
      final long rollbackTxId =
          proxy.getMostRecentNameNodeFileTxId(NameNodeFile.IMAGE_ROLLBACK);
      assert rollbackTxId != HdfsServerConstants.INVALID_TXID :
          "Expected a valid TXID for fsimage_rollback file";
      FSImage rollbackImage = new FSImage(conf);
      try {
        rollbackImage.getStorage().setStorageInfo(storage);
        MD5Hash hash = TransferFsImage.downloadImageTo