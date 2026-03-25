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

import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorageRetentionManager.StoragePurger;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.VersionInfo;

import javax.management.ObjectName;

/**
 * 文件说明：HDFS SecondaryNameNode主实现类，负责对主NameNode的元数据执行定期检查点操作
 * 核心功能：周期性合并fsimage和edit日志，生成新的检查点，减轻主NameNode压力
 */
/**********************************************************
 * The Secondary NameNode is a helper to the primary NameNode.
 * The Secondary is responsible for supporting periodic checkpoints 
 * of the HDFS metadata. The current design allows only one Secondary
 * NameNode per HDFs cluster.
 *
 * The Secondary NameNode is a daemon that periodically wakes
 * up (determined by the schedule specified in the configuration),
 * triggers a periodic checkpoint and then goes back to sleep.
 * The Secondary NameNode uses the NamenodeProtocol to talk to the
 * primary NameNode.
 *
 **********************************************************/
@InterfaceAudience.Private
/**
 *  SecondaryNameNode类：HDFS中协助主NameNode执行元数据检查点的后台守护进程
 *  核心职责：周期性合并主NameNode的fsimage和edits日志，生成新的检查点上传给主NameNode
 *  实现接口：Runnable（作为后台线程执行）、SecondaryNameNodeInfoMXBean（JMX监控接口）
 *  在非HA架构中承担检查点职责，HA架构中该职责由Standby NameNode承担
 */
public class SecondaryNameNode implements Runnable,
        SecondaryNameNodeInfoMXBean {
    
  static{
    HdfsConfiguration.init();
  }
  public static final Logger LOG =
      LoggerFactory.getLogger(SecondaryNameNode.class.getName());

  private final long starttime = Time.now();
  private volatile long lastCheckpointTime = 0;
  private volatile long lastCheckpointWallclockTime = 0;

  private URL fsName;
  private CheckpointStorage checkpointImage;

  private NamenodeProtocol namenode;
  private Configuration conf;
  private InetSocketAddress nameNodeAddr;
  private volatile boolean shouldRun;
  private HttpServer2 infoServer;

  private Collection<URI> checkpointDirs;
  private List<URI> checkpointEditsDirs;

  private CheckpointConf checkpointConf;
  private FSNamesystem namesystem;

  private Thread checkpointThread;
  private ObjectName nameNodeStatusBeanName;
  private String legacyOivImageDir;

  @Override
  public String toString() {
    return getClass().getSimpleName() + " Status" 
      + "\nName Node Address      : " + nameNodeAddr
      + "\nStart Time             : " + new Date(starttime)
      + "\nLast Checkpoint        : " + (lastCheckpointTime == 0? "--":
        new Date(lastCheckpointWallclockTime))
      + " (" + ((Time.monotonicNow() - lastCheckpointTime) / 1000)
      + " seconds ago)"
      + "\nCheckpoint Period      : " + checkpointConf.getPeriod() + " seconds"
      + "\nCheckpoint Transactions: " + checkpointConf.getTxnCount()
      + "\nCheckpoint Dirs        : " + checkpointDirs
      + "\nCheckpoint Edits Dirs  : " + checkpointEditsDirs;
  }

  @VisibleForTesting
  FSImage getFSImage() {
    return checkpointImage;
  }

  @VisibleForTesting
  int getMergeErrorCount() {
    return checkpointImage.getMergeErrorCount();
  }

  @VisibleForTesting
  public FSNamesystem getFSNamesystem() {
    return namesystem;
  }
  
  @VisibleForTesting
  void setFSImage(CheckpointStorage image) {
    this.checkpointImage = image;
  }
  
  @VisibleForTesting
  NamenodeProtocol getNameNode() {
    return namenode;
  }
  
  @VisibleForTesting
  void setNameNode(NamenodeProtocol namenode) {
    this.namenode = namenode;
  }

  /**
   * 构造函数：创建到主NameNode的连接，初始化SecondaryNameNode
   * @param conf Hadoop配置对象
   * @throws IOException 初始化失败时抛出异常
   */
  public SecondaryNameNode(Configuration conf)  throws IOException {
    this(conf, new CommandLineOpts());
  }
  
  /**
   * 构造函数：带命令行参数的SecondaryNameNode初始化
   * @param conf Hadoop配置对象
   * @param commandLineOpts 命令行参数解析结果
   * @throws IOException 初始化失败时抛出异常
   */
  public SecondaryNameNode(Configuration conf,
      CommandLineOpts commandLineOpts) throws IOException {
    try {
      String nsId = DFSUtil.getSecondaryNameServiceId(conf);
      // HA集群不允许使用SecondaryNameNode，检查点由Standby NameNode负责
      if (HAUtil.isHAEnabled(conf, nsId)) {
        throw new IOException(
            "Cannot use SecondaryNameNode in an HA cluster." +
            " The Standby Namenode will perform checkpointing.");
      }
      NameNode.initializeGenericKeys(conf, nsId, null);
      initialize(conf, commandLineOpts);
    } catch (IOException e) {
      shutdown();
      throw e;
    } catch (HadoopIllegalArgumentException e) {
      shutdown();
      throw e;
    }
  }
  
  /**
   * 获取SecondaryNameNode的HTTP服务地址
   * @param conf Hadoop配置对象
   * @return 解析后的HTTP服务地址
   */
  public static InetSocketAddress getHttpAddress(Configuration conf) {
    return NetUtils.createSocketAddr(conf.getTrimmed(
        DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_DEFAULT));
  }
  
  /**
   * 初始化SecondaryNameNode的各项资源：安全认证、 metrics、RPC连接、存储目录等
   * @param conf Hadoop配置对象
   * @param commandLineOpts 命令行参数解析结果
   * @throws IOException 初始化过程中IO错误抛出异常
   */
  private void initialize(final Configuration conf,
      CommandLineOpts commandLineOpts) throws IOException {
    final InetSocketAddress infoSocAddr = getHttpAddress(conf);
    final String infoBindAddress = infoSocAddr.getHostName();
    UserGroupInformation.setConfiguration(conf);
    // 安全开启时登录Kerberos
    if (UserGroupInformation.isSecurityEnabled()) {
      SecurityUtil.login(conf,
          DFSConfigKeys.DFS_SECONDARY_NAMENODE_KEYTAB_FILE_KEY,
          DFSConfigKeys.DFS_SECONDARY_NAMENODE_KERBEROS_PRINCIPAL_KEY, infoBindAddress);
    }
    // 初始化JVM metrics
    DefaultMetricsSystem.initialize("SecondaryNameNode");
    JvmMetrics.create("SecondaryNameNode",
        conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY),
        DefaultMetricsSystem.instance());

    // 创建到主NameNode的连接
    shouldRun = true;
    nameNodeAddr = NameNode.getServiceAddress(conf, true);

    this.conf = conf;
    // 创建非HA模式的主NameNode代理
    this.namenode = NameNodeProxies.createNonHAProxy(conf, nameNodeAddr, 
        NamenodeProtocol.class, UserGroupInformation.getCurrentUser(),
        true).getProxy();

    // 初始化检查点存储目录
    fsName = getInfoServer();
    checkpointDirs = FSImage.getCheckpointDirs(conf,
                                  "/tmp/hadoop/dfs/namesecondary");
    checkpointEditsDirs = FSImage.getCheckpointEditsDirs(conf, 
                                  "/tmp/hadoop/dfs/namesecondary");    
    checkpointImage = new CheckpointStorage(conf, checkpointDirs, checkpointEditsDirs);
    checkpointImage.recoverCreate(commandLineOpts.shouldFormat());
    checkpointImage.deleteTempEdits();
    
    namesystem = new FSNamesystem(conf, checkpointImage, true);

    // 关闭配额检查，SecondaryNameNode不需要检查配额
    namesystem.dir.disableQuotaChecks();

    // 从配置加载检查点调度参数
    checkpointConf = new CheckpointConf(conf);
    // 注册JMX监控Bean
    nameNodeStatusBeanName = MBeans.register("SecondaryNameNode",
            "SecondaryNameNodeInfo", this);

    legacyOivImageDir = conf.get(
        DFSConfigKeys.DFS_NAMENODE_LEGACY_OIV_IMAGE_DIR_KEY);

    LOG.info("Checkpoint Period   :" + checkpointConf.getPeriod() + " secs "
        + "(" + checkpointConf.getPeriod() / 60 + " min)");
    LOG.info("Log Size Trigger    :" + checkpointConf.getTxnCount() + " txns");
  }

  /**
   * 等待信息服务器退出，阻塞直到服务停止
   */
  private void join() {
    try {
      infoServer.join();
    } catch (InterruptedException ie) {
      LOG.debug("Exception ", ie);
    }
  }

  /**
   * 关闭SecondaryNameNode，释放所有资源
   */
  public void shutdown() {
    shouldRun = false;
    // 中断检查点线程并等待退出
    if (checkpointThread != null) {
      checkpointThread.interrupt();
      try {
        checkpointThread.join(10000);
      } catch (InterruptedException e) {
        LOG.info("Interrupted waiting to join on checkpointer thread");
        Thread.currentThread().interrupt(); // 保持中断状态
      }
    }
    // 停止HTTP信息服务器
    try {
      if (infoServer != null) {
        infoServer.stop();
        infoServer = null;
      }
    } catch (Exception e) {
      LOG.warn("Exception shutting down SecondaryNameNode", e);
    }
    // 注销JMX监控Bean
    if (nameNodeStatusBeanName != null) {
      MBeans.unregister(nameNodeStatusBeanName);
      nameNodeStatusBeanName = null;
    }
    // 关闭检查点存储
    try {
      if (checkpointImage != null) {
        checkpointImage.close();
        checkpointImage = null;
      }
    } catch(IOException e) {
      LOG.warn("Exception while closing CheckpointStorage", e);
    }
    // 关闭文件系统命名空间
    if (namesystem != null) {
      namesystem.shutdown();
      namesystem = null;
    }
  }

  @Override
  public void run() {
    // 使用登录用户身份执行检查点任务
    SecurityUtil.doAsLoginUserOrFatal(
        new PrivilegedAction<Object>() {
        @Override
        public Object run() {
          doWork();
          return null;
        }
      });
  }
  /**
   * 检查点主循环：定期轮询主NameNode，满足检查点条件时触发检查点
   */
  public void doWork() {
    // 每checkPeriod秒轮询一次，检查是否需要检查点
    long period = checkpointConf.getCheckPeriod();
    int maxRetries = checkpointConf.getMaxRetriesOnMergeError();

    while (shouldRun) {
      try {
        Thread.sleep(1000 * period);
      } catch (InterruptedException ie) {
        // 忽略中断，继续循环，检查shouldRun后退出
      }
      if (!shouldRun) {
        break;
      }
      try {
        // 安全开启时重新登录Kerberos，刷新票据
        if(UserGroupInformation.isSecurityEnabled())
          UserGroupInformation.getCurrentUser().checkTGTAndReloginFromKeytab();
        
        final long monotonicNow = Time.monotonicNow();
        final long now = Time.now();

        // 满足事务数阈值或时间间隔阈值，触发检查点
        if (shouldCheckpointBasedOnCount() ||
            monotonicNow >= lastCheckpointTime + 1000 * checkpointConf.getPeriod()) {
          doCheckpoint();
          lastCheckpointTime = monotonicNow;
          lastCheckpointWallclockTime = now;
        }
      } catch (IOException e) {
        LOG.error("Exception in doCheckpoint", e);
        e.printStackTrace();
        // 合并错误超过最大重试次数，直接退出，避免产生大量未检查日志
        if (checkpointImage.getMergeErrorCount() > maxRetries) {
          LOG.error("Merging failed " +
              checkpointImage.getMergeErrorCount() + " times.");
          terminate(1);
        }
      } catch (Throwable e) {
        LOG.error("Throwable Exception in doCheckpoint", e);
        e.printStackTrace();
        terminate(1, e);
      }
    }
  }

  /**
   * 从主NameNode下载最新fsimage和edits日志文件到本地检查点存储
   * @param nnHostPort 主NameNodeHTTP地址
   * @param dstImage 目标检查点存储对象
   * @param sig 主NameNode返回的检查点签名
   * @param manifest 需要下载的编辑日志清单
   * @return true表示下载了新镜像需要加载，false不需要重新加载
   * @throws IOException 下载过程IO错误抛出异常
   */
  static boolean downloadCheckpointFiles(
      final URL nnHostPort,
      final FSImage dstImage,
      final CheckpointSignature sig,
      final RemoteEditLogManifest manifest
  ) throws IOException {
    
    // 清单校验：没有找到需要下载的日志直接报错
    if (manifest.getLogs().isEmpty()) {
      throw new IOException("Found no edit logs to download on NN since txid " 
          + sig.mostRecentCheckpointTxId);
    }
    
    long expectedTxId = sig.mostRecentCheckpointTxId + 1;
    // 校验第一个日志起始事务ID是否符合预期
    if (manifest.getLogs().get(0).getStartTxId() != expectedTx) {
      throw new IOException("Bad edit log manifest (expected txid = " +