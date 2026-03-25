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
package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.util.DiskChecker;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.Time.now;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.tracing.Tracer;
import org.eclipse.jetty.util.ajax.JSON;

import javax.management.ObjectName;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.InetSocketAddress;/**
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
package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.util.DiskChecker;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.Time.now;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.tracing.Tracer;
import org.eclipse.jetty.util.ajax.JSON;

import javax.management.ObjectName;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @fileoverview JournalNode是HDFS高可用QJM（Quorum Journal Manager）架构中的日志节点守护进程，
 * 允许使用QJM的NameNode远程记录和检索编辑日志，作为参与仲裁协议的轻量级节点，包装本地编辑日志目录。
 * 核心职责是存储HDFS的编辑日志，为多个NameNode提供共享的日志存储，支持HA架构下的元数据一致性。
 */
@InterfaceAudience.Private
public class JournalNode implements Tool, Configurable, JournalNodeMXBean {
  public static final Logger LOG = LoggerFactory.getLogger(JournalNode.class);
  private Configuration conf;
  private JournalNodeRpcServer rpcServer;
  private JournalNodeHttpServer httpServer;
  // 按journal编号存储journal实例映射
  private final Map<String, Journal> journalsById = Maps.newHashMap();
  // 按journal编号存储journal同步器实例映射
  private final Map<String, JournalNodeSyncer> journalSyncersById = Maps
      .newHashMap();
  private ObjectName journalNodeInfoBeanName;
  private String httpServerURI;
  // 本地存储目录列表
  private final ArrayList<File> localDir = Lists.newArrayList();
  Tracer tracer;
  // 节点启动时间戳
  private long startTime = 0;

  static {
    HdfsConfiguration.init();
  }
  
  /**
   * When stopped, the daemon will exit with this code. 
   */
  private int resultCode = 0;

  /**
   * 获取或创建指定编号的journal实例，支持集群联邦场景
   * @param jid journal唯一标识
   * @param nameServiceId 名称服务标识（联邦场景使用）
   * @param startOpt 启动选项
   * @return 已存在或新建的journal实例
   * @throws IOException 初始化journal失败时抛出
   */
  synchronized Journal getOrCreateJournal(String jid,
                                          String nameServiceId,
                                          StartupOption startOpt)
      throws IOException {
    QuorumJournalManager.checkJournalId(jid);
    
    Journal journal = journalsById.get(jid);
    if (journal == null) {
      // 获取journal对应的存储目录
      File logDir = getLogDir(jid, nameServiceId);
      LOG.info("Initializing journal in directory " + logDir);
      // 创建新的journal实例
      journal = new Journal(conf, logDir, jid, startOpt, new ErrorReporter());
      journalsById.put(jid, journal);
      // 如果开启了JournalNode同步功能，启动同步线程
      if (conf.getBoolean(
          DFSConfigKeys.DFS_JOURNALNODE_ENABLE_SYNC_KEY,
          DFSConfigKeys.DFS_JOURNALNODE_ENABLE_SYNC_DEFAULT)) {
        startSyncer(journal, jid, nameServiceId);
      }
    } else if (journalSyncersById.get(jid) != null &&
        !journalSyncersById.get(jid).isJournalSyncerStarted() &&
        !journalsById.get(jid).getTriedJournalSyncerStartedwithnsId() &&
        nameServiceId != null) {
      // 存在同步器但未启动，且首次为当前名称服务尝试启动，启动同步器
      startSyncer(journal, jid, nameServiceId);
    }


    return journal;
  }

  @VisibleForTesting
  /**
   * 根据journal编号获取对应的同步器实例，仅用于测试
   * @param jid journal唯一标识
   * @return 同步器实例
   */
  public JournalNodeSyncer getJournalSyncer(String jid) {
    return journalSyncersById.get(jid);
  }

  @VisibleForTesting
  /**
   * 获取指定journal同步器的启动状态，仅用于测试
   * @param jid journal唯一标识
   * @return true表示同步器已启动，false表示未启动或不存在
   */
  public boolean getJournalSyncerStatus(String jid) {
    if (journalSyncersById.get(jid) != null) {
      return journalSyncersById.get(jid).isJournalSyncerStarted();
    } else {
      return false;
    }
  }

  /**
   * 启动指定journal的同步器
   * @param journal journal实例
   * @param jid journal唯一标识
   * @param nameServiceId 名称服务标识
   */
  private void startSyncer(Journal journal, String jid, String nameServiceId) {
    JournalNodeSyncer jSyncer = journalSyncersById.get(jid);
    if (jSyncer == null) {
      // 同步器不存在则新建
      jSyncer = new JournalNodeSyncer(this, journal, jid, conf, nameServiceId);
      journalSyncersById.put(jid, jSyncer);
    }
    // 启动同步器
    jSyncer.start(nameServiceId);
  }

  @VisibleForTesting
  /**
   * 获取或创建journal（无名称服务标识场景），仅用于测试
   * @param jid journal唯一标识
   * @return journal实例
   * @throws IOException 初始化失败时抛出
   */
  public Journal getOrCreateJournal(String jid) throws IOException {
    return getOrCreateJournal(jid, null, StartupOption.REGULAR);
  }

  /**
   * 获取或创建journal（常规启动选项）
   * @param jid journal唯一标识
   * @param nameServiceId 名称服务标识
   * @return journal实例
   * @throws IOException 初始化失败时抛出
   */
  public Journal getOrCreateJournal(String jid,
                                    String nameServiceId)
      throws IOException {
    return getOrCreateJournal(jid, nameServiceId, StartupOption.REGULAR);
  }

  @Override
  /**
   * 设置JournalNode配置，解析存储目录并初始化追踪器
   * @param conf Hadoop配置对象
   */
  public void setConf(Configuration conf) {
    this.conf = conf;

    String journalNodeDir = null;
    Collection<String> nameserviceIds;

    // 获取名称服务列表
    nameserviceIds = conf.getTrimmedStringCollection(
        DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY);

    if (nameserviceIds.size() == 0) {
      // 未配置内部名称服务，读取公用名称服务配置
      nameserviceIds = conf.getTrimmedStringCollection(
          DFSConfigKeys.DFS_NAMESERVICES);
    }

    // 名称服务少于2个，说明不是联邦部署场景
    if (nameserviceIds.size() < 2) {
      // 遍历名称服务，检查是否配置了带名称服务后缀的编辑日志目录
      for (String nameService : nameserviceIds) {
        journalNodeDir = conf.get(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY +
        "." + nameService);
      }
      if (journalNodeDir == null) {
        // 未配置带后缀的目录，读取全局配置
        journalNodeDir = conf.get(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY,
            DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_DEFAULT);
      }
      // 添加到本地目录列表
      localDir.add(new File(journalNodeDir.trim()));
    }

    if (this.tracer == null) {
      // 初始化分布式追踪器
      this.tracer = new Tracer.Builder("JournalNode").
          conf(TraceUtils.wrapHadoopConf("journalnode.htrace", conf)).
          build();
    }
  }

  /**
   * 验证journal目录合法性，若目录不存在则创建
   * @param dir 待验证的目录
   * @throws IOException 目录检查失败时抛出
   */
  private static void validateAndCreateJournalDir(File dir)
      throws IOException {

    if (!dir.isAbsolute()) {
      throw new IllegalArgumentException(
          "Journal dir '" + dir + "' should be an absolute path");
    }
    DiskChecker.checkDir(dir);
  }

  @Override
  /**
   * 获取JournalNode配置
   * @return 配置对象
   */
  public Configuration getConf() {
    return conf;
  }

  @Override
  /**
   * Tool接口执行方法，启动JournalNode并等待退出
   * @param args 启动参数
   * @return 退出码
   * @throws Exception 启动失败时抛出
   */
  public int run(String[] args) throws Exception {
    start();
    return join();
  }

  /**
   * 启动JournalNode服务，开启RPC和HTTP服务监听
   * @throws IOException 启动失败时抛出
   */
  public void start() throws IOException {
    Preconditions.checkState(!isStarted(), "JN already running");

    try {

      // 验证所有已配置的存储目录
      for (File journalDir : localDir) {
        validateAndCreateJournalDir(journalDir);
      }
      // 初始化指标系统
      DefaultMetricsSystem.initialize("JournalNode");
      JvmMetrics.create("JournalNode",
          conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY),
          DefaultMetricsSystem.instance());

      // 获取RPC服务地址
      InetSocketAddress socAddr = JournalNodeRpcServer.getAddress(conf);
      // 安全登录（Kerberos认证）
      SecurityUtil.login(conf, DFSConfigKeys.DFS_JOURNALNODE_KEYTAB_FILE_KEY,
          DFSConfigKeys.DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY,
          socAddr.getHostName());

      // 注册JMX MBean
      registerJNMXBean();

      // 启动HTTP服务
      httpServer = new JournalNodeHttpServer(conf, this,
          getHttpServerBindAddress(conf));
      httpServer.start();

      // 保存HTTP服务地址
      httpServerURI = httpServer.getServerURI().toString();

      // 启动RPC服务
      rpcServer = new JournalNodeRpcServer(conf, this);
      rpcServer.start();
      // 记录启动时间
      startTime = now();
    } catch (IOException ioe) {
      // 启动失败，关闭节点并抛出异常
      LOG.error("Failed to start JournalNode.", ioe);
      this.stop(1);
      throw ioe;
    }
  }

  /**
   * 判断JournalNode是否已启动
   * @return true表示已启动，false表示未启动
   */
  public boolean isStarted() {
    return rpcServer != null;
  }

  /**
   * 获取IPC服务绑定的地址
   * @return IPC绑定地址
   */
  public InetSocketAddress getBoundIpcAddress() {
    return rpcServer.getAddress();
  }

  /**
   * 获取HTTP服务地址
   * @return HTTP服务URI字符串
   */
  public String getHttpServerURI() {
    return httpServerURI;
  }

  /**
   * 停止JournalNode守护进程
   * @param rc 退出码，非零表示异常退出
   */
  public void stop(int rc) {
    this.resultCode = rc;

    // 停止所有journal同步器
    for (JournalNodeSyncer jSyncer : journalSyncersById.values()) {
      jSyncer.stopSync();
    }

    // 停止RPC服务
    if (rpcServer != null) { 
      rpcServer.stop();
    }

    // 停止HTTP服务
    if (httpServer != null) {
      try {
        httpServer.stop();
      } catch (IOException ioe) {
        LOG.warn("Unable to stop HTTP server for " + this, ioe);
      }
    }
    
    // 关闭所有journal实例
    for (Journal j : journalsById.values()) {
      IOUtils.cleanupWithLogger(LOG, j);
    }

    // 关闭指标系统
    DefaultMetricsSystem.shutdown();

    // 取消注册JMX MBean
    if (journalNodeInfoBeanName != null) {
      MBeans.unregister(journalNodeInfoBeanName);
      journalNodeInfoBeanName = null;
    }
    // 关闭追踪器
    if (tracer != null) {
      tracer.close();
      tracer = null;
    }
  }

  /**
   * 等待守护进程退出，返回退出码
   * @return 退出码
   * @throws InterruptedException 等待被中断时抛出
   */
  int join() throws InterruptedException {
    if (rpcServer != null) {
      rpcServer.join();
    }
    return resultCode;
  }
  
  /**
   * 停止进程并等待退出
   * @param rc 退出码
   * @throws InterruptedException 等待被中断时抛出
   */
  public void stopAndJoin(int rc) throws InterruptedException {
    stop(rc);
    join();
  }

  /**
   * 获取指定journal对应的本地存储目录
   * @param jid journal唯一标识
   * @param nameServiceId 名称服务标识
   * @return journal存储目录对象
   * @throws IOException 目录验证失败时抛出
   */
  private File getLogDir(String jid, String nameServiceId) throws IOException{
    String dir = null;
    if (nameServiceId != null) {
      // 读取带名称服务后缀的目录配置
      dir = conf.get(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY + "." +
          nameServiceId);
    }
    if (dir == null) {
      // 未配置则读取全局目录配置
      dir = conf.get(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY,
          DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_DEFAULT);
    }

    File journalDir = new File(dir.trim());
    if (!localDir.contains(journalDir)) {
      // 联邦场景下新增目录，需要验证并添加到本地目录列表
      validateAndCreateJournalDir(journalDir);
      localDir.add(journalDir);
    }

    Preconditions.checkArgument(jid != null &&
        !jid.isEmpty(),
        "bad journal identifier: %s", jid);
    assert jid != null;
    // 在基础目录下按journal id生成子目录
    return new File(journalDir, jid);
  }


  @Override // JournalNodeMXBean
  /**
   * 获取所有journal的格式化状态，通过JMX暴露
   * @return JSON格式的状态字符串
   */
  public String getJournalsStatus() {
    // jid:{Formatted:True/False}
    Map<String, Map<String, String>> status = 
        new HashMap<String, Map<String, String>>();
    synchronized (this) {
      // 收集已初始化journal的状态
      for (Map.Entry<String, Journal> entry : journalsById.entrySet()) {
        Map<String, String> jMap = new HashMap<String, String>();
        jMap.put("Formatted", Boolean.toString(entry.getValue().isFormatted()));
        status.put(entry.getKey(), jMap);
      }
    }
    
    // 处理已存在目录但未初始化