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
package org.apache.hadoop.hdfs.tools;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.HealthMonitor;
import org.apache.hadoop.ha.ZKFailoverController;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos.ActiveNodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;

/**
 * HDFS高可用基于ZooKeeper的故障转移控制器实现。
 * 负责管理NameNode的主备切换，通过ZooKeeper实现自动故障转移，保障HDFS服务高可用性。
 * 核心职责包括监控本地NameNode健康状态、与ZooKeeper协同选举、触发故障转移流程。
 */
@InterfaceAudience.Private
public class DFSZKFailoverController extends ZKFailoverController {

  private static final Logger LOG =
      LoggerFactory.getLogger(DFSZKFailoverController.class);
  /** HDFS管理员访问控制列表 */
  private final AccessControlList adminAcl;
  /* the same as superclass's localTarget, but with the more specfic NN type */
  /** 本地NameNode高可用服务目标，子类化的具体实现 */
  private final NNHAServiceTarget localNNTarget;

  // This is used only for unit tests
  /** 标记是否已获取线程转储，仅用于单元测试 */
  private boolean isThreadDumpCaptured = false;

  @Override
  /**
   * 将ZooKeeper中存储的字节数据反序列化为HAServiceTarget对象
   * @param data ZooKeeper中存储的序列化数据
   * @return 反序列化得到的NameNode高可用服务目标
   */
  protected HAServiceTarget dataToTarget(byte[] data) {
    ActiveNodeInfo proto;
    try {
      proto = ActiveNodeInfo.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Invalid data in ZK: " +
          StringUtils.byteToHexString(data));
    }
    NNHAServiceTarget ret = new NNHAServiceTarget(
        conf, proto.getNameserviceId(), proto.getNamenodeId());
    InetSocketAddress addressFromProtobuf = new InetSocketAddress(
        proto.getHostname(), proto.getPort());
    
    if (!addressFromProtobuf.equals(ret.getAddress())) {
      throw new RuntimeException("Mismatched address stored in ZK for " +
          ret + ": Stored protobuf was " + proto + ", address from our own " +
          "configuration for this NameNode was " + ret.getAddress());
    }
    
    ret.setZkfcPort(proto.getZkfcPort());
    return ret;
  }

  @Override
  /**
   * 将HAServiceTarget对象序列化为字节数据，用于写入ZooKeeper
   * @param target 需要序列化的NameNode高可用服务目标
   * @return 序列化后的字节数组
   */
  protected byte[] targetToData(HAServiceTarget target) {
    InetSocketAddress addr = target.getAddress();

    return ActiveNodeInfo.newBuilder()
      .setHostname(addr.getHostName())
      .setPort(addr.getPort())
      .setZkfcPort(target.getZKFCAddress().getPort())
      .setNameserviceId(localNNTarget.getNameServiceId())
      .setNamenodeId(localNNTarget.getNameNodeId())
      .build()
      .toByteArray();
  }
  
  @Override
  /**
   * 获取ZKFC RPC服务需要绑定的地址
   * @return 需要绑定的RPC地址
   */
  protected InetSocketAddress getRpcAddressToBindTo() {
    int zkfcPort = getZkfcPort(conf);
    String zkfcBindAddr = getZkfcServerBindHost(conf);
    if (zkfcBindAddr == null || zkfcBindAddr.isEmpty()) {
      zkfcBindAddr = localTarget.getAddress().getAddress().getHostAddress();
    }
    return new InetSocketAddress(zkfcBindAddr, zkfcPort);
  }

  @Override
  /**
   * 获取HDFS安全权限策略提供器
   * @return HDFS策略提供器实例
   */
  protected PolicyProvider getPolicyProvider() {
    return new HDFSPolicyProvider();
  }

  /**
   * 从配置中获取ZKFC服务端口
   * @param conf Hadoop配置对象
   * @return ZKFC服务端口
   */
  static int getZkfcPort(Configuration conf) {
    return conf.getInt(DFSConfigKeys.DFS_HA_ZKFC_PORT_KEY,
        DFSConfigKeys.DFS_HA_ZKFC_PORT_DEFAULT);
  }

  /**
   * Given a configuration get the bind host that could be used by ZKFC.
   * We derive it from NN service rpc bind host or NN rpc bind host.
   *
   * @param conf input configuration
   * @return the bind host address found in conf
   */
  /**
   * 从配置中获取ZKFC服务绑定地址，优先使用NameNode绑定地址配置
   * @param conf Hadoop配置对象
   * @return ZKFC绑定主机地址
   */
  private static String getZkfcServerBindHost(Configuration conf) {
    String addr = conf.getTrimmed(
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY);
    if (addr == null || addr.isEmpty()) {
      addr = conf.getTrimmed(DFSConfigKeys.DFS_NAMENODE_RPC_BIND_HOST_KEY);
    }
    return addr;
  }

  /**
   * 根据配置创建DFSZKFailoverController实例，完成初始化配置检查
   * @param conf Hadoop配置对象
   * @return 初始化完成的DFSZKFailoverController实例
   */
  public static DFSZKFailoverController create(Configuration conf) {
    Configuration localNNConf = DFSHAAdmin.addSecurityConfiguration(conf);
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);

    if (!HAUtil.isHAEnabled(localNNConf, nsId)) {
      throw new HadoopIllegalArgumentException(
          "HA is not enabled for this namenode.");
    }
    String nnId = HAUtil.getNameNodeId(localNNConf, nsId);
    if (nnId == null) {
      String msg = "Could not get the namenode ID of this node. " +
          "You may run zkfc on the node other than namenode.";
      throw new HadoopIllegalArgumentException(msg);
    }
    NameNode.initializeGenericKeys(localNNConf, nsId, nnId);
    DFSUtil.setGenericConf(localNNConf, nsId, nnId, ZKFC_CONF_KEYS);
    
    NNHAServiceTarget localTarget = new NNHAServiceTarget(
        localNNConf, nsId, nnId);
    return new DFSZKFailoverController(localNNConf, localTarget);
  }

  /**
   * 私有构造函数，通过create方法创建实例
   * @param conf Hadoop配置对象
   * @param localTarget 本地NameNode高可用服务目标
   */
  private DFSZKFailoverController(Configuration conf,
      NNHAServiceTarget localTarget) {
    super(conf, localTarget);
    this.localNNTarget = localTarget;
    // 初始化管理员访问控制列表
    adminAcl = new AccessControlList(
        conf.get(DFSConfigKeys.DFS_ADMIN, " "));
    LOG.info("Failover controller configured for NameNode " +
        localTarget);
}
  
  
  @Override
  /**
   * 初始化RPC服务，设置本地NameNode的ZKFC端口
   * @throws IOException 初始化失败抛出IO异常
   */
  protected void initRPC() throws IOException {
    super.initRPC();
    localNNTarget.setZkfcPort(rpcServer.getAddress().getPort());
  }

  @Override
  /**
   * 使用ZKFC用户完成Kerberos登录认证
   * @throws IOException 登录失败抛出IO异常
   */
  public void loginAsFCUser() throws IOException {
    InetSocketAddress socAddr = DFSUtilClient.getNNAddress(conf);
    SecurityUtil.login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY,
        DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, socAddr.getHostName());
  }
  
  @Override
  /**
   * 获取ZooKeeper父节点下当前节点所在的命名空间，作为隔离范围
   * @return 当前节点所在的命名服务ID
   */
  protected String getScopeInsideParentNode() {
    return localNNTarget.getNameServiceId();
  }

  /**
   * DFSZKFailoverController入口方法，启动ZKFC服务
   * @param args 命令行参数
   * @throws Exception 启动过程中抛出任何异常
   */
  public static void main(String args[])
      throws Exception {
    StringUtils.startupShutdownMessage(DFSZKFailoverController.class,
        args, LOG);
    if (DFSUtil.parseHelpArgument(args, 
        ZKFailoverController.USAGE, System.out, true)) {
      System.exit(0);
    }
    
    GenericOptionsParser parser = new GenericOptionsParser(
        new HdfsConfiguration(), args);
    try {
      DFSZKFailoverController zkfc = DFSZKFailoverController.create(
          parser.getConfiguration());
      System.exit(zkfc.run(parser.getRemainingArgs()));
    } catch (Throwable t) {
      LOG.error("DFSZKFailOverController exiting due to earlier exception "
          + t);
      terminate(1, t);
    }
  }

  @Override
  /**
   * 检查当前用户是否拥有ZKFC RPC管理员访问权限
   * @throws IOException 检查过程抛出IO异常
   * @throws AccessControlException 权限不满足抛出访问控制异常
   */
  protected void checkRpcAdminAccess() throws IOException, AccessControlException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    UserGroupInformation zkfcUgi = UserGroupInformation.getLoginUser();
    if (adminAcl.isUserAllowed(ugi) ||
        ugi.getShortUserName().equals(zkfcUgi.getShortUserName())) {
      LOG.info("Allowed RPC access from " + ugi + " at " + Server.getRemoteAddress());
      return;
    }
    String msg = "Disallowed RPC access from " + ugi + " at " +
        Server.getRemoteAddress() + ". Not listed in " + DFSConfigKeys.DFS_ADMIN; 
    LOG.warn(msg);
    throw new AccessControlException(msg);
  }

  /**
   * capture local NN's thread dump and write it to ZKFC's log.
   * 当NameNode不健康时，获取本地NameNode的线程转储并记录到ZKFC日志，帮助故障诊断
   */
  private void getLocalNNThreadDump() {
    isThreadDumpCaptured = false;
    // We use the same timeout value for both connection establishment
    // timeout and read timeout.
    int httpTimeOut = conf.getInt(
        DFSConfigKeys.DFS_HA_ZKFC_NN_HTTP_TIMEOUT_KEY,
        DFSConfigKeys.DFS_HA_ZKFC_NN_HTTP_TIMEOUT_KEY_DEFAULT);
    if (httpTimeOut == 0) {
      // 超时设置为0表示关闭该功能
      return;
    }
    try {
      // 构造NameNode stacks接口地址
      String stacksUrl = DFSUtil.getInfoServer(localNNTarget.getAddress(),
          conf, DFSUtil.getHttpClientScheme(conf)) + "/stacks";
      URL url = new URL(stacksUrl);
      HttpURLConnection conn = (HttpURLConnection)url.openConnection();
      conn.setReadTimeout(httpTimeOut);
      conn.setConnectTimeout(httpTimeOut);
      conn.connect();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copyBytes(conn.getInputStream(), out, 4096, true);
      StringBuilder localNNThreadDumpContent =
          new StringBuilder("-- Local NN thread dump -- \n");
      localNNThreadDumpContent.append(out)
          .append("\n -- Local NN thread dump -- ");
      LOG.info("{}", localNNThreadDumpContent.toString());
      isThreadDumpCaptured = true;
    } catch (IOException e) {
      LOG.warn("Can't get local NN thread dump due to " + e.getMessage());
    }
  }

  @Override
  /**
   * 设置最新的NameNode健康状态，当状态变为不健康时，自动获取线程转储帮助诊断
   * @param newState 新的健康状态
   */
  protected synchronized void setLastHealthState(HealthMonitor.State newState) {
    super.setLastHealthState(newState);
    // 当NameNode变为不响应或不健康状态时，获取线程转储
    if (getLastHealthState() == HealthMonitor.State.SERVICE_NOT_RESPONDING ||
        getLastHealthState() == HealthMonitor.State.SERVICE_UNHEALTHY) {
      getLocalNNThreadDump();
    }
  }

  @VisibleForTesting
  /**
   * 获取线程转储是否已被捕获，仅用于单元测试
   * @return 线程转储是否已捕获
   */
  boolean isThreadDumpCaptured() {
    return isThreadDumpCaptured;
  }

  @Override
  /**
   * 获取同一命名空间下其他所有NameNode的高可用服务目标列表
   * @return 其他NameNode目标列表
   */
  public List<HAServiceTarget> getAllOtherNodes() {
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    List<String> otherNn = HAUtil.getNameNodeIdOfOtherNodes(conf, nsId);

    List<HAServiceTarget> targets = new ArrayList<HAServiceTarget>(otherNn.size());
    for (String nnId : otherNn) {
      targets.add(new NNHAServiceTarget(conf, nsId, nnId));
    }
    return targets;
  }

  @Override
  /**
   * 检查ZooKeeper客户端是否启用SSL
   * @return 是否启用SSL
   */
  protected boolean isSSLEnabled() {
    return conf.getBoolean(CommonConfigurationKeys.ZK_CLIENT_SSL_ENABLED,
        conf.getBoolean(DFSConfigKeys.ZK_CLIENT_SSL_ENABLED,
            DFSConfigKeys.DEFAULT_ZK_CLIENT_SSL_ENABLED));
  }
}