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

import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：外部存储策略满足器（SPS）服务的启动入口类，负责独立模式下启动SPS服务，完成安全登录、NameNode连接初始化并启动SPS主线程
 * 
 * 该类用于HDFS的外部独立部署SPS服务场景，将SPS从NameNode进程独立出来运行，提升存储策略执行的稳定性和资源隔离能力。
 * 核心功能：负责初始化SPS服务所需资源，建立与NameNode的连接，并启动存储策略满足器主服务。
 */
@InterfaceAudience.Private
public final class ExternalStoragePolicySatisfier {
  /** 日志实例，用于记录服务启动、运行过程中的日志信息 */
  public static final Logger LOG = LoggerFactory.getLogger(ExternalStoragePolicySatisfier.class);

  private ExternalStoragePolicySatisfier() {
    // This is just a class to start and run external sps.
  }

  /**
   * 外部SPS服务的主入口方法，负责启动并运行整个外部存储策略满足器服务
   * @param args 启动参数，目前直接透传日志输出
   * @throws Exception 启动过程中抛出任何异常都会导致服务终止
   */
  public static void main(String[] args) throws Exception {
    NameNodeConnector nnc = null;
    ExternalSPSContext context = null;
    try {
      // 输出服务启动日志信息
      StringUtils.startupShutdownMessage(StoragePolicySatisfier.class, args,
          LOG);
      // 创建HDFS配置对象，加载配置
      HdfsConfiguration spsConf = new HdfsConfiguration();
      // 安全登录，使用配置的keytab和主体完成Kerberos认证
      secureLogin(spsConf);
      // 创建存储策略满足器实例
      StoragePolicySatisfier sps = new StoragePolicySatisfier(spsConf);
      // 创建并获取NameNode连接器，用于和NameNode通信
      nnc = getNameNodeConnector(spsConf);

      // 创建外部SPS上下文，绑定SPS实例和NameNode连接器
      context = new ExternalSPSContext(sps, nnc);
      // 初始化SPS服务，注入上下文
      sps.init(context);
      // 以外部独立模式启动SPS服务
      sps.start(StoragePolicySatisfierMode.EXTERNAL);
      // 初始化SPS监控指标
      context.initMetrics(sps);
      // 等待SPS服务线程终止，保持服务运行
      if (sps != null) {
        sps.join();
      }
    } catch (Throwable e) {
      LOG.error("Failed to start storage policy satisfier.", e);
      terminate(1, e);
    } finally {
      // 服务退出时关闭NameNode连接器，释放资源
      if (nnc != null) {
        nnc.close();
      }
      // 服务退出时关闭监控指标，释放资源
      if (context!= null) {
        if (context.getSpsBeanMetrics() != null) {
          context.closeMetrics();
        }
      }
    }
  }

  /**
   * 完成外部SPS服务的安全认证登录，支持Kerberos认证场景
   * @param conf SPS服务配置对象，包含认证相关配置
   * @throws IOException 登录过程中IO异常或认证失败时抛出
   */
  private static void secureLogin(Configuration conf)
      throws IOException {
    UserGroupInformation.setConfiguration(conf);
    // 获取SPS服务绑定的地址配置
    String addr = conf.get(DFSConfigKeys.DFS_SPS_ADDRESS_KEY,
        DFSConfigKeys.DFS_SPS_ADDRESS_DEFAULT);
    // 解析地址为InetSocketAddress对象
    InetSocketAddress socAddr = NetUtils.createSocketAddr(addr, 0,
        DFSConfigKeys.DFS_SPS_ADDRESS_KEY);
    // 使用配置的keytab和Kerberos主体完成登录
    SecurityUtil.login(conf, DFSConfigKeys.DFS_SPS_KEYTAB_FILE_KEY,
        DFSConfigKeys.DFS_SPS_KERBEROS_PRINCIPAL_KEY,
        socAddr.getHostName());
  }

  /**
   * 创建并初始化NameNode连接器，负责建立和NameNode的RPC连接，支持失败重试
   * @param conf 配置对象，包含NameNode地址信息
   * @return 返回连接成功的NameNode连接器实例
   * @throws InterruptedException 重试过程中线程被中断时抛出
   */
  public static NameNodeConnector getNameNodeConnector(Configuration conf)
      throws InterruptedException {
    // 从配置中获取NameNode内部RPC URI列表
    final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    // 外部SPS的路径ID，用于ZNode锁抢占
    final Path externalSPSPathId = HdfsServerConstants.MOVER_ID_PATH;
    // 获取当前服务名称，用于锁标识
    String serverName = ExternalStoragePolicySatisfier.class.getSimpleName();
    // 循环重试连接，直到连接成功或检测到冲突退出
    while (true) {
      try {
        // 创建NameNode连接器集合，支持多NS场景，这里取第一个
        final List<NameNodeConnector> nncs = NameNodeConnector
            .newNameNodeConnectors(namenodes,
                serverName,
                externalSPSPathId, conf,
                NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS);
        return nncs.get(0);
      } catch (IOException e) {
        LOG.warn("Failed to connect with namenode", e);
        // 如果检测到已经有另一个SPS实例运行，则直接退出
        if (e.getMessage().equals("Another " + serverName + " is running.")) {
          ExitUtil.terminate(-1,
              "Exit immediately because another " + serverName + " is running");
        }
        // 连接失败等待3秒后重试
        Thread.sleep(3000); // retry the connection after few secs
      }
    }
  }
}