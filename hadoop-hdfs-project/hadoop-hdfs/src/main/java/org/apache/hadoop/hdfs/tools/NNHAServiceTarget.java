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

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.BadFencingConfigurationException;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.NodeFencer;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetUtils;

import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICES;

/**
 * @file NNHAServiceTarget.java
 * @brief HDFS高可用场景中，表示NameNode作为高可用管理命令的操作目标
 *
 * 该类继承自HAServiceTarget，封装了目标NameNode的地址、配置和隔离相关信息，
 * 供故障转移等高可用管理命令使用，用于对指定NameNode执行管理操作。
 */
@InterfaceAudience.Private
public class NNHAServiceTarget extends HAServiceTarget {

  // 隔离脚本环境中添加的键名
  private static final String NAMESERVICE_ID_KEY = "nameserviceid";
  private static final String NAMENODE_ID_KEY = "namenodeid";
  
  private final InetSocketAddress addr;
  private final InetSocketAddress lifelineAddr;
  private InetSocketAddress zkfcAddr;
  private NodeFencer fencer;
  private BadFencingConfigurationException fenceConfigError;
  private HdfsConfiguration targetConf;
  private String nnId;
  private String nsId;
  private boolean autoFailoverEnabled;

  /**
   * 构造目标NameNode的高可用服务目标，从配置中查找地址信息
   * @param conf HDFS配置对象
   * @param nsId 当前NameNode所属的名称服务ID
   * @param nnId 当前NameNode的节点ID
   */
  public NNHAServiceTarget(Configuration conf,
      String nsId, String nnId) {
    initializeNnConfig(conf, nsId, nnId);

    String serviceAddr =
        DFSUtil.getNamenodeServiceAddr(targetConf, nsId, nnId);
    if (serviceAddr == null) {
      throw new IllegalArgumentException(
          "Unable to determine service address for namenode '" + nnId + "'");
    }

    this.addr = NetUtils.createSocketAddr(serviceAddr,
        HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT);

    String lifelineAddrStr =
        DFSUtil.getNamenodeLifelineAddr(targetConf, nsId, nnId);
    this.lifelineAddr = (lifelineAddrStr != null) ?
        NetUtils.createSocketAddr(lifelineAddrStr) : null;

    initializeFailoverConfig();
  }

  /**
   * 构造目标NameNode的高可用服务目标，直接使用提供的地址无需从配置查找
   * @param conf HDFS配置对象
   * @param nsId 当前NameNode所属的名称服务ID
   * @param nnId 当前NameNode的节点ID
   * @param addr 提供的服务地址
   * @param lifelineAddr 提供的生命线地址
   */
  public NNHAServiceTarget(Configuration conf,
      String nsId, String nnId,
      String addr, String lifelineAddr) {
    initializeNnConfig(conf, nsId, nnId);

    this.addr = NetUtils.createSocketAddr(addr);
    this.lifelineAddr = NetUtils.createSocketAddr(lifelineAddr);

    initializeFailoverConfig();
  }

  /**
   * 初始化目标NameNode的配置，根据nsId和nnId设置对应配置项
   * @param conf 原始配置对象
   * @param providedNsId 传入的名称服务ID
   * @param providedNnId 传入的NameNode节点ID
   */
  private void initializeNnConfig(Configuration conf,
      String providedNsId, String providedNnId) {
    Preconditions.checkNotNull(providedNnId);

    if (providedNsId == null) {
      // 自动从配置中推断唯一名称服务ID
      providedNsId = DFSUtil.getOnlyNameServiceIdOrNull(conf);
      if (providedNsId == null) {
        String errorString = "Unable to determine the name service ID.";
        String[] dfsNames = conf.getStrings(DFS_NAMESERVICES);
        if ((dfsNames != null) && (dfsNames.length > 1)) {
          // 多名称服务场景下无法自动推断，提示用户指定
          errorString = "Unable to determine the name service ID. " +
              "This is an HA configuration with multiple name services " +
              "configured. " + DFS_NAMESERVICES + " is set to " +
              Arrays.toString(dfsNames) + ". Please re-run with the -ns option.";
        }
        throw new IllegalArgumentException(errorString);
      }
    }

    // 复制配置，根据目标节点覆盖对应配置项，使用目标节点配置而非运行节点配置
    this.targetConf = new HdfsConfiguration(conf);
    NameNode.initializeGenericKeys(targetConf, providedNsId, providedNnId);

    this.nsId = providedNsId;
    this.nnId = providedNnId;
  }

  /**
   * 初始化故障转移相关配置，包括自动故障转移、ZKFC地址和隔离器配置
   */
  private void initializeFailoverConfig() {
    this.autoFailoverEnabled = targetConf.getBoolean(
        DFSConfigKeys.DFS_HA_AUTO_FAILOVER_ENABLED_KEY,
        DFSConfigKeys.DFS_HA_AUTO_FAILOVER_ENABLED_DEFAULT);
    if (autoFailoverEnabled) {
      // 自动故障转移开启时，获取ZKFC端口并设置地址
      int port = DFSZKFailoverController.getZkfcPort(targetConf);
      if (port != 0) {
        setZkfcPort(port);
      }
    }

    try {
      // 创建隔离器，用于故障转移时隔离原Active节点
      this.fencer = NodeFencer.create(targetConf,
          DFSConfigKeys.DFS_HA_FENCE_METHODS_KEY);
    } catch (BadFencingConfigurationException e) {
      // 保存隔离配置错误，后续检查时抛出
      this.fenceConfigError = e;
    }
  }

  /**
   * 获取NameNode的RPC服务地址
   * @return NameNode IPC地址
   */
  @Override
  public InetSocketAddress getAddress() {
    return addr;
  }

  /**
   * 获取健康监测使用的生命线地址
   * @return 生命线RPC地址，可用于独立的健康检查
   */
  @Override
  public InetSocketAddress getHealthMonitorAddress() {
    return lifelineAddr;
  }

  /**
   * 获取ZKFC（ZK故障转移控制器）的RPC地址
   * @return ZKFC地址
   * @throws IllegalStateException 自动故障转移未开启时抛出
   */
  @Override
  public InetSocketAddress getZKFCAddress() {
    Preconditions.checkState(autoFailoverEnabled,
        "ZKFC address not relevant when auto failover is off");
    assert zkfcAddr != null;
    
    return zkfcAddr;
  }
  
  /**
   * 设置ZKFC端口，构造ZKFC地址
   * @param port ZKFC端口号
   */
  void setZkfcPort(int port) {
    assert autoFailoverEnabled;
          
    this.zkfcAddr = new InetSocketAddress(addr.getAddress(), port);
  }

  /**
   * 检查隔离配置是否合法
   * @throws BadFencingConfigurationException 配置错误时抛出该异常
   */
  @Override
  public void checkFencingConfigured() throws BadFencingConfigurationException {
    if (fenceConfigError != null) {
      throw fenceConfigError;
    }
    if (fencer == null) {
      throw new BadFencingConfigurationException(
          "No fencer configured for " + this);
    }
  }
  
  /**
   * 获取隔离器实例
   * @return 配置好的节点隔离器
   */
  @Override
  public NodeFencer getFencer() {
    return fencer;
  }
  
  @Override
  public String toString() {
    return "NameNode at " + (lifelineAddr != null ? lifelineAddr : addr);
  }

  /**
   * 获取当前目标所属的名称服务ID
   * @return 名称服务ID
   */
  public String getNameServiceId() {
    return this.nsId;
  }
  
  /**
   * 获取当前目标NameNode的节点ID
   * @return NameNode节点ID
   */
  public String getNameNodeId() {
    return this.nnId;
  }

  /**
   * 添加隔离参数到环境变量，供隔离脚本使用
   * @param ret 存储参数的Map，参数会添加到该Map中
   */
  @Override
  protected void addFencingParameters(Map<String, String> ret) {
    super.addFencingParameters(ret);
    
    ret.put(NAMESERVICE_ID_KEY, getNameServiceId());
    ret.put(NAMENODE_ID_KEY, getNameNodeId());
  }

  /**
   * 检查是否启用自动故障转移
   * @return true表示启用自动故障转移，false表示手动故障转移
   */
  @Override
  public boolean isAutoFailoverEnabled() {
    return autoFailoverEnabled;
  }

  /**
   * 检查是否支持Observer角色
   * @return HDFS NameNode支持Observer角色，返回true
   */
  @Override
  public boolean supportObserver() {
    return true;
  }
}