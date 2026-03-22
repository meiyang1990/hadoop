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
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_ALLOW_STALE_READ_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_ALLOW_STALE_READ_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.NameNodeProxiesClient.ProxyAndInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.AbstractNNFailoverProxyProvider;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.Lists;
import org.slf4j.LoggerFactory;

/**
 * HDFS高可用(HA)功能工具类，提供HA相关配置检查、节点ID获取、代理创建、状态检查等通用工具能力。
 * 支撑HDFS HA架构下各类核心操作的工具实现，为上层模块提供统一的HA相关工具方法。
 */
@InterfaceAudience.Private
public class HAUtil {

  public static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(HAUtil.class.getName());

  // HA模式下每个NameNode独立配置，不需要共享的特殊配置项列表
  private static final String[] HA_SPECIAL_INDEPENDENT_KEYS = new String[]{
    DFS_NAMENODE_RPC_ADDRESS_KEY,
    DFS_NAMENODE_RPC_BIND_HOST_KEY,
    DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY,
    DFS_NAMENODE_LIFELINE_RPC_BIND_HOST_KEY,
    DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
    DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY,
    DFS_NAMENODE_HTTP_ADDRESS_KEY,
    DFS_NAMENODE_HTTPS_ADDRESS_KEY,
    DFS_NAMENODE_HTTP_BIND_HOST_KEY,
    DFS_NAMENODE_HTTPS_BIND_HOST_KEY,
  };

  private HAUtil() { /* Hidden constructor */ }

  /**
   * 检查指定名称服务是否配置了NameNode高可用(HA)
   * @param conf 配置对象
   * @param nsId 名称服务ID，未配置联邦时传null
   * @return 如果配置了HA返回true，否则返回false
   */
  public static boolean isHAEnabled(Configuration conf, String nsId) {
    Map<String, Map<String, InetSocketAddress>> addresses =
        DFSUtilClient.getHaNnRpcAddresses(conf);
    if (addresses == null) return false;
    Map<String, InetSocketAddress> nnMap = addresses.get(nsId);
    return nnMap != null && nnMap.size() > 1;
  }

  /**
   * 检查当前HA配置是否使用共享edits目录
   * @param conf 配置对象
   * @return 如果配置了共享edits目录返回true，否则返回false
   */
  public static boolean usesSharedEditsDir(Configuration conf) {
    return null != conf.get(DFS_NAMENODE_SHARED_EDITS_DIR_KEY);
  }

  /**
   * 获取当前节点的NameNode ID，如果配置未显式设置则通过本地地址匹配自动推导
   * @param conf 配置对象
   * @param nsId 名称服务ID
   * @return 成功则返回NameNode ID，失败返回null
   * @throws HadoopIllegalArgumentException 配置错误时抛出异常
   */
  public static String getNameNodeId(Configuration conf, String nsId) {
    String namenodeId = conf.getTrimmed(DFS_HA_NAMENODE_ID_KEY);
    if (namenodeId != null) {
      return namenodeId;
    }
    
    String suffixes[] = DFSUtil.getSuffixIDs(conf, DFS_NAMENODE_RPC_ADDRESS_KEY,
        nsId, null, DFSUtil.LOCAL_ADDRESS_MATCHER);
    if (suffixes == null) {
      String msg = "Configuration " + DFS_NAMENODE_RPC_ADDRESS_KEY + 
          " must be suffixed with nameservice and namenode ID for HA " +
          "configuration.";
      throw new HadoopIllegalArgumentException(msg);
    }
    
    return suffixes[1];
  }

  /**
   * 根据地址从配置中推导对应NameNode的ID
   * @param conf 配置对象
   * @param address 目标NameNode地址
   * @param keys 需要匹配的配置key列表
   * @return 推导得到的NameNode ID，无法推导则返回null
   */
  public static String getNameNodeIdFromAddress(final Configuration conf, 
      final InetSocketAddress address, String... keys) {
    // Configuration with a single namenode and no nameserviceId
    String[] ids = DFSUtil.getSuffixIDs(conf, address, keys);
    if (ids != null && ids.length > 1) {
      return ids[1];
    }
    return null;
  }
  
  /**
   * 获取当前HA集群中，除本节点外其他所有NameNode的ID列表
   * @param conf 当前节点配置
   * @param nsId 名称服务ID
   * @return 其他NameNode的ID列表
   */
  public static List<String> getNameNodeIdOfOtherNodes(Configuration conf, String nsId) {
    Preconditions.checkArgument(nsId != null,
        "Could not determine namespace id. Please ensure that this " +
        "machine is one of the machines listed as a NN RPC address, " +
        "or configure " + DFSConfigKeys.DFS_NAMESERVICE_ID);
    
    Collection<String> nnIds = DFSUtilClient.getNameNodeIds(conf, nsId);
    String myNNId = conf.get(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY);
    Preconditions.checkArgument(nnIds != null,
        "Could not determine namenode ids in namespace '%s'. " +
        "Please configure " +
        DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_HA_NAMODES_KEY_PREFIX,
            nsId),
        nsId);
    Preconditions.checkArgument(nnIds.size() >= 2,
        "Expected at least 2 NameNodes in namespace '%s'. " +
          "Instead, got only %s (NN ids were '%s')",
          nsId, nnIds.size(), Joiner.on("','").join(nnIds));
    Preconditions.checkState(myNNId != null && !myNNId.isEmpty(),
        "Could not determine own NN ID in namespace '%s'. Please " +
        "ensure that this node is one of the machines listed as an " +
        "NN RPC address, or configure " + DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY,
        nsId);

    ArrayList<String> namenodes = Lists.newArrayList(nnIds);
    namenodes.remove(myNNId);
    assert namenodes.size() >= 1;
    return namenodes;
  }

  /**
   * 根据当前节点配置，生成HA集群中所有其他NameNode的配置对象列表
   * @param myConf 当前节点配置
   * @return 其他NameNode的配置对象列表
   */
  public static List<Configuration> getConfForOtherNodes(
      Configuration myConf) {
    
    String nsId = DFSUtil.getNamenodeNameServiceId(myConf);
    List<String> otherNodes = getNameNodeIdOfOtherNodes(myConf, nsId);

    // Look up the address of the other NNs
    List<Configuration> confs = new ArrayList<Configuration>(otherNodes.size());
    myConf = new Configuration(myConf);
    // 清除每个节点独立配置，后续会按节点ID重新初始化
    for (String idpKey : HA_SPECIAL_INDEPENDENT_KEYS) {
      myConf.unset(idpKey);
    }
    for (String nn : otherNodes) {
      Configuration confForOtherNode = new Configuration(myConf);
      NameNode.initializeGenericKeys(confForOtherNode, nsId, nn);
      confs.add(confForOtherNode);
    }
    return confs;
  }

  /**
   * 检查配置是否允许Standby状态的NameNode处理读请求（ stale读）
   * @return true允许Standby读，false不允许
   */
  public static boolean shouldAllowStandbyReads(Configuration conf) {
    return conf.getBoolean(DFS_HA_ALLOW_STALE_READ_KEY,
        DFS_HA_ALLOW_STALE_READ_DEFAULT);
  }
  
  /**
   * 设置是否允许Standby读配置
   * @param conf 目标配置对象
   * @param val 是否允许
   */
  public static void setAllowStandbyReads(Configuration conf, boolean val) {
    conf.setBoolean(DFS_HA_ALLOW_STALE_READ_KEY, val);
  }

  /**
   * 检查当前配置是否需要使用逻辑URI对接NameNode和故障转移代理
   * @param conf 配置对象
   * @param nameNodeUri NameNode URI
   * @return true需要使用逻辑URI，false不需要
   * @throws IOException 配置错误时抛出异常
   */
  public static boolean useLogicalUri(Configuration conf, URI nameNodeUri) 
      throws IOException {
    // Create the proxy provider. Actual proxy is not created.
    AbstractNNFailoverProxyProvider<ClientProtocol> provider = NameNodeProxiesClient
        .createFailoverProxyProvider(conf, nameNodeUri, ClientProtocol.class,
            false, null);

    // No need to use logical URI since failover is not configured.
    if (provider == null) {
      return false;
    }
    // Check whether the failover proxy provider uses logical URI.
    return provider.useLogicalURI();
  }

  /**
   * 获取当前处于Active状态的NameNode地址，注意：该方法获取地址后，如果发生故障切换，地址会失效，因此不推荐频繁使用
   * @param fs 目标文件系统对象
   * @return 当前Active NameNode地址，未找到则返回null
   * @throws IOException 解析过程发生IO错误抛出异常
   */
  public static InetSocketAddress getAddressOfActive(FileSystem fs)
      throws IOException {
    InetSocketAddress inAddr = null;
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IllegalArgumentException("FileSystem " + fs + " is not a DFS.");
    }
    // 触发客户端地址解析，刷新代理信息
    fs.exists(new Path("/"));
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    Configuration dfsConf = dfs.getConf();
    URI dfsUri = dfs.getUri();
    String nsId = dfsUri.getHost();
    if (isHAEnabled(dfsConf, nsId)) {
      List<ClientProtocol> namenodes =
          getProxiesForAllNameNodesInNameservice(dfsConf, nsId);
      for (ClientProtocol proxy : namenodes) {
        try {
          if (proxy.getHAServiceState().equals(HAServiceState.ACTIVE)) {
            inAddr = RPC.getServerAddress(proxy);
          }
        } catch (Exception e) {
          //连接失败忽略该节点，继续尝试其他节点
          LOG.debug("Error while connecting to namenode", e);
        }
      }
    } else {
      DFSClient dfsClient = dfs.getClient();
      inAddr = RPC.getServerAddress(dfsClient.getNamenode());
    }
    return inAddr;
  }
  
  /**
   * 为HA名称服务下所有NameNode创建RPC代理，用于需要向所有NN发起请求的场景
   * @param conf 配置对象
   * @param nsId 名称服务ID
   * @return 所有NameNode的ClientProtocol代理列表
   * @throws IOException 创建代理发生错误抛出异常
   */
  public static List<ClientProtocol> getProxiesForAllNameNodesInNameservice(
      Configuration conf, String nsId) throws IOException {
    List<ProxyAndInfo<ClientProtocol>> proxies =
        getProxiesForAllNameNodesInNameservice(conf, nsId, ClientProtocol.class);

    List<ClientProtocol> namenodes = new ArrayList<ClientProtocol>(
        proxies.size());
    for (ProxyAndInfo<ClientProtocol> proxy : proxies) {
      namenodes.add(proxy.getProxy());
    }
    return namenodes;
  }

  /**
   * 为HA名称服务下所有NameNode创建指定协议的RPC代理，泛型版本支持自定义协议
   * @param conf 配置对象
   * @param nsId 名称服务ID
   * @param xface 协议接口类
   * @return 所有NameNode的代理信息列表，包含代理和额外信息
   * @throws IOException 创建代理发生错误抛出异常
   */
  public static <T> List<ProxyAndInfo<T>> getProxiesForAllNameNodesInNameservice(
      Configuration conf, String nsId, Class<T> xface) throws IOException {
    Map<String, InetSocketAddress> nnAddresses =
        DFSUtil.getRpcAddressesForNameserviceId(conf, nsId, null);
    
    List<ProxyAndInfo<T>> proxies = new ArrayList<ProxyAndInfo<T>>(
        nnAddresses.size());
    for (InetSocketAddress nnAddress : nnAddresses.values()) {
      ProxyAndInfo<T> proxyInfo = NameNodeProxies.createNonHAProxy(conf,
          nnAddress, xface,
          UserGroupInformation.getCurrentUser(), false);
      proxies.add(proxyInfo);
    }
    return proxies;
  }
  
  /**
   * 检查给定NameNode代理列表中是否至少有一个处于Active状态
   * @param namenodes NameNode代理列表
   * @return 至少一个Active返回true，全部Standby返回false
   * @throws IOException 多个非Standby异常时抛出聚合异常
   */
  public static boolean isAtLeastOneActive(List<ClientProtocol> namenodes)
      throws IOException {
    List<IOException> exceptions = new ArrayList<>();
    for (ClientProtocol namenode : namenodes) {
      try {
        // 尝试访问根目录文件信息验证是否可处理请求
        namenode.getFileInfo("/");
        return true;
      } catch (RemoteException re) {
        IOException cause = re.unwrapRemoteException();
        if (cause instanceof StandbyException) {
          // Standby节点抛出异常是预期行为，直接忽略
        } else {
          exceptions.add(re);
        }
      } catch (IOException ioe) {
        exceptions.add(ioe);
      }
    }
    if(!exceptions.isEmpty()){
      throw MultipleIOException.createIOException(exceptions);
    }
    return false;
  }
}