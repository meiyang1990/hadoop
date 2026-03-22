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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.Multimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.UnmodifiableIterator;

import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeAdminProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;

import java.io.IOException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;



import org.apache.hadoop.hdfs.util.CombinedHostsFileReader;

/**
 * 文件级注释：HDFS DataNode主机配置管理器，通过JSON格式文件统一管理DataNode的管理属性
 * 
 * 本类使用JSON格式的组合主机配置文件管理DataNode的状态，支持包含/排除、维护状态、升级域等配置
 * 具体JSON格式请参考{@link CombinedHostsFileReader}
 * <p>
 * 条目可以指定或不指定端口，若不指定端口则对该主机上所有DataNode生效。
 * 所有条目会被解析为标准IP地址。
 * <p>
 * 解析失败无法解析IP地址的条目会被忽略，DNS解析仅在加载配置时执行，避免运行时延迟。
 * 无法通过正向反向DNS解析的DataNode，NameNode默认会拒绝其注册，因此忽略解析失败条目是安全的。
 */
public class CombinedHostFileManager extends HostConfigManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      CombinedHostFileManager.class);
  private Configuration conf;
  private HostProperties hostProperties = new HostProperties();

  /**
   * 内部类，存储解析后的所有DataNode管理属性，提供查询能力
   */
  static class HostProperties {
    // 按IP地址存储所有DataNode的管理属性，一个IP可以对应多个不同端口的DataNode配置
    private Multimap<InetAddress, DatanodeAdminProperties> allDNs =
        HashMultimap.create();
    // 优化标记：当配置文件中没有任何正常服务状态节点时，认为所有节点都允许注册，等价于空包含列表
    private boolean emptyInServiceNodeLists = true;
    
    /**
     * 添加一个DataNode的管理属性配置
     * @param addr DataNodeIP地址
     * @param properties DataNode管理属性
     */
    synchronized void add(InetAddress addr,
        DatanodeAdminProperties properties) {
      allDNs.put(addr, properties);
      if (properties.getAdminState().equals(
          AdminStates.NORMAL)) {
        emptyInServiceNodeLists = false;
      }
    }

    /**
     * 检查指定地址的DataNode是否在允许注册的包含列表中
     * @param address DataNode地址
     * @return true表示允许注册，false表示不允许
     */
    // 如果包含列表为空，则认为所有节点都被包含
    synchronized boolean isIncluded(final InetSocketAddress address) {
      return emptyInServiceNodeLists || allDNs.get(address.getAddress())
          .stream().anyMatch(
              input -> input.getPort() == 0 ||
                  input.getPort() == address.getPort());
    }

    /**
     * 检查指定地址的DataNode是否被标记为已退役（排除）
     * @param address DataNode地址
     * @return true表示该节点需要退役排除，false表示不需要
     */
    synchronized boolean isExcluded(final InetSocketAddress address) {
      return allDNs.get(address.getAddress()).stream().anyMatch(
          input -> input.getAdminState().equals(
              AdminStates.DECOMMISSIONED) &&
              (input.getPort() == 0 ||
                  input.getPort() == address.getPort()));
    }

    /**
     * 获取指定DataNode的升级域
     * @param address DataNode地址
     * @return 升级域名称，没有配置则返回null
     */
    synchronized String getUpgradeDomain(final InetSocketAddress address) {
      Iterable<DatanodeAdminProperties> datanode =
          allDNs.get(address.getAddress()).stream().filter(
              input -> (input.getPort() == 0 ||
                  input.getPort() == address.getPort())).collect(
              Collectors.toList());
      return datanode.iterator().hasNext() ?
          datanode.iterator().next().getUpgradeDomain() : null;
    }

    /**
     * 获取所有允许注册节点的地址迭代器
     * @return 允许注册节点地址迭代器
     */
    Iterable<InetSocketAddress> getIncludes() {
      return new Iterable<InetSocketAddress>() {
        @Override
        public Iterator<InetSocketAddress> iterator() {
            return new HostIterator(allDNs.entries());
        }
      };
    }

    /**
     * 获取所有退役排除节点的地址迭代器
     * @return 退役排除节点地址迭代器
     */
    Iterable<InetSocketAddress> getExcludes() {
      return () -> new HostIterator(
          allDNs.entries().stream().filter(
              entry -> entry.getValue().getAdminState().equals(
                  AdminStates.DECOMMISSIONED)).collect(
              Collectors.toList()));
    }

    /**
     * 获取指定DataNode的维护过期时间戳
     * @param address DataNode地址
     * @return 维护过期时间（毫秒时间戳），未配置维护则返回0
     */
    synchronized long getMaintenanceExpireTimeInMS(
        final InetSocketAddress address) {
      Iterable<DatanodeAdminProperties> datanode =
          allDNs.get(address.getAddress()).stream().filter(
              input -> input.getAdminState().equals(
                  AdminStates.IN_MAINTENANCE) &&
                  (input.getPort() == 0 ||
                      input.getPort() == address.getPort())).collect(
              Collectors.toList());
      // 若DataNode未被设置为维护状态，则忽略配置中的维护过期时间
      return datanode.iterator().hasNext() ?
          datanode.iterator().next().getMaintenanceExpireTimeInMS() : 0;
    }

    /**
     * 内部迭代器，遍历存储的节点条目转换为InetSocketAddress输出
     */
    static class HostIterator extends UnmodifiableIterator<InetSocketAddress> {
      private final Iterator<Map.Entry<InetAddress,
          DatanodeAdminProperties>> it;
      
      /**
       * 构造函数，基于节点条目集合构造迭代器
       * @param nodes 节点条目集合
       */
      public HostIterator(Collection<java.util.Map.Entry<InetAddress,
          DatanodeAdminProperties>> nodes) {
        this.it = nodes.iterator();
      }
      
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public InetSocketAddress next() {
        Map.Entry<InetAddress, DatanodeAdminProperties> e = it.next();
        return new InetSocketAddress(e.getKey(), e.getValue().getPort());
      }
    }
  }

  @Override
  public Iterable<InetSocketAddress> getIncludes() {
    return hostProperties.getIncludes();
  }

  @Override
  public Iterable<InetSocketAddress> getExcludes() {
    return hostProperties.getExcludes();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void refresh() throws IOException {
    refresh(conf.get(DFSConfigKeys.DFS_HOSTS, ""),
        conf.getInt(DFSConfigKeys.DFS_HOSTS_TIMEOUT, DFSConfigKeys.DFS_HOSTS_TIMEOUT_DEFAULT)
    );
  }
  
  /**
   * 从指定主机文件重新加载配置，解析所有DataNode管理属性
   * @param hostsFile 主机配置文件路径
   * @param readTimeout 读取超时时间
   * @throws IOException 读取或解析配置文件失败时抛出
   */
  private void refresh(final String hostsFile, final int readTimeout) throws IOException {
    HostProperties hostProps = new HostProperties();
    // 根据是否设置自定义超时，选择不同读取方式
    DatanodeAdminProperties[] all = readTimeout != DFSConfigKeys.DFS_HOSTS_TIMEOUT_DEFAULT
        ? CombinedHostsFileReader.readFileWithTimeout(hostsFile, readTimeout)
        : CombinedHostsFileReader.readFile(hostsFile);
    // 遍历解析每个配置条目
    for(DatanodeAdminProperties properties : all) {
      InetSocketAddress addr = parseEntry(hostsFile,
          properties.getHostName(), properties.getPort());
      if (addr != null) {
        hostProps.add(addr.getAddress(), properties);
      }
    }
    // 替换为新解析的配置
    refresh(hostProps);
  }

  /**
   * 解析主机条目，将主机名和端口转换为InetSocketAddress
   * @param fn 配置文件名，用于错误日志
   * @param hostName 主机名
   * @param port 端口
   * @return 解析成功返回地址对象，解析失败返回null
   */
  @VisibleForTesting
  static InetSocketAddress parseEntry(final String fn, final String hostName,
      final int port) {
    InetSocketAddress addr = new InetSocketAddress(hostName, port);
    if (addr.isUnresolved()) {
      LOG.warn("Failed to resolve {} in {}. ", hostName, fn);
      return null;
    }
    return addr;
  }

  @Override
  public synchronized boolean isIncluded(final DatanodeID dn) {
    return hostProperties.isIncluded(dn.getResolvedAddress());
  }

  @Override
  public synchronized boolean isExcluded(final DatanodeID dn) {
    return isExcluded(dn.getResolvedAddress());
  }

  private boolean isExcluded(final InetSocketAddress address) {
    return hostProperties.isExcluded(address);
  }

  @Override
  public synchronized String getUpgradeDomain(final DatanodeID dn) {
    return hostProperties.getUpgradeDomain(dn.getResolvedAddress());
  }

  @Override
  public long getMaintenanceExpirationTimeInMS(DatanodeID dn) {
    return hostProperties.getMaintenanceExpireTimeInMS(dn.getResolvedAddress());
  }

  /**
   * 替换当前生效的主机配置，旧配置被丢弃
   * @param hostProperties 新解析的主机配置对象
   */
  @VisibleForTesting
  private void refresh(final HostProperties hostProperties) {
    synchronized (this) {
      this.hostProperties = hostProperties;
    }
  }
}