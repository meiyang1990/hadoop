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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.util.HostsFileReader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;

/**
 * 文件级注释：HDFS 允许/排除 DataNode 节点列表文件管理器，负责从配置文件读取并维护集群允许接入和需要排除的 DataNode 信息
 * <p>
 * This class manages the include and exclude files for HDFS.
 * <p>
 * These files control which DataNodes the NameNode expects to see in the
 * cluster.  Loosely speaking, the include file, if it exists and is not
 * empty, is a list of everything we expect to see.  The exclude file is
 * a list of everything we want to ignore if we do see it.
 * <p>
 * Entries may or may not specify a port.  If they don't, we consider
 * them to apply to every DataNode on that host. The code canonicalizes the
 * entries into IP addresses.
 * <p>
 * The code ignores all entries that the DNS fails to resolve their IP
 * addresses. This is okay because by default the NN rejects the registrations
 * of DNs when it fails to do a forward and reverse lookup. Note that DNS
 * resolutions are only done during the loading time to minimize the latency.
 */
public class HostFileManager extends HostConfigManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(HostFileManager.class);
  private Configuration conf;
  private HostSet includes = new HostSet();
  private HostSet excludes = new HostSet();

  @Override
  /**
   * 设置当前管理器使用的配置对象
   * @param conf Hadoop配置对象
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  /**
   * 获取当前管理器使用的配置对象
   * @return Hadoop配置对象
   */
  public Configuration getConf() {
    return conf;
  }

  @Override
  /**
   * 从配置文件刷新允许/排除节点列表
   * @throws IOException 读取文件失败时抛出异常
   */
  public void refresh() throws IOException {
    refresh(conf.get(DFSConfigKeys.DFS_HOSTS, ""),
        conf.get(DFSConfigKeys.DFS_HOSTS_EXCLUDE, ""));
  }

  /**
   * 读取指定主机列表文件，解析后生成HostSet集合
   * @param type 列表类型（included/excluded），用于日志打印
   * @param filename 主机列表文件路径
   * @return 解析完成的主机集合
   * @throws IOException 读取文件失败时抛出异常
   */
  private static HostSet readFile(String type, String filename)
          throws IOException {
    HostSet res = new HostSet();
    if (!filename.isEmpty()) {
      HashSet<String> entrySet = new HashSet<String>();
      // 读取文件内容到条目集合
      HostsFileReader.readFileToSet(type, filename, entrySet);
      // 遍历每个条目解析为地址
      for (String str : entrySet) {
        InetSocketAddress addr = parseEntry(type, filename, str);
        if (addr != null) {
          // 解析成功则添加到结果集合
          res.add(addr);
        }
      }
    }
    return res;
  }

  @VisibleForTesting
  /**
   * 解析主机列表文件中的单条条目，转换为InetSocketAddress
   * @param type 列表类型（included/excluded），用于日志打印
   * @param fn 主机列表文件路径，用于日志打印
   * @param line 文件中的条目内容
   * @return 解析成功返回地址对象，解析/解析失败返回null
   */
  static InetSocketAddress parseEntry(String type, String fn, String line) {
    try {
      // 利用URI解析主机名和端口
      URI uri = new URI("dummy", line, null, null, null);
      int port = uri.getPort() == -1 ? 0 : uri.getPort();
      InetSocketAddress addr = new InetSocketAddress(uri.getHost(), port);
      if (addr.isUnresolved()) {
        // DNS解析失败，记录警告并忽略该条目
        LOG.warn(String.format("Failed to resolve address `%s` in `%s`. " +
                "Ignoring in the %s list.", line, fn, type));
        return null;
      }
      return addr;
    } catch (URISyntaxException e) {
      // 语法解析错误，记录警告并忽略该条目
      LOG.warn(String.format("Failed to parse `%s` in `%s`. " + "Ignoring in " +
              "the %s list.", line, fn, type));
    }
    return null;
  }

  @Override
  /**
   * 获取当前允许接入的主机集合
   * @return 允许接入的主机集合
   */
  public synchronized HostSet getIncludes() {
    return includes;
  }

  @Override
  /**
   * 获取当前需要排除的主机集合
   * @return 需要排除的主机集合
   */
  public synchronized HostSet getExcludes() {
    return excludes;
  }

  // If the includes list is empty, act as if everything is in the
  // includes list.
  @Override
  /**
   * 检查指定DataNode是否在允许接入列表中
   * @param dn 待检查的DataNode
   * @return 如果允许列表为空或该节点匹配允许列表返回true，否则返回false
   */
  public synchronized boolean isIncluded(DatanodeID dn) {
    return includes.isEmpty() || includes.match(dn.getResolvedAddress());
  }

  @Override
  /**
   * 检查指定DataNode是否在排除列表中
   * @param dn 待检查的DataNode
   * @return 如果该节点匹配排除列表返回true，否则返回false
   */
  public synchronized boolean isExcluded(DatanodeID dn) {
    return isExcluded(dn.getResolvedAddress());
  }

  /**
   * 根据地址检查节点是否被排除
   * @param address DataNode解析后的地址
   * @return 匹配排除列表返回true，否则返回false
   */
  private boolean isExcluded(InetSocketAddress address) {
    return excludes.match(address);
  }

  @Override
  /**
   * 获取指定DataNode的升级域，基于文件的配置不支持升级域功能
   * @param dn 待查询的DataNode
   * @return 固定返回null
   */
  public synchronized String getUpgradeDomain(final DatanodeID dn) {
    // The include/exclude files based config doesn't support upgrade domain
    // config.
    return null;
  }

  @Override
  /**
   * 获取指定DataNode的维护模式过期时间，基于文件的配置不支持维护模式功能
   * @param dn 待查询的DataNode
   * @return 固定返回0，表示不在维护模式
   */
  public long getMaintenanceExpirationTimeInMS(DatanodeID dn) {
    // The include/exclude files based config doesn't support maintenance mode.
    return 0;
  }

  /**
   * Read the includes and excludes lists from the named files.  Any previous
   * includes and excludes lists are discarded.
   * @param includeFile the path to the new includes list
   * @param excludeFile the path to the new excludes list
   * @throws IOException thrown if there is a problem reading one of the files
   */
  /**
   * 从指定文件路径刷新允许和排除节点列表，丢弃旧配置
   * @param includeFile 允许列表文件路径
   * @param excludeFile 排除列表文件路径
   * @throws IOException 读取文件失败时抛出异常
   */
  private void refresh(String includeFile, String excludeFile)
      throws IOException {
    // 读取解析允许列表文件
    HostSet newIncludes = readFile("included", includeFile);
    // 读取解析排除列表文件
    HostSet newExcludes = readFile("excluded", excludeFile);

    // 更新管理器中的列表
    refresh(newIncludes, newExcludes);
  }

  /**
   * Set the includes and excludes lists by the new HostSet instances. The
   * old instances are discarded.
   * @param newIncludes the new includes list
   * @param newExcludes the new excludes list
   */
  /**
   * 直接用新的HostSet更新允许和排除列表，丢弃旧配置
   * @param newIncludes 新的允许列表
   * @param newExcludes 新的排除列表
   */
  @VisibleForTesting
  void refresh(HostSet newIncludes, HostSet newExcludes) {
    synchronized (this) {
      includes = newIncludes;
      excludes = newExcludes;
    }
  }
}