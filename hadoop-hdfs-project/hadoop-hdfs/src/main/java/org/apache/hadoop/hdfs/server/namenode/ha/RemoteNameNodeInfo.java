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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

/**
 * 存储单个远程NameNode的连接信息，用于HDFS高可用场景下获取对端NameNode的地址配置
 */
public class RemoteNameNodeInfo {

  /**
   * 从配置中解析出所有远程NameNode的信息，自动获取当前命名空间ID
   * @param conf Hadoop配置对象
   * @return 所有远程NameNode的信息列表，单NameNode场景返回空列表
   * @throws IOException 配置解析异常时抛出
   */
  public static List<RemoteNameNodeInfo> getRemoteNameNodes(Configuration conf) throws IOException {
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    return getRemoteNameNodes(conf, nsId);
  }

  /**
   * 从配置中解析出指定命名空间下所有远程NameNode的信息
   * @param conf Hadoop配置对象
   * @param nsId 命名空间ID
   * @return 所有远程NameNode的信息列表，单NameNode场景返回空列表
   * @throws IOException 配置解析异常时抛出
   */
  public static List<RemoteNameNodeInfo> getRemoteNameNodes(Configuration conf, String nsId)
      throws IOException {
    // 未配置命名空间，说明是单NameNode部署，没有其他远程NameNode
    if (nsId == null) {
      return Collections.emptyList();
    }
    // 获取所有其他NameNode的配置片段
    List<Configuration> otherNodes = HAUtil.getConfForOtherNodes(conf);
    List<RemoteNameNodeInfo> nns = new ArrayList<RemoteNameNodeInfo>();

    for (Configuration otherNode : otherNodes) {
      // 获取当前远程NameNode的ID
      String otherNNId = HAUtil.getNameNodeId(otherNode, nsId);
      // 获取RPC服务地址，此处不做验证，后续流程可能覆盖该配置
      InetSocketAddress otherIpcAddr = NameNode.getServiceAddress(otherNode, true);

      // 获取HTTP服务scheme（http/https）
      final String scheme = DFSUtil.getHttpClientScheme(conf);
      // 构造HTTP服务地址URL
      URL otherHttpAddr = DFSUtil.getInfoServerWithDefaultHost(otherIpcAddr.getHostName(),
          otherNode, scheme).toURL();

      nns.add(new RemoteNameNodeInfo(otherNode, otherNNId, otherIpcAddr, otherHttpAddr));
    }
    return nns;
  }

  private final Configuration conf;
  private final String nnId;
  private InetSocketAddress ipcAddress;
  private final URL httpAddress;

  /**
   * 构造远程NameNode信息对象
   * @param conf 对应远程NameNode的配置
   * @param nnId NameNode ID
   * @param ipcAddress RPC服务地址
   * @param httpAddress HTTP服务地址
   */
  private RemoteNameNodeInfo(Configuration conf, String nnId, InetSocketAddress ipcAddress,
      URL httpAddress) {
    this.conf = conf;
    this.nnId = nnId;
    this.ipcAddress = ipcAddress;
    this.httpAddress = httpAddress;
  }

  /**
   * 获取远程NameNode的RPC服务地址
   * @return RPC服务地址对象
   */
  public InetSocketAddress getIpcAddress() {
    return this.ipcAddress;
  }

  /**
   * 获取远程NameNode的ID
   * @return NameNode ID字符串
   */
  public String getNameNodeID() {
    return this.nnId;
  }

  /**
   * 获取远程NameNode的HTTP服务地址
   * @return HTTP服务URL对象
   */
  public URL getHttpAddress() {
    return this.httpAddress;
  }

  /**
   * 获取远程NameNode对应的配置对象
   * @return 配置对象
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * 更新远程NameNode的RPC服务地址
   * @param ipc 新的RPC服务地址
   */
  public void setIpcAddress(InetSocketAddress ipc) {
    this.ipcAddress = ipc;
  }

  @Override
  public String toString() {
    return "RemoteNameNodeInfo [nnId=" + nnId + ", ipcAddress=" + ipcAddress
        + ", httpAddress=" + httpAddress + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RemoteNameNodeInfo that = (RemoteNameNodeInfo) o;

    if (!nnId.equals(that.nnId)) return false;
    if (!ipcAddress.equals(that.ipcAddress)) return false;
    // URL.equals会触发DNS解析，是阻塞调用，因此转换为字符串比较
    String httpString = httpAddress.toString();
    String thatHttpString  = that.httpAddress.toString();
    return httpString.equals(thatHttpString);

  }

  @Override
  public int hashCode() {
    int result = nnId.hashCode();
    result = 31 * result + ipcAddress.hashCode();
    // URL.hashCode会触发DNS解析，是阻塞调用，因此转换为字符串计算哈希
    result = 31 * result + httpAddress.toString().hashCode();
    return result;
  }
}