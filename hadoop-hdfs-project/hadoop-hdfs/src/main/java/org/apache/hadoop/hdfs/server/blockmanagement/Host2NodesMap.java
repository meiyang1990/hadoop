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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 主机名到数据节点描述符的映射表
 * 用于在HDFS块管理中按主机/IP地址快速查找对应的数据节点，支持同一主机部署多个DataNode
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class Host2NodesMap {
  private HashMap<String, String> mapHost = new HashMap<String, String>();
  private final HashMap<String, DatanodeDescriptor[]> map
    = new HashMap<String, DatanodeDescriptor[]>();
  private final ReadWriteLock hostmapLock = new ReentrantReadWriteLock();

  /**
   * 检查指定节点是否已经存在于映射表中
   * @param node 待检查的数据节点描述符
   * @return 存在返回true，否则返回false
   */
  boolean contains(DatanodeDescriptor node) {
    if (node==null) {
      return false;
    }
      
    String ipAddr = node.getIpAddr();
    // 获取读锁
    hostmapLock.readLock().lock();
    try {
      DatanodeDescriptor[] nodes = map.get(ipAddr);
      if (nodes != null) {
        // 遍历IP对应所有节点，检查是否存在当前节点
        for(DatanodeDescriptor containedNode:nodes) {
          if (node==containedNode) {
            return true;
          }
        }
      }
    } finally {
      // 释放读锁
      hostmapLock.readLock().unlock();
    }
    return false;
  }
    
  /**
   * 添加数据节点到映射表
   * @param node 待添加的数据节点描述符
   * @return 添加成功返回true，节点已存在或为空返回false
   */
  boolean add(DatanodeDescriptor node) {
    // 获取写锁
    hostmapLock.writeLock().lock();
    try {
      if (node==null || contains(node)) {
        return false;
      }
      
      String ipAddr = node.getIpAddr();
      String hostname = node.getHostName();
      
      // 保存主机名到IP的映射
      mapHost.put(hostname, ipAddr);
      
      DatanodeDescriptor[] nodes = map.get(ipAddr);
      DatanodeDescriptor[] newNodes;
      if (nodes==null) {
        // 该IP首次添加节点，创建长度为1的数组
        newNodes = new DatanodeDescriptor[1];
        newNodes[0]=node;
      } else { // 同一主机存在多个DataNode，扩容数组
        newNodes = new DatanodeDescriptor[nodes.length+1];
        System.arraycopy(nodes, 0, newNodes, 0, nodes.length);
        newNodes[nodes.length] = node;
      }
      // 更新IP到节点数组的映射
      map.put(ipAddr, newNodes);
      return true;
    } finally {
      // 释放写锁
      hostmapLock.writeLock().unlock();
    }
  }
    
  /**
   * 从映射表移除指定数据节点
   * @param node 待移除的数据节点描述符
   * @return 移除成功返回true，节点不存在或为空返回false
   */
  boolean remove(DatanodeDescriptor node) {
    if (node==null) {
      return false;
    }
      
    String ipAddr = node.getIpAddr();
    String hostname = node.getHostName();
    // 获取写锁
    hostmapLock.writeLock().lock();
    try {

      DatanodeDescriptor[] nodes = map.get(ipAddr);
      if (nodes==null) {
        return false;
      }
      if (nodes.length==1) {
        // IP仅对应一个节点，直接移除整个条目
        if (nodes[0]==node) {
          map.remove(ipAddr);
          // 移除主机名对应IP的映射，因为该主机已无存活节点
          mapHost.remove(hostname);
          return true;
        } else {
          return false;
        }
      }
      // 同一IP对应多个节点，查找待删除节点的索引
      int i=0;
      for(; i<nodes.length; i++) {
        if (nodes[i]==node) {
          break;
        }
      }
      if (i==nodes.length) {
        // 未找到待删除节点
        return false;
      } else {
        // 创建新数组并拷贝元素，跳过待删除节点
        DatanodeDescriptor[] newNodes;
        newNodes = new DatanodeDescriptor[nodes.length-1];
        System.arraycopy(nodes, 0, newNodes, 0, i);
        System.arraycopy(nodes, i+1, newNodes, i, nodes.length-i-1);
        // 更新映射表
        map.put(ipAddr, newNodes);
        return true;
      }
    } finally {
      // 释放写锁
      hostmapLock.writeLock().unlock();
    }
  }
    
  /**
   * 根据IP地址获取一个数据节点
   * 若同一IP对应多个节点，随机返回其中一个
   * @param ipAddr 数据节点的IP地址
   * @return 找到返回数据节点描述符，否则返回null
   */
  DatanodeDescriptor getDatanodeByHost(String ipAddr) {
    if (ipAddr == null) {
      return null;
    }
      
    // 获取读锁
    hostmapLock.readLock().lock();
    try {
      DatanodeDescriptor[] nodes = map.get(ipAddr);
      // 无对应节点
      if (nodes== null) {
        return null;
      }
      // 仅一个节点，直接返回
      if (nodes.length == 1) {
        return nodes[0];
      }
      // 多个节点，随机选择一个返回
      return nodes[ThreadLocalRandom.current().nextInt(nodes.length)];
    } finally {
      // 释放读锁
      hostmapLock.readLock().unlock();
    }
  }
  
  /**
   * 根据传输地址（IP+端口）查找对应数据节点
   * @param ipAddr 数据节点IP地址
   * @param xferPort 数据节点数据传输端口
   * @return 找到返回数据节点描述符，否则返回null
   */
  public DatanodeDescriptor getDatanodeByXferAddr(String ipAddr,
      int xferPort) {
    if (ipAddr==null) {
      return null;
    }

    // 获取读锁
    hostmapLock.readLock().lock();
    try {
      DatanodeDescriptor[] nodes = map.get(ipAddr);
      // 无对应节点
      if (nodes== null) {
        return null;
      }
      // 遍历匹配传输端口
      for(DatanodeDescriptor containedNode:nodes) {
        if (xferPort == containedNode.getXferPort()) {
          return containedNode;
        }
      }
      return null;
    } finally {
      // 释放读锁
      hostmapLock.readLock().unlock();
    }
  }

  

  /**
   * 根据主机名获取对应数据节点
   * 仅适用于一个主机名对应一个数据节点的场景，若同一主机多个节点则随机返回一个
   * @param hostname 数据节点主机名
   * @return 找到返回数据节点描述符，否则返回null
   */
  DatanodeDescriptor getDataNodeByHostName(String hostname) {
    if(hostname == null) {
      return null;
    }
    
    // 获取读锁
    hostmapLock.readLock().lock();
    try {
      String ipAddr = mapHost.get(hostname);
      if(ipAddr == null) {
        return null;
      } else {  
        // 通过IP获取数据节点
        return getDatanodeByHost(ipAddr);
      }
    } finally {
      // 释放读锁
      hostmapLock.readLock().unlock();
    }
  }

  /**
   * 生成映射表的字符串表示，用于调试日志输出
   * @return 映射表格式化字符串，包含所有主机->IP->数据节点的映射关系
   */
  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(getClass().getSimpleName())
        .append("[");
    // 遍历所有主机映射，拼接信息
    for(Map.Entry<String, String> host: mapHost.entrySet()) {
      DatanodeDescriptor[] e = map.get(host.getValue());
      b.append("\n  " + host.getKey() + " => "+host.getValue() + " => " 
          + Arrays.asList(e));
    }
    return b.append("\n]").toString();
  }
}