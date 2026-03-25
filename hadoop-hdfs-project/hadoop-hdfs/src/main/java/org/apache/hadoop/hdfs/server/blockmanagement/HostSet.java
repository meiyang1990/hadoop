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


import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.Multimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.UnmodifiableIterator;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * 主机地址集合，支持对通配符地址的高效匹配查询
 * <p>
 * 对于相同主机地址的两个InetSocketAddress对象A和B，定义偏序关系：
 * A &lt;= B 当且仅当 A端口等于B端口 或者 B端口等于0（端口0表示通配符，匹配该主机所有端口）
 * </p>
 * 主要用于HDFS权限管理、节点准入控制场景，快速匹配允许/禁止的主机地址规则
 */
public class HostSet implements Iterable<InetSocketAddress> {
  // 存储主机地址到端口列表的映射
  private final Multimap<InetAddress, Integer> addrs = HashMultimap.create();

  /**
   * 检查集合中是否存在条目foo满足 foo &lt;= addr，用于判断是否被当前地址是否被集合中已有条目匹配
   * @param addr 待检查的目标地址
   * @return 若存在满足条件的条目返回true，否则返回false
   */
  boolean matchedBy(InetSocketAddress addr) {
    Collection<Integer> ports = addrs.get(addr.getAddress());
    return addr.getPort() == 0 ? !ports.isEmpty() : ports.contains(addr
        .getPort());
  }

  /**
   * 检查集合中是否存在条目foo满足 addr &lt;= foo，即判断当前地址是否匹配集合中已有的规则
   * @param addr 待匹配的目标地址
   * @return 若匹配返回true，否则返回false
   */
  boolean match(InetSocketAddress addr) {
    int port = addr.getPort();
    Collection<Integer> ports = addrs.get(addr.getAddress());
    boolean exactMatch = ports.contains(port);
    boolean genericMatch = ports.contains(0);
    return exactMatch || genericMatch;
  }

  /**
   * 检查集合是否为空
   * @return 集合中没有条目时返回true，否则返回false
   */
  boolean isEmpty() {
    return addrs.isEmpty();
  }

  /**
   * 获取集合中总条目数量
   * @return 总条目数量
   */
  int size() {
    return addrs.size();
  }

  /**
   * 向集合中添加一个主机地址条目
   * @param addr 待添加的地址，必须已经解析完成，不能是未解析地址
   */
  void add(InetSocketAddress addr) {
    Preconditions.checkArgument(!addr.isUnresolved());
    addrs.put(addr.getAddress(), addr.getPort());
  }

  @Override
  public Iterator<InetSocketAddress> iterator() {
    return new UnmodifiableIterator<InetSocketAddress>() {
      private final Iterator<Map.Entry<InetAddress,
          Integer>> it = addrs.entries().iterator();

      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public InetSocketAddress next() {
        Map.Entry<InetAddress, Integer> e = it.next();
        return new InetSocketAddress(e.getKey(), e.getValue());
      }
    };
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("HostSet(");
    Iterator<InetSocketAddress> iter = iterator();
    String sep = "";
    while (iter.hasNext()) {
      InetSocketAddress addr = iter.next();
      sb.append(sep);
      sb.append(addr.getAddress().getHostAddress());
      sb.append(':');
      sb.append(addr.getPort());
      sep = ",";
    }
    return sb.append(')').toString();
  }
}