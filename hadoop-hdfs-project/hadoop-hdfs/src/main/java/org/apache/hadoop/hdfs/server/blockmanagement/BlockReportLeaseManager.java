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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 文件说明: 块报告租约管理器，用于限制NameNode同时处理的全量块报告数量，避免突发大量全量块报告导致NameNode压力过高
 * 
 * 核心机制：DataNode通过心跳向NameNode申请块报告租约，NameNode控制同时发放的租约数量，
 * 只有获得租约的DataNode才能发送全量块报告并被NameNode接受处理。租约会在超时后自动过期回收，
 * 避免死节点占用租约名额。优先级优先分配给长时间未发送全量块报告的DataNode。
 * 租约ID为0表示跳过限流，直接接受块报告（手动触发块报告默认使用0租约）。
 */
class BlockReportLeaseManager {
  static final Logger LOG =
      LoggerFactory.getLogger(BlockReportLeaseManager.class);

  /**
   * 存储单个DataNode的块报告租约信息，维护双向链表节点
   */
  private static class NodeData {
    /**
     * DataNode的唯一标识UUID
     */
    final String datanodeUuid;

    /**
     * 租约ID，0表示当前无有效租约
     */
    long leaseId;

    /**
     * 租约发放时间，单位毫秒，0表示当前无有效租约
     */
    long leaseTimeMs;

    /**
     * 双向链表前驱节点
     */
    NodeData prev;

    /**
     * 双向链表后继节点
     */
    NodeData next;

    /**
     * 创建双向链表头节点
     * @param name 链表名称
     * @return 初始化完成的头节点
     */
    static NodeData ListHead(String name) {
      NodeData node = new NodeData(name);
      node.next = node;
      node.prev = node;
      return node;
    }

    NodeData(String datanodeUuid) {
      this.datanodeUuid = datanodeUuid;
    }

    /**
     * 从当前所在双向链表中移除自身
     */
    void removeSelf() {
      if (this.prev != null) {
        this.prev.next = this.next;
      }
      if (this.next != null) {
        this.next.prev = this.prev;
      }
      this.next = null;
      this.prev = null;
    }

    /**
     * 将指定节点添加到当前节点之前（双向链表尾部插入）
     * @param node 待插入节点
     */
    void addToEnd(NodeData node) {
      Preconditions.checkState(node.next == null);
      Preconditions.checkState(node.prev == null);
      node.prev = this.prev;
      node.next = this;
      this.prev.next = node;
      this.prev = node;
    }

    /**
     * 将指定节点添加到当前节点之后（双向链表头部插入）
     * @param node 待插入节点
     */
    void addToBeginning(NodeData node) {
      Preconditions.checkState(node.next == null);
      Preconditions.checkState(node.prev == null);
      node.next = this.next;
      node.prev = this;
      this.next.prev = node;
      this.next = node;
    }
  }

  /**
   * 存储当前未获得块报告租约的DataNode双向链表头
   */
  private final NodeData deferredHead = NodeData.ListHead("deferredHead");

  /**
   * 存储当前已获得块报告租约的DataNode双向链表头
   */
  private final NodeData pendingHead = NodeData.ListHead("pendingHead");

  /**
   * DataNode UUID到节点租约信息的映射表
   */
  private final HashMap<String, NodeData> nodes = new HashMap<>();

  /**
   * 当前已发放的租约数量
   */
  private int numPending = 0;

  /**
   * 任意时刻最大可发放的租约数量上限
   */
  private final int maxPending;

  /**
   * 租约过期时间，单位毫秒
   */
  private final long leaseExpiryMs;

  /**
   * 下一个待分配的租约ID基准值
   */
  private long nextId = ThreadLocalRandom.current().nextLong();

  /**
   * 从配置构造块报告租约管理器
   * @param conf Hadoop配置对象
   */
  BlockReportLeaseManager(Configuration conf) {
    this(conf.getInt(
          DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES,
          DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES_DEFAULT),
        conf.getLong(
          DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
          DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT));
  }

  /**
   * 构造块报告租约管理器，指定最大租约数和过期时间
   * @param maxPending 最大同时发放租约数量
   * @param leaseExpiryMs 租约过期毫秒数
   */
  BlockReportLeaseManager(int maxPending, long leaseExpiryMs) {
    Preconditions.checkArgument(maxPending >= 1,
        "Cannot set the maximum number of block report leases to a " +
            "value less than 1.");
    this.maxPending = maxPending;
    Preconditions.checkArgument(leaseExpiryMs >= 1,
        "Cannot set full block report lease expiry period to a value " +
         "less than 1.");
    this.leaseExpiryMs = leaseExpiryMs;
  }

  /**
   * 获取下一个可用的非零租约ID
   * @return 非零租约ID
   */
  private long getNextId() {
    return ++nextId == 0L ? ++nextId : nextId;
  }

  /**
   * 注册一个新的DataNode到租约管理器
   * @param dn 待注册的DataNode描述信息
   */
  public synchronized void register(DatanodeDescriptor dn) {
    registerNode(dn);
  }

  /**
   * 内部方法：注册DataNode到租约管理器
   * @param dn 待注册的DataNode描述信息
   * @return 注册生成的节点信息对象，注册失败返回null
   */
  private synchronized NodeData registerNode(DatanodeDescriptor dn) {
    if (nodes.containsKey(dn.getDatanodeUuid())) {
      LOG.info("Can't register DN {} ({}) because it is already registered.",
          dn.getDatanodeUuid(), dn.getXferAddr());
      return null;
    }
    NodeData node = new NodeData(dn.getDatanodeUuid());
    deferredHead.addToBeginning(node);
    nodes.put(dn.getDatanodeUuid(), node);
    LOG.info("Registered DN {} ({}).", dn.getDatanodeUuid(), dn.getXferAddr());
    return node;
  }

  /**
   * 内部方法：移除指定节点的租约信息，并从链表中删除
   * @param node 待移除节点信息
   */
  private synchronized void remove(NodeData node) {
    if (node.leaseId != 0) {
      numPending--;
      node.leaseId = 0;
      node.leaseTimeMs = 0;
    }
    node.removeSelf();
  }

  /**
   * 从租约管理器注销一个DataNode
   * @param dn 待注销的DataNode描述信息
   */
  public synchronized void unregister(DatanodeDescriptor dn) {
    NodeData node = nodes.remove(dn.getDatanodeUuid());
    if (node == null) {
      LOG.info("Can't unregister DN {} ({}) because it is not currently " +
          "registered.", dn.getDatanodeUuid(), dn.getXferAddr());
      return;
    }
    remove(node);
  }

  /**
   * 处理DataNode的块报告租约申请，尝试发放新租约
   * @param dn 申请租约的DataNode
   * @return 发放的租约ID，无法发放返回0
   */
  public synchronized long requestLease(DatanodeDescriptor dn) {
    NodeData node = nodes.get(dn.getDatanodeUuid());
    if (node == null) {
      LOG.warn("DN {} ({}) requested a lease even though it wasn't yet " +
          "registered. Registering now.", dn.getDatanodeUuid(),
          dn.getXferAddr());
      node = registerNode(dn);
    }
    if (node.leaseId != 0) {
      // DataNode已持有租约仍重新申请，通常是DataNode重启导致，移除原有租约
      LOG.debug("Removing existing BR lease 0x{} for DN {} ({}) in order to " +
               "issue a new one.", Long.toHexString(node.leaseId),
               dn.getDatanodeUuid(), dn.getXferAddr());
    }
    remove(node);
    long monotonicNowMs = Time.monotonicNow();
    // 先清理已过期的租约，释放名额
    pruneExpiredPending(monotonicNowMs);
    if (numPending >= maxPending) {
      // 已达到最大租约数量，无法发放新租约
      if (LOG.isDebugEnabled()) {
        StringBuilder allLeases = new StringBuilder();
        String prefix = "";
        for (NodeData cur = pendingHead.next; cur != pendingHead;
             cur = cur.next) {
          allLeases.append(prefix).append(cur.datanodeUuid);
          prefix = ", ";
        }
        LOG.debug("Can't create a new BR lease for DN {} ({}), because " +
              "numPending equals maxPending at {}. Current leases: {}",
              dn.getDatanodeUuid(), dn.getXferAddr(), numPending, allLeases);
      }
      return 0;
    }
    numPending++;
    node.leaseId = getNextId();
    node.leaseTimeMs = monotonicNowMs;
    pendingHead.addToEnd(node);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created a new BR lease 0x{} for DN {} ({}). numPending = {}",
          Long.toHexString(node.leaseId), dn.getDatanodeUuid(), dn.getXferAddr(), numPending);
    }
    return node.leaseId;
  }

  /**
   * 检查指定节点租约是否过期，过期则移除回收
   * @param monotonicNowMs 当前时间戳
   * @param node 待检查节点
   * @return true表示租约已过期并移除，false表示租约仍有效
   */
  private synchronized boolean pruneIfExpired(long monotonicNowMs,
                                              NodeData node) {
    if (monotonicNowMs - node.leaseTimeMs < leaseExpiryMs) {
      return false;
    }
    LOG.info("Removing expired block report lease 0x{} for DN {}.",
        Long.toHexString(node.leaseId), node.datanodeUuid);
    Preconditions.checkState(node.leaseId != 0);
    remove(node);
    deferredHead.addToBeginning(node);
    return true;
  }

  /**
   * 遍历清理pending链表中所有已过期的租约，从链表头部开始清理，遇到第一个未过期租约即停止
   * @param monotonicNowMs 当前时间戳
   */
  private synchronized void pruneExpiredPending(long monotonicNowMs) {
    NodeData cur = pendingHead.next;
    while (cur != pendingHead) {
      NodeData next = cur.next;
      if (!pruneIfExpired(monotonicNowMs, cur)) {
        return;
      }
      cur = next;
    }
    LOG.trace("No entries remaining in the pending list.");
  }

  /**
   * 验证DataNode发送块报告携带的租约是否有效
   * @param dn 发送块报告的DataNode
   * @param monotonicNowMs 当前时间戳
   * @param id 块报告携带的租约ID
   * @return true表示租约有效，接受块报告；false表示租约无效，拒绝块报告
   */
  public synchronized boolean checkLease(DatanodeDescriptor dn,
                                         long monotonicNowMs, long id) {
    if (id == 0) {
      // 租约ID为0，直接跳过限流，接受块报告
      LOG.debug("Datanode {} ({}) is using BR lease id 0x0 to bypass " +
          "rate-limiting.", dn.getDatanodeUuid(), dn.getXferAddr());
      return true;
    }
    NodeData node = nodes.get(dn.getDatanodeUuid());
    if (node == null) {
      LOG.info("BR lease 0x{} is not valid for unknown datanode {} ({})",
          Long.toHexString(id), dn.getDatanodeUuid(), dn.getXferAddr());
      return false;
    }
    if (node.leaseId == 0) {
      LOG.warn("BR lease 0x{} is not valid for DN {} ({}), because the DN " +
               "is not in the pending set.",
               Long.toHexString(id), dn.getDatanodeUuid(), dn.getXferAddr());
      return false;
    }
    if (pruneIfExpired(monotonicNowMs, node)) {
      LOG.warn("BR lease 0x{} is not valid for DN {} ({}), because the lease " +
          "has expired.", Long.toHexString(id), dn.getDatanodeUuid(), dn.getXferAddr());
      return false;
    }
    if (id != node.leaseId) {
      LOG.warn("BR lease 0x{} is not valid for DN {} ({}). Expected BR lease 0x{}.",
          Long.toHexString(id), dn.getDatanodeUuid(), dn.getXferAddr(),
          Long.toHexString(node.leaseId));
      return false;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("BR lease 0x{} is valid for DN {} ({}).",
          Long.toHexString(id), dn.getDatanodeUuid(), dn.getXferAddr());
    }
    return true;
  }

  /**
   * 移除并回收DataNode已使用完的块报告租约
   * @param dn 对应DataNode
   * @return 被移除的租约ID，无租约返回0
   */
  public synchronized long removeLease(DatanodeDescriptor dn) {
    NodeData node = nodes.get(dn.getDatanodeUuid());
    if (node == null) {
      LOG.info("Can't remove lease for unknown datanode {} ({})",
          dn.getDatanodeUuid(), dn.getXferAddr());
      return 0;
    }
    long id = node.leaseId;
    if (id == 0) {
      LOG.debug("DN {} ({}) has no lease to remove.", dn.getDatanodeUuid(), dn.getXferAddr());
      return 0;
    }
    remove(node);
    deferredHead.addToEnd(node);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Removed BR lease 0x{} for DN {} ({}). numPending = {}",
          Long.toHexString(id), dn.getDatanodeUuid(), dn.getXferAddr(), numPending);
    }
    return id;
  }
}