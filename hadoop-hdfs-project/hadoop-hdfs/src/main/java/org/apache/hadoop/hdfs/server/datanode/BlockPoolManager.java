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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Sets;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.slf4j.Logger;

/**
 * 文件: BlockPoolManager.java
 * 所属模块: HDFS DataNode服务端核心模块
 * 核心职责: 管理DataNode上所有块池服务(BPOfferService)，负责所有块池服务的创建、启动、停止、删除和刷新操作，
 * 支持HDFS联邦和NameNode HA架构下多命名服务的动态管理
 * 
 * Manages the BPOfferService objects for the data node.
 * Creation, removal, starting, stopping, shutdown on BPOfferService
 * objects must be done via APIs in this class.
 */
@InterfaceAudience.Private
class BlockPoolManager {
  private static final Logger LOG = DataNode.LOG;
  
  // 通过命名服务ID索引块池服务
  private final Map<String, BPOfferService> bpByNameserviceId =
    Maps.newHashMap();
  // 通过块池ID索引块池服务
  private final Map<String, BPOfferService> bpByBlockPoolId =
    Maps.newHashMap();
  // 所有块池服务列表，使用CopyOnWriteArrayList保证并发迭代安全
  private final List<BPOfferService> offerServices =
      new CopyOnWriteArrayList<>();

  // 所属DataNode实例
  private final DataNode dn;

  // 保证refreshNamenodes操作互斥的锁对象
  private final Object refreshNamenodesLock = new Object();
  
  /**
   * 构造函数，绑定所属DataNode实例
   * @param dn DataNode实例
   */
  BlockPoolManager(DataNode dn) {
    this.dn = dn;
  }
  
  /**
   * 将已有的块池服务按块池ID添加到索引中
   * @param bpos 块池服务实例
   */
  synchronized void addBlockPool(BPOfferService bpos) {
    Preconditions.checkArgument(offerServices.contains(bpos),
        "Unknown BPOS: %s", bpos);
    if (bpos.getBlockPoolId() == null) {
      throw new IllegalArgumentException("Null blockpool id");
    }
    bpByBlockPoolId.put(bpos.getBlockPoolId(), bpos);
  }
  
  /**
   * Returns a list of BPOfferService objects. The underlying list
   * implementation is a CopyOnWriteArrayList so it can be safely
   * iterated while BPOfferServices are being added or removed.
   *
   * Caution: The BPOfferService returned could be shutdown any time.
   * @return 返回所有块池服务的不可修改列表
   */
  synchronized List<BPOfferService> getAllNamenodeThreads() {
    return Collections.unmodifiableList(offerServices);
  }
      
  /**
   * 根据块池ID获取对应块池服务实例
   * @param bpid 块池ID
   * @return 对应块池服务实例，不存在则返回null
   */
  synchronized BPOfferService get(String bpid) {
    return bpByBlockPoolId.get(bpid);
  }
  
  /**
   * 从管理器中移除指定块池服务，清理所有索引
   * @param t 待移除的块池服务实例
   */
  synchronized void remove(BPOfferService t) {
    offerServices.remove(t);
    if (t.hasBlockPoolId()) {
      // 块池从未成功注册到NameNode时，可能未添加到该索引，需要判断处理
      bpByBlockPoolId.remove(t.getBlockPoolId());
    }
    
    boolean removed = false;
    for (Iterator<BPOfferService> it = bpByNameserviceId.values().iterator();
         it.hasNext() && !removed;) {
      BPOfferService bpos = it.next();
      if (bpos == t) {
        it.remove();
        LOG.info("Removed " + bpos);
        removed = true;
      }
    }
    
    if (!removed) {
      LOG.warn("Couldn't remove BPOS " + t + " from bpByNameserviceId map");
    }
  }
  
  /**
   * 关闭指定列表中所有块池服务，先中断线程再等待线程退出
   * @param bposList 待关闭的块池服务列表
   * @throws InterruptedException 等待线程退出时被中断抛出
   */
  void shutDownAll(List<BPOfferService> bposList) throws InterruptedException {
    for (BPOfferService bpos : bposList) {
      bpos.stop(); //中断服务线程
    }
    //等待所有线程退出
    for (BPOfferService bpos : bposList) {
      bpos.join();
    }
  }
  
  /**
   * 启动所有已注册的块池服务，以登录用户身份执行启动操作
   * @throws IOException 获取用户信息或启动失败抛出
   */
  synchronized void startAll() throws IOException {
    try {
      UserGroupInformation.getLoginUser().doAs(
          new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
              for (BPOfferService bpos : offerServices) {
                bpos.start();
              }
              return null;
            }
          });
    } catch (InterruptedException ex) {
      IOException ioe = new IOException();
      ioe.initCause(ex.getCause());
      throw ioe;
    }
  }
  
  /**
   * 等待所有块池服务线程退出
   */
  void joinAll() {
    for (BPOfferService bpos: this.getAllNamenodeThreads()) {
      bpos.join();
    }
  }
  
  /**
   * 刷新NameNode地址配置，动态添加/删除/更新命名服务，无需重启DataNode
   * @param conf 新的配置对象
   * @throws IOException 获取NameNode地址失败或无有效服务配置抛出
   */
  void refreshNamenodes(Configuration conf)
      throws IOException {
    LOG.info("Refresh request received for nameservices: " +
        conf.get(DFSConfigKeys.DFS_NAMESERVICES));

    Map<String, Map<String, InetSocketAddress>> newAddressMap = null;
    Map<String, Map<String, InetSocketAddress>> newLifelineAddressMap = null;

    try {
      //从配置解析所有NameNode服务RPC地址
      newAddressMap =
          DFSUtil.getNNServiceRpcAddressesForCluster(conf);
      //从配置解析所有NameNode生命线RPC地址
      newLifelineAddressMap =
          DFSUtil.getNNLifelineRpcAddressesForCluster(conf);
    } catch (IOException ioe) {
      LOG.warn("Unable to get NameNode addresses.", ioe);
    }

    if (newAddressMap == null || newAddressMap.isEmpty()) {
      throw new IOException("No services to connect, missing NameNode " +
          "address.");
    }

    synchronized (refreshNamenodesLock) {
      //执行实际的刷新逻辑
      doRefreshNamenodes(newAddressMap, newLifelineAddressMap);
    }
  }
  
  /**
   * 实际执行NameNode地址刷新的核心逻辑，区分新增、删除、更新三种场景处理
   * @param addrMap 各命名服务下NameNode服务地址映射
   * @param lifelineAddrMap 各命名服务下NameNode生命线地址映射
   * @throws IOException 刷新过程中发生IO异常抛出
   */
  private void doRefreshNamenodes(
      Map<String, Map<String, InetSocketAddress>> addrMap,
      Map<String, Map<String, InetSocketAddress>> lifelineAddrMap)
      throws IOException {
    assert Thread.holdsLock(refreshNamenodesLock);

    Set<String> toRefresh = new LinkedHashSet<>();
    Set<String> toAdd = new LinkedHashSet<>();
    Set<String> toRemove;
    
    synchronized (this) {
      // 第一步：区分已有命名服务（需要刷新）和新命名服务（需要新增）
      for (String nameserviceId : addrMap.keySet()) {
        if (bpByNameserviceId.containsKey(nameserviceId)) {
          toRefresh.add(nameserviceId);
        } else {
          toAdd.add(nameserviceId);
        }
      }
      
      // 第二步：计算需要删除的已不存在的命名服务
      toRemove = Sets.difference(
          bpByNameserviceId.keySet(), addrMap.keySet());
      
      assert toRefresh.size() + toAdd.size() ==
        addrMap.size() :
          "toAdd: " + Joiner.on(",").useForNull("<default>").join(toAdd) +
          "  toRemove: " + Joiner.on(",").useForNull("<default>").join(toRemove) +
          "  toRefresh: " + Joiner.on(",").useForNull("<default>").join(toRefresh);

      
      // 第三步：启动新增的命名服务
      if (!toAdd.isEmpty()) {
        LOG.info("Starting BPOfferServices for nameservices: " +
            Joiner.on(",").useForNull("<default>").join(toAdd));
      
        for (String nsToAdd : toAdd) {
          Map<String, InetSocketAddress> nnIdToAddr = addrMap.get(nsToAdd);
          Map<String, InetSocketAddress> nnIdToLifelineAddr =
              lifelineAddrMap.get(nsToAdd);
          ArrayList<InetSocketAddress> addrs =
              Lists.newArrayListWithCapacity(nnIdToAddr.size());
          ArrayList<String> nnIds =
              Lists.newArrayListWithCapacity(nnIdToAddr.size());
          ArrayList<InetSocketAddress> lifelineAddrs =
              Lists.newArrayListWithCapacity(nnIdToAddr.size());
          //提取当前命名服务下所有NameNode的地址和ID
          for (String nnId : nnIdToAddr.keySet()) {
            addrs.add(nnIdToAddr.get(nnId));
            nnIds.add(nnId);
            lifelineAddrs.add(nnIdToLifelineAddr != null ?
                nnIdToLifelineAddr.get(nnId) : null);
          }
          //创建新块池服务并添加到索引
          BPOfferService bpos = createBPOS(nsToAdd, nnIds, addrs,
              lifelineAddrs);
          bpByNameserviceId.put(nsToAdd, bpos);
          offerServices.add(bpos);
        }
      }
      //启动所有新增的块池服务
      startAll();
    }

    // 第四步：关闭移除旧命名服务，在同步块外执行避免死锁，因为关闭线程会回调remove方法
    if (!toRemove.isEmpty()) {
      LOG.info("Stopping BPOfferServices for nameservices: " +
          Joiner.on(",").useForNull("<default>").join(toRemove));
      
      for (String nsToRemove : toRemove) {
        BPOfferService bpos = bpByNameserviceId.get(nsToRemove);
        bpos.stop();
        bpos.join();
        // 服务会自行调用remove方法完成清理
      }
    }
    
    // 第五步：更新已有命名服务的NameNode列表
    if (!toRefresh.isEmpty()) {
      LOG.info("Refreshing list of NNs for nameservices: " +
          Joiner.on(",").useForNull("<default>").join(toRefresh));
      
      for (String nsToRefresh : toRefresh) {
        BPOfferService bpos = bpByNameserviceId.get(nsToRefresh);
        Map<String, InetSocketAddress> nnIdToAddr = addrMap.get(nsToRefresh);
        Map<String, InetSocketAddress> nnIdToLifelineAddr =
            lifelineAddrMap.get(nsToRefresh);
        ArrayList<InetSocketAddress> addrs =
            Lists.newArrayListWithCapacity(nnIdToAddr.size());
        ArrayList<InetSocketAddress> lifelineAddrs =
            Lists.newArrayListWithCapacity(nnIdToAddr.size());
        ArrayList<String> nnIds = Lists.newArrayListWithCapacity(
            nnIdToAddr.size());
        //提取更新后的NameNode地址和ID
        for (String nnId : nnIdToAddr.keySet()) {
          addrs.add(nnIdToAddr.get(nnId));
          lifelineAddrs.add(nnIdToLifelineAddr != null ?
              nnIdToLifelineAddr.get(nnId) : null);
          nnIds.add(nnId);
        }
        try {
          //以登录用户身份刷新块池服务的NameNode列表
          UserGroupInformation.getLoginUser()
              .doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception {
                  bpos.refreshNNList(nsToRefresh, nnIds, addrs, lifelineAddrs);
                  return null;
                }
              });
        } catch (InterruptedException ex) {
          IOException ioe = new IOException();
          ioe.initCause(ex.getCause());
          throw ioe;
        }
      }
    }
  }

  /**
   * 创建BPOfferService实例，抽离此方法方便单元测试 mock
   * @param nameserviceId 命名服务ID
   * @param nnIds NameNode ID列表
   * @param nnAddrs NameNode服务地址列表
   * @param lifelineNnAddrs NameNode生命线地址列表
   * @return 新建的BPOfferService实例
   */
  protected BPOfferService createBPOS(
      final String nameserviceId,
      List<String> nnIds,
      List<InetSocketAddress> nnAddrs,
      List<InetSocketAddress> lifelineNnAddrs) {
    return new BPOfferService(nameserviceId, nnIds, nnAddrs, lifelineNnAddrs,
        dn);
  }

  /**
   * 仅用于测试，获取命名服务ID到块池服务的索引映射
   * @return 命名服务ID索引映射
   */
  @VisibleForTesting
  Map<String, BPOfferService> getBpByNameserviceId() {
    return bpByNameserviceId;
  }

  /**
   * 根据块池ID查询当前节点是否被对应NameNode标记为慢节点
   * @param bpId 块池ID
   * @return 如果被标记为慢节点返回true，否则返回false
   */
  boolean isSlownodeByBlockPoolId(String bpId) {
    if (bpByBlockPoolId.containsKey(bpId)) {
      return bpByBlockPoolId.get(bpId).isSlownode();
    }
    return false;
  }

  /**
   * 查询当前节点是否被任意一个块池的NameNode标记为慢节点
   * @只要有一个块池标记为慢节点就返回true，全部不标记返回false
   */
  boolean isSlownode() {
    for (BPOfferService bpOfferService : bpByBlockPoolId.values()) {
      if (bpOfferService.isSlownode()) {
        return true;
      }
    }
    return false;
  }
}