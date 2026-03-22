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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap;
import org.apache.hadoop.hdfs.server.common.BlockAlias;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.util.RwLock;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.util.ReflectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.protobuf.ByteString;

/**
 * 文件级注释：管理数据节点本地存储与外部提供存储的多路复用，支持HDFS Provided存储模式，
 * 负责维护提供存储的元数据信息，处理块报告和位置构建，实现外部数据在HDFS中的统一管理。
 * 
 * This class allows us to manage and multiplex between storages local to
 * datanodes, and provided storage.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ProvidedStorageMap {

  private static final Logger LOG =
      LoggerFactory.getLogger(ProvidedStorageMap.class);

  // 当前仅支持一个存储提供者
  private RwLock lock;
  private BlockManager bm;
  private BlockAliasMap aliasMap;

  private final String storageId;
  private final ProvidedDescriptor providedDescriptor;
  private final DatanodeStorageInfo providedStorageInfo;
  private boolean providedEnabled;
  private int defaultReplication;

  /**
   * 构造函数：初始化提供存储管理器，加载块别名映射配置
   * @param lock 全局读写锁
   * @param bm 块管理器引用
   * @param conf Hadoop配置对象
   */
  ProvidedStorageMap(RwLock lock, BlockManager bm, Configuration conf) {

    storageId = conf.get(DFSConfigKeys.DFS_PROVIDER_STORAGEUUID,
        DFSConfigKeys.DFS_PROVIDER_STORAGEUUID_DEFAULT);

    providedEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED,
        DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED_DEFAULT);

    if (!providedEnabled) {
      // 提供存储功能未启用，置空所有相关对象
      aliasMap = null;
      providedDescriptor = null;
      providedStorageInfo = null;
      return;
    }

    DatanodeStorage ds = new DatanodeStorage(
        storageId, State.NORMAL, StorageType.PROVIDED);
    providedDescriptor = new ProvidedDescriptor();
    providedStorageInfo = providedDescriptor.createProvidedStorage(ds);
    this.defaultReplication = conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
        DFSConfigKeys.DFS_REPLICATION_DEFAULT);

    this.bm = bm;
    this.lock = lock;

    // 加载配置指定的块别名映射实现类
    Class<? extends BlockAliasMap> aliasMapClass = conf.getClass(
            DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
            TextFileRegionAliasMap.class, BlockAliasMap.class);
    aliasMap = ReflectionUtils.newInstance(aliasMapClass, conf);

    LOG.info("Loaded alias map class: " +
        aliasMap.getClass() + " storage: " + providedStorageInfo);
  }

  /**
   * 获取数据节点存储信息，如果是提供存储则返回统一的提供存储信息
   * @param dn 数据节点描述符
   * @param s 数据节点存储对象
   * @return 对应存储的信息对象
   * @throws IO 异常
   */
  DatanodeStorageInfo getStorage(DatanodeDescriptor dn, DatanodeStorage s)
      throws IOException {
    if (providedEnabled && storageId.equals(s.getStorageID())) {
      if (StorageType.PROVIDED.equals(s.getStorageType())) {
        // 如果存储从失败恢复为正常，更新状态
        if (providedStorageInfo.getState() == State.FAILED
            && s.getState() == State.NORMAL) {
          providedStorageInfo.setState(State.NORMAL);
          LOG.info("Provided storage transitioning to state " + State.NORMAL);
        }
        // 如果数据节点未注入提供存储，注入它
        if (dn.getStorageInfo(s.getStorageID()) == null) {
          dn.injectStorage(providedStorageInfo);
        }
        // 处理首次块报告
        processProvidedStorageReport();
        return providedDescriptor.getProvidedStorage(dn, s);
      }
      LOG.warn("Reserved storage {} reported as non-provided from {}", s, dn);
    }
    return dn.getStorageInfo(s.getStorageID());
  }

  /**
   * 处理提供存储的首次块报告，从块别名映射加载所有提供块信息并注册到块管理器
   * @throws IOException 加载块信息异常
   */
  private void processProvidedStorageReport()
      throws IOException {
    assert lock.hasWriteLock(RwLockMode.GLOBAL) : "Not holding write lock";
    // 仅在首次块报告或所有数据节点离线后重新上线时处理
    if (providedStorageInfo.getBlockReportCount() == 0
        || providedDescriptor.activeProvidedDatanodes() == 0) {
      LOG.info("Calling process first blk report from storage: "
          + providedStorageInfo);
      // 获取块别名读取器
      BlockAliasMap.Reader<BlockAlias> reader =
          aliasMap.getReader(null, bm.getBlockPoolId());
      if (reader != null) {
        // 处理首次块报告，将所有提供块注册到块管理器
        bm.processFirstBlockReport(providedStorageInfo,
                new ProvidedBlockList(reader.iterator()));
      }
    }
  }

  @VisibleForTesting
  public DatanodeStorageInfo getProvidedStorageInfo() {
    return providedStorageInfo;
  }

  /**
   * 创建LocatedBlocks构建器，如果启用提供存储则返回提供块专用构建器
   * @param maxValue 最大块数量
   * @return 对应构建器实例
   */
  public LocatedBlockBuilder newLocatedBlocks(int maxValue) {
    if (!providedEnabled) {
      return new LocatedBlockBuilder(maxValue);
    }
    return new ProvidedBlocksBuilder(maxValue);
  }

  /**
   * 移除下线数据节点，更新提供存储活跃节点计数
   * @param dnToRemove 要移除的数据节点
   */
  public void removeDatanode(DatanodeDescriptor dnToRemove) {
    if (providedEnabled) {
      assert lock.hasWriteLock(RwLockMode.BM) : "Not holding write lock";
      // 从描述符中移除节点
      providedDescriptor.remove(dnToRemove);
      // 如果所有提供节点都下线，重置块报告计数，下次重新加载
      if (providedDescriptor.activeProvidedDatanodes() == 0) {
        providedStorageInfo.setBlockReportCount(0);
      }
    }
  }

  public long getCapacity() {
    if (providedStorageInfo == null) {
      return 0;
    }
    return providedStorageInfo.getCapacity();
  }

  /**
   * 更新数据节点存储信息，如果是提供存储则注入统一存储信息
   * @param node 数据节点描述符
   * @param storage 存储对象
   */
  public void updateStorage(DatanodeDescriptor node, DatanodeStorage storage) {
    if (isProvidedStorage(storage.getStorageID())) {
      if (StorageType.PROVIDED.equals(storage.getStorageType())) {
        node.injectStorage(providedStorageInfo);
        return;
      } else {
        LOG.warn("Reserved storage {} reported as non-provided from {}",
            storage, node);
      }
    }
    node.updateStorage(storage);
  }

  private boolean isProvidedStorage(String dnStorageId) {
    return providedEnabled && storageId.equals(dnStorageId);
  }

  /**
   * 随机选择一个报告了PROVIDED类型存储的数据节点
   *
   * @return 选中的数据节点描述符，如果有多个则均匀随机选择一个
   */
  public DatanodeDescriptor chooseProvidedDatanode() {
    return providedDescriptor.chooseRandom();
  }

  @VisibleForTesting
  public BlockAliasMap getAliasMap() {
    return aliasMap;
  }

  /**
   * 提供存储专用LocatedBlock构建器，为提供块补充足够的提供存储位置，满足副本数要求
   * Builder used for creating {@link LocatedBlocks} when a block is provided.
   */
  class ProvidedBlocksBuilder extends LocatedBlockBuilder {

    ProvidedBlocksBuilder(int maxBlocks) {
      super(maxBlocks);
    }

    /**
     * 选择一个不在排除列表中的提供存储数据节点
     * @param excludedUUids 排除的节点UUID列表
     * @return 选中的数据节点，无可用节点返回null
     */
    private DatanodeDescriptor chooseProvidedDatanode(
        Set<String> excludedUUids) {
      DatanodeDescriptor dn = providedDescriptor.choose(null, excludedUUids);
      if (dn == null) {
        dn = providedDescriptor.choose(null);
      }
      return dn;
    }

    @Override
    LocatedBlock newLocatedBlock(ExtendedBlock eb,
        DatanodeStorageInfo[] storages, long pos, boolean isCorrupt) {

      List<DatanodeInfoWithStorage> locs = new ArrayList<>();
      List<String> sids = new ArrayList<>();
      List<StorageType> types = new ArrayList<>();
      boolean isProvidedBlock = false;
      Set<String> excludedUUids = new HashSet<>();

      // 遍历所有传入的存储位置，分离普通存储和提供存储
      for (int i = 0; i < storages.length; ++i) {
        DatanodeStorageInfo currInfo = storages[i];
        StorageType storageType = currInfo.getStorageType();
        sids.add(currInfo.getStorageID());
        types.add(storageType);
        if (StorageType.PROVIDED.equals(storageType)) {
          // 标记这是提供块，后续补充提供存储位置
          isProvidedBlock = true;
        } else {
          // 添加普通存储位置
          locs.add(new DatanodeInfoWithStorage(
              currInfo.getDatanodeDescriptor(),
              currInfo.getStorageID(), storageType));
          excludedUUids.add(currInfo.getDatanodeDescriptor().getDatanodeUuid());
        }
      }

      int numLocations = locs.size();
      if (isProvidedBlock) {
        // 添加第一个提供存储位置
        DatanodeDescriptor dn = chooseProvidedDatanode(excludedUUids);
        locs.add(
            new DatanodeInfoWithStorage(dn, storageId, StorageType.PROVIDED));
        excludedUUids.add(dn.getDatanodeUuid());
        numLocations++;
        // 补充提供存储副本直到达到默认副本数
        for (int count = numLocations + 1;
            count <= defaultReplication && count <= providedDescriptor
                .activeProvidedDatanodes(); count++) {
          dn = chooseProvidedDatanode(excludedUUids);
          locs.add(new DatanodeInfoWithStorage(
              dn, storageId, StorageType.PROVIDED));
          sids.add(storageId);
          types.add(StorageType.PROVIDED);
          excludedUUids.add(dn.getDatanodeUuid());
        }
      }
      // 构建并返回LocatedBlock
      return new LocatedBlock(eb,
          locs.toArray(new DatanodeInfoWithStorage[locs.size()]),
          sids.toArray(new String[sids.size()]),
          types.toArray(new StorageType[types.size()]),
          pos, isCorrupt, null);
    }

    @Override
    LocatedBlocks build(DatanodeDescriptor client) {
      // TODO 后续优化：选择靠近客户端的提供存储位置
      return new LocatedBlocks(
          flen, isUC, blocks, last, lastComplete, feInfo, ecPolicy);
    }

    @Override
    LocatedBlocks build() {
      // 随机选择一个提供节点作为客户端位置构建结果
      return build(providedDescriptor.chooseRandom());
    }
  }

  /**
   * 维护所有带提供存储的数据节点的抽象描述符，作为统一的提供存储入口，不会注册到集群拓扑中
   * An abstract DatanodeDescriptor to track datanodes with provided storages.
   * NOTE: never resolved through registerDatanode, so not in the topology.
   */
  public static class ProvidedDescriptor extends DatanodeDescriptor {

    private final NavigableMap<String, DatanodeDescriptor> dns =
        new ConcurrentSkipListMap<>();
    // 单独维护活跃节点列表，加速随机选择操作
    private final List<DatanodeDescriptor> dnR = new ArrayList<>();
    public final static String NETWORK_LOCATION = "/REMOTE";
    public final static String NAME = "PROVIDED";

    /**
     * 构造函数：创建抽象提供存储描述符，使用虚拟ID和端口
     */
    ProvidedDescriptor() {
      super(new DatanodeID(
            null,                         // String ipAddr,
            null,                         // String hostName,
            UUID.randomUUID().toString(), // String datanodeUuid,
            0,                            // int xferPort,
            0,                            // int infoPort,
            0,                            // int infoSecurePort,
            0));                          // int ipcPort
    }

    /**
     * 获取提供存储信息，并将当前数据节点注册到活跃列表
     * @param dn 上报提供存储的数据节点
     * @param s 存储对象
     * @return 提供存储信息对象
     */
    DatanodeStorageInfo getProvidedStorage(
        DatanodeDescriptor dn, DatanodeStorage s) {
      dns.put(dn.getDatanodeUuid(), dn);
      dnR.add(dn);
      return storageMap.get(s.getStorageID());
    }

    /**
     * 创建提供存储信息对象并注册到存储映射
     * @param ds 存储对象
     * @return 创建的存储信息对象
     */
    DatanodeStorageInfo createProvidedStorage(DatanodeStorage ds) {
      assert null == storageMap.get(ds.getStorageID());
      DatanodeStorageInfo storage = new ProvidedDatanodeStorageInfo(this, ds);
      storage.setHeartbeatedSinceFailover(true);
      storageMap.put(storage.getStorageID(), storage);
      return storage;
    }

    DatanodeDescriptor choose(DatanodeDescriptor client) {
      return choose(client, Collections.<String>emptySet());
    }

    /**
     * 根据客户端位置和排除列表选择合适的提供存储节点
     * @param client 客户端所在节点，优先选择同节点
     * @param excludedUUids 需要排除的节点UUID列表
     * @return 选中的节点，无可用节点返回null
     */
    DatanodeDescriptor choose(DatanodeDescriptor client,
        Set<String> excludedUUids) {
      // 优先选择客户端所在节点，如果客户端节点在可用列表中
      if (client != null && !excludedUUids.contains(client.getDatanodeUuid())) {
        DatanodeDescriptor dn = dns.get(client.getDatanodeUuid());
        if (dn != null) {
          return dn;
        }
      }
      // 优先选择在线节点
      DatanodeDescriptor dn = chooseRandomNode(excludedUUids, true);
      if (dn == null) {
        // 如果没有在线节点，选择任意可用节点
        dn = chooseRandomNode(excludedUUids, false);
      }
      return dn;
    }

    /**
     * 随机选择一个满足条件的节点，使用Fisher-Yates洗牌算法保证均匀随机
     * @param excludedUUids 排除的节点UUID列表
     * @param preferLiveNodes 是否优先选择在线节点
     * @return