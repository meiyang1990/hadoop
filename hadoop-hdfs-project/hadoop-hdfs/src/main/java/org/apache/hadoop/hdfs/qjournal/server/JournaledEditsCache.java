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
package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.hadoop.classification.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.Preconditions;

/**
 * 序列化编辑日志的内存缓存，用于支持QJM的增量获取编辑日志请求
 * 当启用增量同步尾日志（{@value DFSConfigKeys#DFS_HA_TAILEDITS_INPROGRESS_KEY}）时，
 * 该缓存用于响应{@link Journal#getJournaledEdits(long, int)}调用，向备用NameNode返回增量编辑日志。
 *
 * <p>JournalNode收到一批编辑日志后，会通过{@link #storeEdits(byte[], long, long, int)}放入缓存。
 * 缓存要求存储连续事务ID的编辑日志；如果新写入的批次和之前存储的编辑不连续，缓存会被清空后再存储新批次，
 * 这是因为缓存设计仅服务正常同步流程，间隙处理仅在恢复模式进行。
 *
 * <p>编辑批次存储在{@link TreeMap}中，key为批次的起始事务ID，value为序列化后的编辑数据。
 * 检索请求时，拼接相关数据缓冲区并添加头信息，构造完整的编辑日志流返回。
 *
 * <p>缓存容量由{@value DFSConfigKeys#DFS_JOURNALNODE_EDIT_CACHE_SIZE_KEY}配置决定，单位为字节。
 * 新增批次后如果超过容量限制，会从最老的事务批次开始删除，直到总容量低于限制。经验规则是每个事务约需200字节，
 * 建议通过{@link JournalMetrics#rpcRequestCacheMissAmount}指标监控缓存缺失情况，以此判断缓存容量是否足够。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class JournaledEditsCache {

  private static final int INVALID_LAYOUT_VERSION = 0;
  private static final long INVALID_TXN_ID = -1;

  /** 缓存总容量，单位字节 */
  private final long capacity;

  /**
   * 包装为AutoCloseable的读写锁，底层共用同一个锁实例
   */
  private final AutoCloseableLock readLock;
  private final AutoCloseableLock writeLock;

  // ** 以下字段受读写锁保护 **

  /**
   * 存储编辑日志数据：key为批次起始事务ID，value为序列化后的编辑批次数据
   * 存储的事务一定是连续的，即上一批次的最后事务ID一定比下一批次起始事务ID小1
   * 整个Map受锁保护，但单个数据缓冲区是不可变的，可在无锁情况下访问
   */
  private final NavigableMap<Long, byte[]> dataMap = new TreeMap<>();
  /** 当前缓存中编辑日志使用的布局版本 */
  private int layoutVersion = INVALID_LAYOUT_VERSION;
  /** 当前布局版本对应的编辑日志头序列化结果 */
  private ByteBuffer layoutHeader;

  /**
   * 缓存中当前存在的事务ID范围，最低和最高事务ID
   * 如果缓存中没有事务则为{@value INVALID_TXN_ID}
   */
  private long lowestTxnId;
  private long highestTxnId;
  /**
   * 自上次缓存重置（初始化或不同步导致清空）以来，缓存曾保存过的最低事务ID
   * 缓存容量未超出限制时，该值等于lowestTxnId
   */
  private long initialTxnId;
  /** 当前缓存所有缓冲区的总大小 */
  private long totalSize;

  // ** 以上字段受读写锁保护 **

  /**
   * 构造基于配置的JournaledEdits缓存实例
   * @param conf Hadoop配置对象
   */
  JournaledEditsCache(Configuration conf) {
    float fraction = conf.getFloat(DFSConfigKeys.DFS_JOURNALNODE_EDIT_CACHE_SIZE_FRACTION_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_EDIT_CACHE_SIZE_FRACTION_DEFAULT);
    Preconditions.checkArgument((fraction > 0 && fraction < 1.0f),
        String.format("Cache config %s is set at %f, it should be a positive float value, " +
            "less than 1.0. The recommended value is less than 0.9.",
            DFSConfigKeys.DFS_JOURNALNODE_EDIT_CACHE_SIZE_FRACTION_KEY, fraction));
    capacity = conf.getLong(DFSConfigKeys.DFS_JOURNALNODE_EDIT_CACHE_SIZE_KEY,
        (long) (Runtime.getRuntime().maxMemory() * fraction));
    if (capacity > 0.9 * Runtime.getRuntime().maxMemory()) {
      Journal.LOG.warn(String.format("Cache capacity is set at %d bytes but " +
          "maximum JVM memory is only %d bytes. It is recommended that you " +
          "decrease the cache size/fraction or increase the heap size.",
          capacity, Runtime.getRuntime().maxMemory()));
    }
    Journal.LOG.info("Enabling the journaled edits cache with a capacity " +
        "of bytes: " + capacity);
    ReadWriteLock lock = new ReentrantReadWriteLock(true);
    readLock = new AutoCloseableLock(lock.readLock());
    writeLock = new AutoCloseableLock(lock.writeLock());
    initialize(INVALID_TXN_ID);
  }

  /**
   * 从缓存检索从指定事务ID开始，最多包含maxTxns个事务的编辑日志
   * 将序列化后的编辑日志填充到输出缓冲区列表，并返回实际包含的事务数量
   * 序列化结果前会添加标准编辑日志头，包含布局版本信息，返回的事务ID保证连续
   *
   * 如果请求起始事务ID大于缓存中最新事务ID，返回空缓冲区和0个事务
   * 如果请求起始事务ID小于缓存中最老事务ID，或缓存为空，则抛出异常
   *
   * @param requestedStartTxn 要返回的第一个事务ID，返回结果保证第一个事务就是此ID
   * @param maxTxns 最多返回的事务数量
   * @param outputBuffers 输出缓冲区列表，拼接后即为完整响应
   * @return 输出缓冲区包含的事务数量
   * @throws IOException 无法从缓存满足请求时抛出异常
   */
  int retrieveEdits(long requestedStartTxn, int maxTxns,
      List<ByteBuffer> outputBuffers) throws IOException {
    int txnCount = 0;

    try (AutoCloseableLock l = readLock.acquire()) {
      // 缓存为空或请求起始ID小于最老事务ID，抛出缓存缺失异常
      if (lowestTxnId == INVALID_TXN_ID || requestedStartTxn < lowestTxnId) {
        throw getCacheMissException(requestedStartTxn);
      // 请求起始ID超过最新事务ID，返回0个事务
      } else if (requestedStartTxn > highestTxnId) {
        return 0;
      }
      // 添加布局版本头到输出缓冲区
      outputBuffers.add(layoutHeader);
      // 获取从请求起始事务所在批次开始的所有后续批次迭代器
      Iterator<Map.Entry<Long, byte[]>> incrBuffIter =
          dataMap.tailMap(dataMap.floorKey(requestedStartTxn), true)
              .entrySet().iterator();
      long prevTxn = requestedStartTxn;
      byte[] prevBuf = null;
      // 循环条件：未达到最大事务数 且 还有数据需要处理
      while ((txnCount < maxTxns) &&
          (incrBuffIter.hasNext() || prevBuf != null)) {
        long currTxn;
        byte[] currBuf;
        if (incrBuffIter.hasNext()) {
          // 读取下一个批次
          Map.Entry<Long, byte[]> ent = incrBuffIter.next();
          currTxn = ent.getKey();
          currBuf = ent.getValue();
        } else {
          // 处理最后一个批次
          currTxn = highestTxnId + 1;
          currBuf = null;
        }
        if (prevBuf != null) { // 第一次迭代不处理，从第二个批次开始
          outputBuffers.add(ByteBuffer.wrap(prevBuf));
          // 累加事务数量，如果起始事务在当前批次内，仅累加需要的部分
          txnCount += currTxn - Math.max(requestedStartTxn, prevTxn);
        }
        prevTxn = currTxn;
        prevBuf = currBuf;
      }
      // 在锁释放后再操作缓冲区（查找事务边界、修改位置限制），减少锁持有时间
    }
    // 裁剪第一个缓冲区，去掉起始事务之前的多余事务
    ByteBuffer firstBuf = outputBuffers.get(1); // 第0位是头，第一个数据块从1开始
    firstBuf.position(
        findTransactionPosition(firstBuf.array(), requestedStartTxn));
    // 如果事务总数超过最大限制，裁剪最后一个缓冲区去掉多余事务
    if (txnCount > maxTxns) {
      ByteBuffer lastBuf = outputBuffers.get(outputBuffers.size() - 1);
      int limit =
          findTransactionPosition(lastBuf.array(), requestedStartTxn + maxTxns);
      lastBuf.limit(limit);
      txnCount = maxTxns;
    }

    return txnCount;
  }

  /**
   * 将一批序列化后的编辑日志存储到缓存中
   * 容量超出限制时会删除最老批次保证总容量不超过配置值，输入异常会优雅处理不抛出异常
   * 保证JournalNode其他操作可以正常进行
   *
   * @param inputData 序列化后的编辑日志数据缓冲区
   * @param newStartTxn 该批次第一个事务的ID
   * @param newEndTxn 该批次最后一个事务的ID
   * @param newLayoutVersion 该批次使用的编辑日志布局版本
   */
  void storeEdits(byte[] inputData, long newStartTxn, long newEndTxn,
      int newLayoutVersion) {
    // 参数合法性检查
    if (newStartTxn < 0 || newEndTxn < newStartTxn) {
      Journal.LOG.error(String.format("Attempted to cache data of length %d " +
          "with newStartTxn %d and newEndTxn %d",
          inputData.length, newStartTxn, newEndTxn));
      return;
    }
    try (AutoCloseableLock l = writeLock.acquire()) {
      // 如果布局版本变更，更新版本并清空缓存
      if (newLayoutVersion != layoutVersion) {
        try {
          updateLayoutVersion(newLayoutVersion, newStartTxn);
        } catch (IOException ioe) {
          Journal.LOG.error(String.format("Unable to save new edits [%d, %d] " +
              "due to exception when updating to new layout version %d",
              newStartTxn, newEndTxn, newLayoutVersion), ioe);
          return;
        }
      // 如果缓存为空，初始化缓存从当前批次开始
      } else if (lowestTxnId == INVALID_TXN_ID) {
        Journal.LOG.info("Initializing edits cache starting from txn ID " +
            newStartTxn);
        initialize(newStartTxn);
      // 如果新批次起始事务不连续，清空缓存避免存储不连续区间
      } else if (highestTxnId + 1 != newStartTxn) {
        // Cache is out of sync; clear to avoid storing noncontiguous regions
        Journal.LOG.error(String.format("Edits cache is out of sync; " +
            "looked for next txn id at %d but got start txn id for " +
            "cache put request at %d. Reinitializing at new request.",
            highestTxnId + 1, newStartTxn));
        initialize(newStartTxn);
      }

      // 容量不足时，从最老批次开始删除直到能容纳新批次
      while ((totalSize + inputData.length) > capacity && !dataMap.isEmpty()) {
        Map.Entry<Long, byte[]> lowest = dataMap.firstEntry();
        dataMap.remove(lowest.getKey());
        totalSize -= lowest.getValue().length;
      }
      // 单个批次大小超过整个缓存容量，清空缓存返回
      if (inputData.length > capacity) {
        initialize(INVALID_TXN_ID);
        Journal.LOG.warn(String.format("A single batch of edits was too " +
                "large to fit into the cache: startTxn = %d, endTxn = %d, " +
                "input length = %d. The cache size (%s) or cache fraction (%s) must be " +
                "increased for it to work properly (current capacity %d)." +
                "Cache is now empty.",
            newStartTxn, newEndTxn, inputData.length,
            DFSConfigKeys.DFS_JOURNALNODE_EDIT_CACHE_SIZE_KEY,
            DFSConfigKeys.DFS_JOURNALNODE_EDIT_CACHE_SIZE_FRACTION_KEY, capacity));
        return;
      }
      // 更新缓存中当前最低事务ID
      if (dataMap.isEmpty()) {
        lowestTxnId = newStartTxn;
      } else {
        lowestTxnId = dataMap.firstKey();
      }

      // 存储新批次，更新最高事务ID和总大小
      dataMap.put(newStartTxn, inputData);
      highestTxnId = newEndTxn;
      totalSize += inputData.length;
    }
  }

  /**
   * 在序列化缓冲区中查找指定事务ID的起始位置，返回该事务之前的字节数
   * @param buf 包含序列化编辑日志的缓冲区
   * @param txnId 要查找的事务ID
   * @return 指定事务起始位置之前的字节偏移量
   */
  private int findTransactionPosition(byte[] buf, long txnId)
      throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(buf);
    FSEditLogLoader.PositionTrackingInputStream tracker =
        new FSEditLogLoader.PositionTrackingInputStream(bais);
    FSEditLogOp.Reader reader = FSEditLogOp.Reader.create(
        new DataInputStream(tracker), tracker, layoutVersion);
    long previousPos = 0;
    while (reader.scanOp() < txnId) {
      previousPos = tracker.getPos();
    }
    // 缓冲区基于字节数组，位置不会超过int范围，直接强转
    return (int) previousPos;
  }

  /**
   * 更新缓存的布局版本，清空所有已有条目，生成新版本对应的编辑日志头
   * @param newLayoutVersion 新的布局版本
   * @param newStartTxn 缓存中新的最低事务ID
   */
  private void updateLayoutVersion(int newLayoutVersion, long newStartTxn)
      throws IOException {
    StringBuilder logMsg = new StringBuilder()
        .append("Updating edits cache to use layout version ")
        .append(newLayoutVersion)
        .append(" starting from txn ID ")
        .append(newStartTxn);
    if (layoutVersion != INVALID_LAYOUT_VERSION) {
      logMsg.append("; previous version was ").append(layoutVersion)
          .append("; old entries will be cleared.");
    }
    Journal.LOG.info(logMsg.toString());
    initialize(newStartTxn);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // 写入新版本对应编辑头到输出流
    EditLogFileOutputStream.writeHeader(newLayoutVersion,
        new DataOutputStream(baos));
    layoutVersion = newLayoutVersion;
    layoutHeader = ByteBuffer.wrap(baos.toByteArray());
  }

  /**
   * 重置缓存到清空状态
   * @param newInitialTxnId 新的初始最低事务ID，如果要保持缓存为空则传{@value INVALID_TXN_ID}
   */
  private void initialize(long newInitialTxnId) {
    dataMap.clear();
    totalSize = 0;
    initialTxnId = newInitialTxnId;
    lowestTxnId = initialTxnId;
    highestTxnId = INVALID_TXN_ID; // 之后会更新
  }

  /**
   * 测试用方法，获取包含指定事务的数据缓冲区
   * @param txnId 事务ID
   * @return 包含该事务