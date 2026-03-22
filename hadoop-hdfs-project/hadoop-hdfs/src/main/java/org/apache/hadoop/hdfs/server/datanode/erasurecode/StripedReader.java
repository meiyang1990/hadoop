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
package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient.CorruptedBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.BlockReadStats;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.StripingChunkReadResult;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;

/**
 * 条带化读取管理器，负责从多个源DataNode读取条带块数据，为纠删码解码提供输入数据。
 * 管理并行读取流程，处理读取失败、超时的故障恢复，保证获取足够的源数据用于解码。
 */
@InterfaceAudience.Private
class StripedReader {
  private static final Logger LOG = DataNode.LOG;

  private final int stripedReadTimeoutInMills;
  private final int stripedReadBufferSize;

  private StripedReconstructor reconstructor;
  private final DataNode datanode;
  private final Configuration conf;

  private final int dataBlkNum;
  private final int parityBlkNum;


  private DataChecksum checksum;
  // Striped read buffer size
  private int bufferSize;
  private int[] successList;

  private final int minRequiredSources;
  // the number of xmits used by the re-construction task.
  private final int xmits;
  // 存储长度为0的条带块对应的缓冲区和索引
  private ByteBuffer[] zeroStripeBuffers;
  private short[] zeroStripeIndices;

  // 存活块索引数组
  private final byte[] liveIndices;
  // 源DataNode节点数组
  private final DatanodeInfo[] sources;

  private final List<StripedBlockReader> readers;

  private final Map<Future<BlockReadStats>, Integer> futures = new HashMap<>();
  private final CompletionService<BlockReadStats> readService;

  /**
   * 构造条带化读取器，初始化纠删码重建所需的配置和元数据。
   * @param reconstructor 纠删码重建器实例
   * @param datanode 当前DataNode实例
   * @param conf Hadoop配置
   * @param stripedReconInfo 条带重建信息，包含EC策略、块组信息、源节点等
   */
  StripedReader(StripedReconstructor reconstructor, DataNode datanode,
      Configuration conf, StripedReconstructionInfo stripedReconInfo) {
    stripedReadTimeoutInMills = conf.getInt(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_TIMEOUT_MILLIS_KEY,
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_TIMEOUT_MILLIS_DEFAULT);
    stripedReadBufferSize = conf.getInt(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_KEY,
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_DEFAULT);

    this.reconstructor = reconstructor;
    this.datanode = datanode;
    this.conf = conf;

    dataBlkNum = stripedReconInfo.getEcPolicy().getNumDataUnits();
    parityBlkNum = stripedReconInfo.getEcPolicy().getNumParityUnits();

    int cellsNum = (int) ((stripedReconInfo.getBlockGroup().getNumBytes() - 1)
        / stripedReconInfo.getEcPolicy().getCellSize() + 1);
    minRequiredSources = Math.min(cellsNum, dataBlkNum);

    if (minRequiredSources < dataBlkNum) {
      int zeroStripNum = dataBlkNum - minRequiredSources;
      zeroStripeBuffers = new ByteBuffer[zeroStripNum];
      zeroStripeIndices = new short[zeroStripNum];
    }

    // It is calculated by the maximum number of connections from either sources
    // or targets.
    xmits = Math.max(minRequiredSources,
        stripedReconInfo.getTargets() != null ?
        stripedReconInfo.getTargets().length : 0);

    this.liveIndices = stripedReconInfo.getLiveIndices();
    assert liveIndices != null;
    this.sources = stripedReconInfo.getSources();
    assert sources != null;

    readers = new ArrayList<>(sources.length);
    readService = reconstructor.createReadService();

    Preconditions.checkArgument(liveIndices.length >= minRequiredSources,
        "No enough live striped blocks.");
    Preconditions.checkArgument(liveIndices.length == sources.length,
        "liveBlockIndices and source datanodes should match");
  }

  /**
   * 初始化读取器：初始化所有块读取器、缓冲区大小和空条带。
   * @throws IOException 初始化失败抛出异常
   */
  void init() throws IOException {
    initReaders();

    initBufferSize();

    initZeroStrip();
  }

  /**
   * 初始化块读取器，预先创建minRequiredSources个可用的读取器，保证至少获取最小要求的源节点。
   * @throws IOException 无法获取足够源节点时抛出异常
   */
  private void initReaders() throws IOException {
    // Store the array indices of source DNs we have read successfully.
    // In each iteration of read, the successList list may be updated if
    // some source DN is corrupted or slow. And use the updated successList
    // list of DNs for next iteration read.
    successList = new int[minRequiredSources];

    StripedBlockReader reader;
    int nSuccess = 0;
    for (int i = 0; i < sources.length && nSuccess < minRequiredSources; i++) {
      reader = createReader(i, 0);
      readers.add(reader);
      if (reader.getBlockReader() != null) {
        initOrVerifyChecksum(reader);
        successList[nSuccess++] = i;
      }
    }

    if (nSuccess < minRequiredSources) {
      String error = "Can't find minimum sources required by "
          + "reconstruction, block id: "
          + reconstructor.getBlockGroup().getBlockId();
      throw new IOException(error);
    }
  }

  /**
   * 创建指定源索引处的条带块读取器。
   * @param idxInSources 源数组中的索引
   * @param offsetInBlock 块内读取偏移量
   * @return 创建好的条带块读取器实例
   */
  StripedBlockReader createReader(int idxInSources, long offsetInBlock) {
    return new StripedBlockReader(this, datanode,
        conf, liveIndices[idxInSources],
        reconstructor.getBlock(liveIndices[idxInSources]),
        sources[idxInSources], offsetInBlock);
  }

  /**
   * 根据校验和对齐要求，计算最终的读取缓冲区大小。
   */
  private void initBufferSize() {
    int bytesPerChecksum = checksum.getBytesPerChecksum();
    // The bufferSize is flat to divide bytesPerChecksum
    int readBufferSize = stripedReadBufferSize;
    bufferSize = readBufferSize < bytesPerChecksum ? bytesPerChecksum :
        readBufferSize - readBufferSize % bytesPerChecksum;
  }

  // 从第一个块读取器初始化校验和，后续读取器校验和需和第一个保持一致
  private void initOrVerifyChecksum(StripedBlockReader reader) {
    if (checksum == null) {
      checksum = reader.getBlockReader().getDataChecksum();
    } else {
      assert reader.getBlockReader().getDataChecksum().equals(checksum);
    }
  }

  /**
   * 从重建器分配指定大小的读取缓冲区。
   * @return 分配好的字节缓冲区
   */
  protected ByteBuffer allocateReadBuffer() {
    return reconstructor.allocateBuffer(getBufferSize());
  }

  /**
   * 初始化空条带的缓冲区和索引，用于处理长度为0的块。
   */
  private void initZeroStrip() {
    if (zeroStripeBuffers != null) {
      for (int i = 0; i < zeroStripeBuffers.length; i++) {
        zeroStripeBuffers[i] = reconstructor.allocateBuffer(bufferSize);
      }
    }

    BitSet bitset = reconstructor.getLiveBitSet();
    int k = 0;
    // 遍历所有条带，收集已损坏且长度为0的块索引存入零条带数组
    for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
      if (!bitset.get(i)) {
        if (reconstructor.getBlockLen(i) <= 0) {
          zeroStripeIndices[k++] = (short)i;
        }
      }
    }
  }

  /**
   * 计算本次读取实际需要读取的长度，不超过块剩余长度和重建要求长度。
   * @param index 块索引
   * @param reconstructLength 重建要求长度
   * @return 实际读取长度
   */
  private int getReadLength(int index, int reconstructLength) {
    // the reading length should not exceed the length for reconstruction
    long blockLen = reconstructor.getBlockLen(index);
    long remaining = blockLen - reconstructor.getPositionInBlock();
    return (int) Math.min(remaining, reconstructLength);
  }

  /**
   * 获取解码所需的所有输入缓冲区，包含成功读取的块和零填充的空块。
   * @param toReconstructLen 本次重建长度
   * @return 输入缓冲区数组，按块索引排列
   */
  ByteBuffer[] getInputBuffers(int toReconstructLen) {
    ByteBuffer[] inputs = new ByteBuffer[dataBlkNum + parityBlkNum];

    // 填充成功读取的块数据
    for (int i = 0; i < successList.length; i++) {
      int index = successList[i];
      StripedBlockReader reader = getReader(index);
      ByteBuffer buffer = reader.getReadBuffer();
      paddingBufferToLen(buffer, toReconstructLen);
      inputs[reader.getIndex()] = (ByteBuffer)buffer.flip();
    }

    // 填充长度为0的空块，用零补齐
    if (successList.length < dataBlkNum) {
      for (int i = 0; i < zeroStripeBuffers.length; i++) {
        ByteBuffer buffer = zeroStripeBuffers[i];
        paddingBufferToLen(buffer, toReconstructLen);
        int index = zeroStripeIndices[i];
        inputs[index] = (ByteBuffer)buffer.flip();
      }
    }

    return inputs;
  }

  /**
   * 将缓冲区填充到指定长度，不足部分补零。
   * @param buffer 目标缓冲区
   * @param len 目标长度
   */
  private void paddingBufferToLen(ByteBuffer buffer, int len) {
    if (len > buffer.limit()) {
      buffer.limit(len);
    }
    int toPadding = len - buffer.position();
    for (int i = 0; i < toPadding; i++) {
      buffer.put((byte) 0);
    }
  }

  /**
   * 读取重建所需最小数量的源数据，处理失败/超时自动切换到其他源节点，更新成功列表。
   * 读取完成后上报损坏块给NameNode。
   * @param reconstructLength 本次需要重建的数据长度
   * @throws IOException 无法读取足够源数据时抛出异常
   */
  void readMinimumSources(int reconstructLength) throws IOException {
    CorruptedBlocks corruptedBlocks = new CorruptedBlocks();
    try {
      successList = doReadMinimumSources(reconstructLength, corruptedBlocks);
    } finally {
      // 上报检测到的损坏块给NameNode
      datanode.reportCorruptedBlocks(corruptedBlocks);
    }
  }

  /**
   * 实际执行最小源数据读取，并行读取并处理失败、超时故障，动态替换故障源。
   * @param reconstructLength 本次重建长度
   * @param corruptedBlocks 用于收集损坏块
   * @return 更新后的成功源索引数组
   * @throws IOException 无法获取足够源数据时抛出异常
   */
  int[] doReadMinimumSources(int reconstructLength,
                             CorruptedBlocks corruptedBlocks)
      throws IOException {
    Preconditions.checkArgument(reconstructLength >= 0 &&
        reconstructLength <= bufferSize);
    int nSuccess = 0;
    int[] newSuccess = new int[minRequiredSources];
    BitSet usedFlag = new BitSet(sources.length);
    /*
     * Read from minimum source DNs required, the success list contains
     * source DNs which we think best.
     */
    // 先提交上一轮成功列表中的源节点进行读取
    for (int i = 0; i < minRequiredSources; i++) {
      StripedBlockReader reader = readers.get(successList[i]);
      int toRead = getReadLength(liveIndices[successList[i]],
          reconstructLength);
      if (toRead > 0) {
        Callable<BlockReadStats> readCallable =
            reader.readFromBlock(toRead, corruptedBlocks);
        Future<BlockReadStats> f = readService.submit(readCallable);
        futures.put(f, successList[i]);
      } else {
        // 读取长度为0，无需实际读取，直接加入成功列表
        reader.getReadBuffer().position(0);
        newSuccess[nSuccess++] = successList[i];
      }
      usedFlag.set(successList[i]);
    }

    // 处理已完成的读取结果
    while (!futures.isEmpty()) {
      try {
        StripingChunkReadResult result =
            StripedBlockUtil.getNextCompletedStripedRead(
                readService, futures, stripedReadTimeoutInMills);
        int resultIndex = -1;
        if (result.state == StripingChunkReadResult.SUCCESSFUL) {
          // 读取成功，记录结果索引
          resultIndex = result.index;
        } else if (result.state == StripingChunkReadResult.FAILED) {
          // If read failed for some source DN, we should not use it anymore
          // and schedule read from another source DN.
          // 读取失败，关闭失败读取器，调度新源节点读取
          StripedBlockReader failedReader = readers.get(result.index);
          failedReader.closeBlockReader();
          resultIndex = scheduleNewRead(usedFlag,
              reconstructLength, corruptedBlocks);
        } else if (result.state == StripingChunkReadResult.TIMEOUT) {
          // 读取超时，同样调度新源节点读取
          resultIndex = scheduleNewRead(usedFlag,
              reconstructLength, corruptedBlocks);
        }
        if (resultIndex >= 0) {
          // 获取到有效成功源，加入成功列表
          newSuccess[nSuccess++] = resultIndex;
          if (nSuccess >= minRequiredSources) {
            // 已经收集到足够源，取消剩余未完成读取，清理资源
            cancelReads(futures.keySet());
            clearFuturesAndService();
            break;
          }
        }
      } catch (InterruptedException e) {
        LOG.info("Read data interrupted.", e);
        cancelReads(futures.keySet());
        clearFuturesAndService();
        break;
      }
    }

    if (nSuccess < minRequiredSources) {
      String error = "Can't read data from minimum number of sources "
          + "required by reconstruction, block id: " +
          reconstructor.getBlockGroup().getBlockId();
      throw new IOException(error);
    }

    return newSuccess;
  }

  /**
   * 当原有源读取失败/超时后，调度一个新的源节点进行读取。
   * 优先从未使用过的新源创建读取器，没有新源则复用已创建但未使用的读取器。
   * @param used 当前迭代已使用的源标记
   * @param reconstructLength 本次重建长度
   * @param corruptedBlocks 损坏块收集器
   * @return 如果不需要实际读取返回源索引，否则返回-1
   */
  private int scheduleNewRead(BitSet used, int reconstructLength,
                              CorruptedBlocks corruptedBlocks) {
    StripedBlockReader reader = null;
    // step1: 优先尝试从未