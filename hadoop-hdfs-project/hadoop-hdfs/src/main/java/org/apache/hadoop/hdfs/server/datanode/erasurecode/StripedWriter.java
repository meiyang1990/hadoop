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

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * 擦除编码场景下的条带化写入管理器，管理多个条带块写入器，将重构后的数据写入目标数据节点
 * 负责处理丢失块重构后的数据传输，协调多个目标块的并行写入流程
 */
@InterfaceAudience.Private
class StripedWriter {
  private static final Logger LOG = DataNode.LOG;
  private final static int WRITE_PACKET_SIZE = 64 * 1024;

  private final StripedReconstructor reconstructor;
  private final DataNode datanode;
  private final Configuration conf;

  private final int dataBlkNum;
  private final int parityBlkNum;

  private boolean[] targetsStatus;

  // 目标数据节点信息数组
  private final DatanodeInfo[] targets;
  private final short[] targetIndices;
  private boolean hasValidTargets;
  private final StorageType[] targetStorageTypes;
  private final String[] targetStorageIds;

  private StripedBlockWriter[] writers;

  private int maxChunksPerPacket;
  private byte[] packetBuf;
  private byte[] checksumBuf;
  private int bytesPerChecksum;
  private int checksumSize;

  /**
   * 构造条带化写入管理器，初始化基础配置和目标信息
   * @param reconstructor 条带重构器实例，提供重构上下文和能力
   * @param datanode 当前数据节点实例
   * @param conf Hadoop配置对象
   * @param stripedReconInfo 条带重构信息，包含目标块和节点信息
   */
  StripedWriter(StripedReconstructor reconstructor, DataNode datanode,
      Configuration conf, StripedReconstructionInfo stripedReconInfo) {
    this.reconstructor = reconstructor;
    this.datanode = datanode;
    this.conf = conf;

    dataBlkNum = stripedReconInfo.getEcPolicy().getNumDataUnits();
    parityBlkNum = stripedReconInfo.getEcPolicy().getNumParityUnits();

    this.targets = stripedReconInfo.getTargets();
    assert targets != null;
    this.targetStorageTypes = stripedReconInfo.getTargetStorageTypes();
    assert targetStorageTypes != null;
    this.targetStorageIds = stripedReconInfo.getTargetStorageIds();
    assert targetStorageIds != null;

    writers = new StripedBlockWriter[targets.length];
    targetIndices = new short[targets.length];
    Preconditions.checkArgument(
            targetIndices.length <= dataBlkNum + parityBlkNum - reconstructor.getNumLiveBlocks(),
            "Reconstruction work gets too much targets.");
    Preconditions.checkArgument(targetIndices.length <= parityBlkNum,
        "Too much missed striped blocks.");
    initTargetIndices();
    long maxTargetLength = 0L;
    for (short targetIndex : targetIndices) {
      maxTargetLength = Math.max(maxTargetLength,
          reconstructor.getBlockLen(targetIndex));
    }
    reconstructor.setMaxTargetLength(maxTargetLength);

    // targetsStatus store whether some target is success, it will record
    // any failed target once, if some target failed (invalid DN or transfer
    // failed), will not transfer data to it any more.
    targetsStatus = new boolean[targets.length];
  }

  /**
   * 初始化写入缓冲区、校验信息和目标数据流
   * @throws IOException 当所有目标都初始化失败时抛出异常
   */
  void init() throws IOException {
    DataChecksum checksum = reconstructor.getChecksum();
    checksumSize = checksum.getChecksumSize();
    bytesPerChecksum = checksum.getBytesPerChecksum();
    int chunkSize = bytesPerChecksum + checksumSize;
    // 计算单个数据包最多可容纳多少个数据校验块
    maxChunksPerPacket = Math.max(
        (WRITE_PACKET_SIZE - PacketHeader.PKT_MAX_HEADER_LEN) / chunkSize, 1);
    int maxPacketSize = chunkSize * maxChunksPerPacket
        + PacketHeader.PKT_MAX_HEADER_LEN;

    // 分配数据包缓冲区
    packetBuf = new byte[maxPacketSize];
    int tmpLen = checksumSize *
        (reconstructor.getBufferSize() / bytesPerChecksum);
    // 分配校验和缓冲区
    checksumBuf = new byte[tmpLen];

    // 初始化所有目标数据流，失败计数为0则抛出异常
    if (initTargetStreams() == 0) {
      String error = "All targets are failed.";
      throw new IOException(error);
    }
  }

  /**
   * 初始化需要重构的目标块索引，找出所有丢失且需要恢复的块索引
   */
  private void initTargetIndices() {
    BitSet bitset = reconstructor.getLiveBitSet();
    BitSet excludebitset=reconstructor.getExcludeBitSet();

    int m = 0;
    hasValidTargets = false;
    // 遍历所有条带单元，找出存活块以外需要恢复的块
    for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
      if (!bitset.get(i)) {
        if (reconstructor.getBlockLen(i) > 0) {
          if (m < targets.length && !excludebitset.get(i)) {
            targetIndices[m++] = (short)i;
            hasValidTargets = true;
          }
        }
      }
    }
  }

  /**
   * 将重构完成的数据传输到所有有效目标数据节点
   * @return 成功传输的目标数量
   */
  int transferData2Targets() {
    int nSuccess = 0;
    for (int i = 0; i < targets.length; i++) {
      if (targetsStatus[i]) {
        boolean success = false;
        try {
          writers[i].transferData2Target(packetBuf);
          nSuccess++;
          success = true;
        } catch (IOException e) {
          LOG.warn(e.getMessage());
        }
        targetsStatus[i] = success;
      }
    }
    return nSuccess;
  }

  /**
   * 发送空数据包标记块传输结束，完成所有目标块写入
   */
  void endTargetBlocks() {
    for (int i = 0; i < targets.length; i++) {
      if (targetsStatus[i]) {
        try {
          writers[i].endTargetBlock(packetBuf);
        } catch (IOException e) {
          LOG.warn(e.getMessage());
        }
      }
    }
  }

  /**
   * 初始化所有目标块的输出流，发送创建块请求到目标数据节点
   * @return 初始化成功的目标数量
   */
  int initTargetStreams() {
    int nSuccess = 0;
    for (short i = 0; i < targets.length; i++) {
      try {
        writers[i] = createWriter(i);
        nSuccess++;
        targetsStatus[i] = true;
      } catch (Throwable e) {
        LOG.warn(e.getMessage());
      }
    }
    return nSuccess;
  }

  /**
   * 创建单个条带块写入器实例
   * @param index 目标在目标数组中的索引
   * @return 初始化完成的条带块写入器
   * @throws IOException 创建失败时抛出异常
   */
  private StripedBlockWriter createWriter(short index) throws IOException {
    return new StripedBlockWriter(this, datanode, conf,
        reconstructor.getBlock(targetIndices[index]), targets[index],
        targetStorageTypes[index], targetStorageIds[index]);
  }

  /**
   * 从重构器分配写入缓冲区
   * @return 分配好的字节缓冲区
   */
  ByteBuffer allocateWriteBuffer() {
    return reconstructor.allocateBuffer(reconstructor.getBufferSize());
  }

  /**
   * 获取目标总数
   * @return 目标数量
   */
  int getTargets() {
    return targets.length;
  }

  /**
   * 获取当前状态有效的目标数量
   * @return 有效目标数量
   */
  private int getRealTargets() {
    int m = 0;
    for (int i = 0; i < targets.length; i++) {
      if (targetsStatus[i]) {
        m++;
      }
    }
    return m;
  }

  /**
   * 获取所有有效目标对应的原始条带索引数组
   * @return 有效目标索引数组
   */
  int[] getRealTargetIndices() {
    int realTargets = getRealTargets();
    int[] results = new int[realTargets];
    int m = 0;
    for (int i = 0; i < targets.length; i++) {
      if (targetsStatus[i]) {
        results[m++] = targetIndices[i];
      }
    }
    return results;
  }

  /**
   * 获取所有有效目标的写入缓冲区数组，设置缓冲区限制为本次重构长度
   * @param toReconstructLen 本次需要重构的数据长度
   * @return 目标缓冲区数组
   */
  ByteBuffer[] getRealTargetBuffers(int toReconstructLen) {
    int numGood = getRealTargets();
    ByteBuffer[] outputs = new ByteBuffer[numGood];
    int m = 0;
    for (int i = 0; i < targets.length; i++) {
      if (targetsStatus[i]) {
        writers[i].getTargetBuffer().limit(toReconstructLen);
        outputs[m++] = writers[i].getTargetBuffer();
      }
    }
    return outputs;
  }

  /**
   * 根据块剩余长度更新所有有效目标缓冲区的限制，处理块末尾不足的情况
   * @param toReconstructLen 本次计划重构长度
   */
  void updateRealTargetBuffers(int toReconstructLen) {
    for (int i = 0; i < targets.length; i++) {
      if (targetsStatus[i]) {
        long blockLen = reconstructor.getBlockLen(targetIndices[i]);
        long remaining = blockLen - reconstructor.getPositionInBlock();
        if (remaining <= 0) {
          writers[i].getTargetBuffer().limit(0);
        } else if (remaining < toReconstructLen) {
          writers[i].getTargetBuffer().limit((int)remaining);
        }
      }
    }
  }

  /**
   * 获取校验和缓冲区
   * @return 校验和缓冲区字节数组
   */
  byte[] getChecksumBuf() {
    return checksumBuf;
  }

  /**
   * 获取每个校验和覆盖的数据字节数
   * @return 每个校验和对应数据字节数
   */
  int getBytesPerChecksum() {
    return bytesPerChecksum;
  }

  /**
   * 获取单个校验和的字节大小
   * @return 校验和字节大小
   */
  int getChecksumSize() {
    return checksumSize;
  }

  /**
   * 获取校验和对象
   * @return 数据校验对象
   */
  DataChecksum getChecksum() {
    return reconstructor.getChecksum();
  }

  /**
   * 获取单个数据包最多容纳的块数
   * @return 单个数据包最大块数
   */
  int getMaxChunksPerPacket() {
    return maxChunksPerPacket;
  }

  /**
   * 获取缓存策略
   * @return 缓存策略对象
   */
  CachingStrategy getCachingStrategy() {
    return reconstructor.getCachingStrategy();
  }

  /**
   * 获取目标数据节点传输用的Socket地址
   * @param target 目标数据节点信息
   * @return 传输用Socket地址
   */
  InetSocketAddress getSocketAddress4Transfer(DatanodeInfo target) {
    return reconstructor.getSocketAddress4Transfer(target);
  }

  /**
   * 获取关联的条带重构器实例
   * @return 条带重构器实例
   */
  StripedReconstructor getReconstructor() {
    return reconstructor;
  }

  /**
   * 检查是否存在有效目标
   * @return 是否有需要写入的有效目标
   */
  boolean hasValidTargets() {
    return hasValidTargets;
  }

  /**
   * 清空所有目标缓冲区
   */
  void clearBuffers() {
    for (StripedBlockWriter writer : writers) {
      ByteBuffer targetBuffer =
          writer != null ? writer.getTargetBuffer() : null;
      if (targetBuffer != null) {
        targetBuffer.clear();
      }
    }
  }

  /**
   * 关闭所有写入器，释放所有缓冲区资源
   */
  void close() {
    for (StripedBlockWriter writer : writers) {
      ByteBuffer targetBuffer =
          writer != null ? writer.getTargetBuffer() : null;
      if (targetBuffer != null) {
        reconstructor.freeBuffer(targetBuffer);
        writer.freeTargetBuffer();
      }
    }

    for (int i = 0; i < targets.length; i++) {
      if (writers[i] != null) {
        writers[i].close();
      }
    }
  }
}