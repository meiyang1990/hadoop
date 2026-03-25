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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DFSUtilClient.CorruptedBlocks;
import org.apache.hadoop.hdfs.client.impl.BlockReaderRemote;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.BlockReadStats;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.concurrent.Callable;

/**
 * 纠删码条带化块读取器，用于从单个源DataNode读取一个条带块数据
 * 每个源DataNode对应一个实例，顺序与源列表保持一致，仅按需分配最少数量的实例
 * 当源DataNode不可用或数据损坏时，会将对应块读取器置空，不再从该源读取
 */
@InterfaceAudience.Private
class StripedBlockReader {
  private static final Logger LOG = DataNode.LOG;

  private StripedReader stripedReader;
  private final DataNode datanode;
  private final Configuration conf;

  private final short index; // 条带块内部索引
  private final ExtendedBlock block;
  private final DatanodeInfo source;
  private BlockReader blockReader;
  private ByteBuffer buffer;
  private boolean isLocal;

  /**
   * 构造条带块读取器，尝试创建块读取连接
   * @param stripedReader 父条带读取器
   * @param datanode 当前DataNode实例
   * @param conf Hadoop配置
   * @param index 条带块索引
   * @param block 要读取的扩展块
   * @param source 源DataNode信息
   * @param offsetInBlock 块内读取起始偏移
   */
  StripedBlockReader(StripedReader stripedReader, DataNode datanode,
                     Configuration conf, short index, ExtendedBlock block,
                     DatanodeInfo source, long offsetInBlock) {
    this.stripedReader = stripedReader;
    this.datanode = datanode;
    this.conf = conf;

    this.index = index;
    this.source = source;
    this.block = block;
    this.isLocal = false;

    BlockReader tmpBlockReader = createBlockReader(offsetInBlock);
    if (tmpBlockReader != null) {
      this.blockReader = tmpBlockReader;
    }
  }

  /**
   * 获取读取缓冲区，延迟分配缓冲区空间
   * @return 读取缓冲区
   */
  ByteBuffer getReadBuffer() {
    if (buffer == null) {
      this.buffer = stripedReader.allocateReadBuffer();
    }
    return buffer;
  }

  /**
   * 释放读取缓冲区
   */
  void freeReadBuffer() {
    DataNodeFaultInjector.get().interceptFreeBlockReaderBuffer();
    buffer = null;
  }

  /**
   * 重置块读取器，从指定偏移重新创建读取连接
   * @param offsetInBlock 块内新的读取起始偏移
   */
  void resetBlockReader(long offsetInBlock) {
    this.blockReader = createBlockReader(offsetInBlock);
  }

  /**
   * 创建远程块读取器，建立与源DataNode的连接
   * @param offsetInBlock 块内读取起始偏移
   * @return 创建好的块读取器，创建失败返回null
   */
  private BlockReader createBlockReader(long offsetInBlock) {
    // 偏移超出块大小，无需读取
    if (offsetInBlock >= block.getNumBytes()) {
      return null;
    }
    Peer peer = null;
    try {
      // 获取源DataNode数据传输地址
      InetSocketAddress dnAddr =
          stripedReader.getSocketAddress4Transfer(source);
      // 获取块读取访问令牌
      Token<BlockTokenIdentifier> blockToken = datanode.getBlockAccessToken(
          block, EnumSet.of(BlockTokenIdentifier.AccessMode.READ),
          StorageType.EMPTY_ARRAY, new String[0]);
        /*
         * 对于本地副本可进一步优化直接读取，需要检查副本是否为FINALIZED状态
         * 不使用短路本地读取，避免依赖域套接字或Windows特定配置
         * TODO: 添加追踪支持
         */
      // 创建与源DataNode的已连接对端
      peer = newConnectedPeer(block, dnAddr, blockToken, source);
      // 标记是否为本地对端
      if (peer.isLocal()) {
        this.isLocal = true;
      }
      // 创建远程块读取器并返回
      return BlockReaderRemote.newBlockReader(
          "dummy", block, blockToken, offsetInBlock,
          block.getNumBytes() - offsetInBlock, true, "", peer, source,
          null, stripedReader.getCachingStrategy(), -1, conf);
    } catch (IOException e) {
      LOG.info("Exception while creating remote block reader for {}, datanode {}",
          block, source, e);
      IOUtils.closeStream(peer);
      return null;
    }
  }

  /**
   * 创建并连接到指定DataNode的对端，处理加密认证
   * @param b 要读取的块
   * @param addr 目标DataNode地址
   * @param blockToken 块访问令牌
   * @param datanodeId 目标DataNodeID
   * @return 已连接的对端对象
   * @throws IOException 连接或认证失败抛出异常
   */
  private Peer newConnectedPeer(ExtendedBlock b, InetSocketAddress addr,
                                Token<BlockTokenIdentifier> blockToken,
                                DatanodeID datanodeId)
      throws IOException {
    Peer peer = null;
    boolean success = false;
    Socket sock = null;
    final int socketTimeout = datanode.getDnConf().getSocketTimeout();
    try {
      // 创建套接字并连接到目标DataNode
      sock = NetUtils.getDefaultSocketFactory(conf).createSocket();
      NetUtils.connect(sock, addr, socketTimeout);
      // 通过密钥和SASL创建对端，处理加密和认证
      peer = DFSUtilClient.peerFromSocketAndKey(datanode.getSaslClient(),
          sock, datanode.getDataEncryptionKeyFactoryForBlock(b),
          blockToken, datanodeId, socketTimeout);
      success = true;
      return peer;
    } finally {
      // 连接失败清理资源
      if (!success) {
        IOUtils.cleanupWithLogger(null, peer);
        IOUtils.closeSocket(sock);
      }
    }
  }

  /**
   * 创建异步读取任务，用于并发从该块读取指定长度数据
   * @param length 要读取的数据长度
   * @param corruptedBlocks 损坏块记录容器
   * @return 异步读取任务，返回读取统计信息
   */
  Callable<BlockReadStats> readFromBlock(final int length,
                               final CorruptedBlocks corruptedBlocks) {
    return new Callable<BlockReadStats>() {

      @Override
      public BlockReadStats call() throws Exception {
        try {
          // 设置缓冲区读取长度限制
          getReadBuffer().limit(length);
          // 执行实际读取
          return actualReadFromBlock();
        } catch (ChecksumException e) {
          // 校验和错误，记录损坏块并抛出异常
          LOG.warn("Found Checksum error for {} from {} at {}", block,
              source, e.getPos());
          corruptedBlocks.addCorruptedBlock(block, source);
          throw e;
        } catch (IOException e) {
          // IO错误，记录日志并抛出
          LOG.info(e.getMessage());
          throw e;
        } finally {
          // 故障注入点
          DataNodeFaultInjector.get().interceptBlockReader();
        }
      }
    };
  }

  /**
   * 实际执行从块读取数据到缓冲区的操作
   * @return 读取统计信息，包含读取字节数、读取方式、网络距离等
   * @throws IOException 读取失败抛出异常
   */
  private BlockReadStats actualReadFromBlock() throws IOException {
    // 故障注入：延迟读取
    DataNodeFaultInjector.get().delayBlockReader();
    int len = buffer.remaining();
    int n = 0;
    // 循环读取直到填满缓冲区或到达流末尾
    while (n < len) {
      int nread = blockReader.read(buffer);
      if (nread <= 0) {
        break;
      }
      n += nread;
      // 累加到重建器的总读取字节统计
      stripedReader.getReconstructor().incrBytesRead(isLocal, nread);
    }
    return new BlockReadStats(n, blockReader.isShortCircuit(),
        blockReader.getNetworkDistance());
  }

  /**
   * 关闭块读取器并释放资源
   */
  void closeBlockReader() {
    IOUtils.closeStream(blockReader);
    blockReader = null;
  }

  /**
   * 获取该条带块的索引
   * @return 条带块索引
   */
  short getIndex() {
    return index;
  }

  /**
   * 获取块读取器实例
   * @return 块读取器，损坏或创建失败返回null
   */
  BlockReader getBlockReader() {
    return blockReader;
  }
}