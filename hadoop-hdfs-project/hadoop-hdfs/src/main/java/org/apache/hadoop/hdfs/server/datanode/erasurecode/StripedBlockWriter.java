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
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSPacket;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.EnumSet;

/**
 * 文件：HDFS DataNode 纠删码编码重建模块
 * 功能：条带化块写入器，负责将重建后的纠删码块数据写入到目标DataNode
 * 用于EC块恢复场景，将重建好的单个块写入到丢失块所在的目标节点
 */
@InterfaceAudience.Private
class StripedBlockWriter {
  private final StripedWriter stripedWriter;
  private final DataNode datanode;
  private final Configuration conf;

  private final ExtendedBlock block;
  private final DatanodeInfo target;
  private final StorageType storageType;
  private final String storageId;

  private Socket targetSocket;
  private DataOutputStream targetOutputStream;
  private DataInputStream targetInputStream;
  private ByteBuffer targetBuffer;
  private long blockOffset4Target = 0;
  private long seqNo4Target = 0;
  private static final ByteBufferPool BUFFER_POOL = new ElasticByteBufferPool();

  /**
   * 构造条带化块写入器，初始化缓冲区并完成网络连接建立
   * @param stripedWriter 父级条带化写入器
   * @param datanode 当前本地DataNode实例
   * @param conf Hadoop配置对象
   * @param block 要写入的目标块信息
   * @param target 目标DataNode节点信息
   * @param storageType 目标存储类型
   * @param storageId 目标存储ID
   * @throws IOException 初始化或连接失败时抛出IO异常
   */
  StripedBlockWriter(StripedWriter stripedWriter, DataNode datanode,
                     Configuration conf, ExtendedBlock block,
                     DatanodeInfo target, StorageType storageType,
                     String storageId) throws IOException {
    this.stripedWriter = stripedWriter;
    this.datanode = datanode;
    this.conf = conf;

    this.block = block;
    this.target = target;
    this.storageType = storageType;
    this.storageId = storageId;

    this.targetBuffer = stripedWriter.allocateWriteBuffer();

    init();
  }

  /**
   * 获取当前写入器的目标数据缓冲区
   * @return 存放待写入目标块数据的ByteBuffer
   */
  ByteBuffer getTargetBuffer() {
    return targetBuffer;
  }

  /**
   * 释放目标数据缓冲区引用
   */
  void freeTargetBuffer() {
    targetBuffer = null;
  }

  /**
   * 初始化到目标DataNode的网络连接，创建输入输出流并发送创建块请求
   * 完成SASL认证和块写入协议初始化
   * @throws IOException 网络连接、认证或协议交互失败时抛出IO异常
   */
  private void init() throws IOException {
    Socket socket = null;
    DataOutputStream out = null;
    DataInputStream in = null;
    boolean success = false;
    try {
      // 获取目标节点数据传输地址
      InetSocketAddress targetAddr =
          stripedWriter.getSocketAddress4Transfer(target);
      // 创建新Socket并连接目标节点
      socket = datanode.newSocket();
      NetUtils.connect(socket, targetAddr,
          datanode.getDnConf().getSocketTimeout());
      // 设置TCP参数
      socket.setTcpNoDelay(
          datanode.getDnConf().getDataTransferServerTcpNoDelay());
      socket.setSoTimeout(datanode.getDnConf().getSocketTimeout());

      // 获取块写入权限的BlockToken
      Token<BlockTokenIdentifier> blockToken =
          datanode.getBlockAccessToken(block,
              EnumSet.of(BlockTokenIdentifier.AccessMode.WRITE),
              new StorageType[]{storageType}, new String[]{storageId});

      // 获取基础输入输出流
      long writeTimeout = datanode.getDnConf().getSocketWriteTimeout();
      OutputStream unbufOut = NetUtils.getOutputStream(socket, writeTimeout);
      InputStream unbufIn = NetUtils.getInputStream(socket);
      // 获取数据加密密钥工厂
      DataEncryptionKeyFactory keyFactory =
          datanode.getDataEncryptionKeyFactoryForBlock(block);
      // 完成SASL认证握手，获取加密后的流
      IOStreamPair saslStreams = datanode.getSaslClient().socketSend(
          socket, unbufOut, unbufIn, keyFactory, blockToken, target);

      unbufOut = saslStreams.out;
      unbufIn = saslStreams.in;

      // 包装为缓冲Data流
      out = new DataOutputStream(new BufferedOutputStream(unbufOut,
          DFSUtilClient.getSmallBufferSize(conf)));
      in = new DataInputStream(unbufIn);

      // 构造当前源节点信息
      DatanodeInfo source = new DatanodeInfoBuilder()
          .setNodeID(datanode.getDatanodeId()).build();
      // 发送writeBlock请求，初始化块写入管道
      new Sender(out).writeBlock(block, storageType,
          blockToken, "", new DatanodeInfo[]{target},
          new StorageType[]{storageType}, source,
          BlockConstructionStage.PIPELINE_SETUP_CREATE, 0, 0, 0, 0,
          stripedWriter.getChecksum(), stripedWriter.getCachingStrategy(),
          false, false, null, storageId, new String[]{storageId});

      // 保存连接和流对象到成员变量
      targetSocket = socket;
      targetOutputStream = out;
      targetInputStream = in;
      success = true;
    } finally {
      // 失败时清理资源
      if (!success) {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeStream(socket);
      }
    }
  }

  /**
   * 将缓冲区中已重建的数据打包为数据包发送到目标DataNode
   * 计算数据校验和，按包分片发送
   * @param packetBuf 数据包缓冲区，用于构造DFSPacket
   * @throws IOException 写入或发送失败时抛出IO异常
   */
  void transferData2Target(byte[] packetBuf) throws IOException {
    if (targetBuffer.remaining() == 0) {
      return;
    }

    // 根据缓冲区类型计算校验和
    if (targetBuffer.isDirect()) {
      ByteBuffer directCheckSumBuf =
          BUFFER_POOL.getBuffer(true, stripedWriter.getChecksumBuf().length);
      stripedWriter.getChecksum().calculateChunkedSums(
          targetBuffer, directCheckSumBuf);
      directCheckSumBuf.get(stripedWriter.getChecksumBuf());
      BUFFER_POOL.putBuffer(directCheckSumBuf);
    } else {
      stripedWriter.getChecksum().calculateChunkedSums(
          targetBuffer.array(), 0, targetBuffer.remaining(),
          stripedWriter.getChecksumBuf(), 0);
    }

    int ckOff = 0;
    // 循环分包发送所有剩余数据
    while (targetBuffer.remaining() > 0) {
      // 创建新的数据包
      DFSPacket packet = new DFSPacket(packetBuf,
          stripedWriter.getMaxChunksPerPacket(),
          blockOffset4Target, seqNo4Target++,
          stripedWriter.getChecksumSize(), false);
      // 计算当前包最多可写入字节数
      int maxBytesToPacket = stripedWriter.getMaxChunksPerPacket()
          * stripedWriter.getBytesPerChecksum();
      int toWrite = targetBuffer.remaining() > maxBytesToPacket ?
          maxBytesToPacket : targetBuffer.remaining();
      // 计算当前包校验和长度
      int ckLen = ((toWrite - 1) / stripedWriter.getBytesPerChecksum() + 1)
          * stripedWriter.getChecksumSize();
      // 将校验和写入数据包
      packet.writeChecksum(stripedWriter.getChecksumBuf(), ckOff, ckLen);
      ckOff += ckLen;
      // 将数据写入数据包
      packet.writeData(targetBuffer, toWrite);

      // 发送数据包到目标节点
      packet.writeTo(targetOutputStream);

      // 更新偏移量和已写入字节统计
      blockOffset4Target += toWrite;
      stripedWriter.getReconstructor().incrBytesWritten(toWrite);
    }
  }

  /**
   * 发送空数据包标记块写入结束，刷新输出流
   * @param packetBuf 数据包缓冲区
   * @throws IOException 发送或刷新失败时抛出IO异常
   */
  void endTargetBlock(byte[] packetBuf) throws IOException {
    DFSPacket packet = new DFSPacket(packetBuf, 0,
        blockOffset4Target, seqNo4Target++,
        stripedWriter.getChecksumSize(), true);
    packet.writeTo(targetOutputStream);
    targetOutputStream.flush();
  }

  /**
   * 关闭所有网络连接和流资源，释放资源
   */
  void close() {
    IOUtils.closeStream(targetOutputStream);
    IOUtils.closeStream(targetInputStream);
    IOUtils.closeStream(targetSocket);
  }
}