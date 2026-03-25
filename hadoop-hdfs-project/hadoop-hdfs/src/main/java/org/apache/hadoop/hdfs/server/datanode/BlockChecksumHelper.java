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

import org.apache.hadoop.util.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.BlockChecksumOptions;
import org.apache.hadoop.hdfs.protocol.BlockChecksumType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.StripedBlockInfo;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.erasurecode.StripedBlockChecksumCompositeCrcReconstructor;
import org.apache.hadoop.hdfs.server.datanode.erasurecode.StripedBlockChecksumMd5CrcReconstructor;
import org.apache.hadoop.hdfs.server.datanode.erasurecode.StripedBlockChecksumReconstructor;
import org.apache.hadoop.hdfs.server.datanode.erasurecode.StripedReconstructionInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.CrcComposer;
import org.apache.hadoop.util.CrcUtil;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

/**
 * 文件级注释：HDFS数据节点块校验和计算工具类，支持三副本块和纠删码条带化块的校验和计算
 * 
 * Utilities for Block checksum computing, for both replicated and striped
 * blocks.
 */
@InterfaceAudience.Private
final class BlockChecksumHelper {

  static final Logger LOG = LoggerFactory.getLogger(BlockChecksumHelper.class);

  private BlockChecksumHelper() {
  }

  /**
   * 抽象块校验和计算器基类，定义校验和计算通用属性和接口
   * The abstract block checksum computer.
   */
  static abstract class AbstractBlockChecksumComputer {
    private final DataNode datanode;
    private final BlockChecksumOptions blockChecksumOptions;

    private byte[] outBytes;
    private int bytesPerCRC = -1;
    private DataChecksum.Type crcType = null;
    private long crcPerBlock = -1;
    private int checksumSize = -1;

    AbstractBlockChecksumComputer(
        DataNode datanode,
        BlockChecksumOptions blockChecksumOptions) throws IOException {
      this.datanode = datanode;
      this.blockChecksumOptions = blockChecksumOptions;
    }

    /**
     * 执行校验和计算，子类实现具体逻辑
     * @throws IOException 计算过程IO异常
     */
    abstract void compute() throws IOException;

    Sender createSender(IOStreamPair pair) {
      DataOutputStream out = (DataOutputStream) pair.out;
      return new Sender(out);
    }

    DataNode getDatanode() {
      return datanode;
    }

    BlockChecksumOptions getBlockChecksumOptions() {
      return blockChecksumOptions;
    }

    InputStream getBlockInputStream(ExtendedBlock block, long seekOffset)
        throws IOException {
      return datanode.data.getBlockInputStream(block, seekOffset);
    }

    void setOutBytes(byte[] bytes) {
      this.outBytes = bytes;
    }

    byte[] getOutBytes() {
      return outBytes;
    }

    int getBytesPerCRC() {
      return bytesPerCRC;
    }

    public void setBytesPerCRC(int bytesPerCRC) {
      this.bytesPerCRC = bytesPerCRC;
    }

    public void setCrcType(DataChecksum.Type crcType) {
      this.crcType = crcType;
    }

    public void setCrcPerBlock(long crcPerBlock) {
      this.crcPerBlock = crcPerBlock;
    }

    public void setChecksumSize(int checksumSize) {
      this.checksumSize = checksumSize;
    }

    DataChecksum.Type getCrcType() {
      return crcType;
    }

    long getCrcPerBlock() {
      return crcPerBlock;
    }

    int getChecksumSize() {
      return checksumSize;
    }
  }

  /**
   * 三副本块校验和计算器抽象基类，封装三副本块校验和计算通用逻辑
   * The abstract base block checksum computer, mainly for replicated blocks.
   */
  static abstract class BlockChecksumComputer
      extends AbstractBlockChecksumComputer {
    private final ExtendedBlock block;
    // client side now can specify a range of the block for checksum
    private final long requestLength;
    private final LengthInputStream metadataIn;
    private final DataInputStream checksumIn;
    private final long visibleLength;
    private final boolean partialBlk;

    private BlockMetadataHeader header;
    private DataChecksum checksum;

    BlockChecksumComputer(DataNode datanode,
                          ExtendedBlock block,
                          BlockChecksumOptions blockChecksumOptions)
        throws IOException {
      super(datanode, blockChecksumOptions);
      this.block = block;
      this.requestLength = block.getNumBytes();
      Preconditions.checkArgument(requestLength >= 0);

      // 获取块元数据输入流
      this.metadataIn = datanode.data.getMetaDataInputStream(block);
      // 获取副本可见长度
      this.visibleLength = datanode.data.getReplicaVisibleLength(block);
      // 标记是否为部分块请求
      this.partialBlk = requestLength < visibleLength;

      int ioFileBufferSize =
          DFSUtilClient.getIoFileBufferSize(datanode.getConf());
      this.checksumIn = new DataInputStream(
          new BufferedInputStream(metadataIn, ioFileBufferSize));
    }

    Sender createSender(IOStreamPair pair) {
      DataOutputStream out = (DataOutputStream) pair.out;
      return new Sender(out);
    }


    ExtendedBlock getBlock() {
      return block;
    }

    long getRequestLength() {
      return requestLength;
    }

    LengthInputStream getMetadataIn() {
      return metadataIn;
    }

    DataInputStream getChecksumIn() {
      return checksumIn;
    }

    long getVisibleLength() {
      return visibleLength;
    }

    boolean isPartialBlk() {
      return partialBlk;
    }

    BlockMetadataHeader getHeader() {
      return header;
    }

    DataChecksum getChecksum() {
      return checksum;
    }

    /**
     * Perform the block checksum computing.
     *
     * @throws IOException
     */
    abstract void compute() throws IOException;

    /**
     * 读取块元数据头，提取校验和参数
     * Read block metadata header.
     *
     * @throws IOException
     */
    void readHeader() throws IOException {
      //read metadata file
      header = BlockMetadataHeader.readHeader(checksumIn);
      checksum = header.getChecksum();
      setChecksumSize(checksum.getChecksumSize());
      setBytesPerCRC(checksum.getBytesPerChecksum());
      long crcPerBlock = checksum.getChecksumSize() <= 0 ? 0 :
          (metadataIn.getLength() -
              BlockMetadataHeader.getHeaderSize()) / checksum.getChecksumSize();
      setCrcPerBlock(crcPerBlock);
      setCrcType(checksum.getChecksumType());
    }

    /**
     * 计算部分块的最后一个不完整数据块的校验和
     * Calculate partial block checksum.
     *
     * @return 部分块校验和字节数组，无部分块返回null
     * @throws IOException
     */
    byte[] crcPartialBlock() throws IOException {
      int partialLength = (int) (requestLength % getBytesPerCRC());
      if (partialLength > 0) {
        byte[] buf = new byte[partialLength];
        final InputStream blockIn = getBlockInputStream(block,
            requestLength - partialLength);
        try {
          // 读取不完整块的原始数据，计算校验和
          IOUtils.readFully(blockIn, buf, 0, partialLength);
        } finally {
          IOUtils.closeStream(blockIn);
        }
        checksum.update(buf, 0, partialLength);
        byte[] partialCrc = new byte[getChecksumSize()];
        checksum.writeValue(partialCrc, 0, true);
        return partialCrc;
      }

      return null;
    }
  }

  /**
   * 三副本块校验和计算器实现，处理普通三副本块的校验和计算
   * Replicated block checksum computer.
   */
  static class ReplicatedBlockChecksumComputer extends BlockChecksumComputer {

    ReplicatedBlockChecksumComputer(DataNode datanode,
                                    ExtendedBlock block,
                                    BlockChecksumOptions blockChecksumOptions)
        throws IOException {
      super(datanode, block, blockChecksumOptions);
    }

    @Override
    void compute() throws IOException {
      try {
        readHeader();

        BlockChecksumType type =
            getBlockChecksumOptions().getBlockChecksumType();
        // 根据校验和类型选择计算方式
        switch (type) {
        case MD5CRC:
          computeMd5Crc();
          break;
        case COMPOSITE_CRC:
          computeCompositeCrc(getBlockChecksumOptions().getStripeLength());
          break;
        default:
          throw new IOException(String.format(
              "Unrecognized BlockChecksumType: %s", type));
        }
      } finally {
        // 关闭输入流
        IOUtils.closeStream(getChecksumIn());
        IOUtils.closeStream(getMetadataIn());
      }
    }

    private void computeMd5Crc() throws IOException {
      MD5Hash md5out;
      if (isPartialBlk() && getCrcPerBlock() > 0) {
        md5out = checksumPartialBlock();
      } else {
        md5out = checksumWholeBlock();
      }
      setOutBytes(md5out.getDigest());

      LOG.debug("block={}, bytesPerCRC={}, crcPerBlock={}, md5out={}",
          getBlock(), getBytesPerCRC(), getCrcPerBlock(), md5out);
    }

    private MD5Hash checksumWholeBlock() throws IOException {
      // 直接对整个校验和文件计算MD5
      MD5Hash md5out = MD5Hash.digest(getChecksumIn());
      return md5out;
    }

    private MD5Hash checksumPartialBlock() throws IOException {
      byte[] buffer = new byte[4 * 1024];
      MessageDigest digester = MD5Hash.getDigester();

      // 计算需要读取的完整校验和字节数
      long remaining = (getRequestLength() / getBytesPerCRC())
          * getChecksumSize();
      for (int toDigest = 0; remaining > 0; remaining -= toDigest) {
        toDigest = getChecksumIn().read(buffer, 0,
            (int) Math.min(remaining, buffer.length));
        if (toDigest < 0) {
          break;
        }
        digester.update(buffer, 0, toDigest);
      }

      // 添加不完整块的计算结果
      byte[] partialCrc = crcPartialBlock();
      if (partialCrc != null) {
        digester.update(partialCrc);
      }

      return new MD5Hash(digester.digest());
    }

    private void computeCompositeCrc(long stripeLength) throws IOException {
      long checksumDataLength =
          Math.min(getVisibleLength(), getRequestLength());
      if (stripeLength <= 0 || stripeLength > checksumDataLength) {
        stripeLength = checksumDataLength;
      }

      // 创建条带化CRC合成器
      CrcComposer crcComposer = CrcComposer.newStripedCrcComposer(
          getCrcType(), getBytesPerCRC(), stripeLength);
      DataInputStream checksumIn = getChecksumIn();

      // Whether getting the checksum for the entire block (which itself may
      // not be a full block size and may have a final chunk smaller than
      // getBytesPerCRC()), we begin with a number of full chunks, all of size
      // getBytesPerCRC().
      // 处理所有完整数据块
      long numFullChunks = checksumDataLength / getBytesPerCRC();
      crcComposer.update(checksumIn, numFullChunks, getBytesPerCRC());

      // There may be a final partial chunk that is not full-sized. Unlike the
      // MD5 case, we still consider this a "partial chunk" even if
      // getRequestLength() == getVisibleLength(), since the CRC composition
      // depends on the byte size of that final chunk, even if it already has a
      // precomputed CRC stored in metadata. So there are two cases:
      //   1. Reading only part of a block via getRequestLength(); we get the
      //      crcPartialBlock() explicitly.
      //   2. Reading full visible length; the partial chunk already has a CRC
      //      stored in block metadata, so we just continue reading checksumIn.
      // 处理最后不完整的数据块
      long partialChunkSize = checksumDataLength % getBytesPerCRC();
      if (partialChunkSize > 0) {
        if (isPartialBlk()) {
          // 部分块请求，重新计算不完整块校验和
          byte[] partialChunkCrcBytes = crcPartialBlock();
          crcComposer.update(
              partialChunkCrcBytes, 0, partialChunkCrcBytes.length,
              partialChunkSize);
        } else {
          // 完整块请求，直接读取元数据中已存储的校验和
          int partialChunkCrc = checksumIn.readInt();
          crcComposer.update(partialChunkCrc, partialChunkSize);
        }
      }

      // 获取合成后的校验和结果
      byte[] composedCrcs = crcComposer.digest();
      setOutBytes(composedCrcs);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "block={}, getBytesPerCRC={}, crcPerBlock={}, compositeCrc={}",
            getBlock(), getBytesPerCRC(), getCrcPerBlock(),
            CrcUtil.toMultiCrcString(composedCrcs));
      }
    }
  }

  /**
   * 条带化块组非条带模式校验和计算器，处理纠删码条带化块组的整体校验和计算
   * Non-striped block group checksum computer for striped blocks.
   */
  static class BlockGroupNonStripedChecksumComputer
      extends AbstractBlockChecksumComputer {

    private final ExtendedBlock blockGroup;
    private final ErasureCodingPolicy ecPolicy;
    private final DatanodeInfo[] datanodes;
    private final Token<BlockTokenIdentifier>[] blockTokens;
    private final byte[] blockIndices;
    private final long requestedNumBytes;

    // 存储所有内部块校验和结果的缓冲区
    private final DataOutputBuffer blockChecksumBuf = new DataOutputBuffer();

    // Keeps track of the positions within blockChecksumBuf where each data
    // block's checksum begins; for fixed-size block checksums this is easily
    // calculated as a multiple of the checksum size, but for striped block
    // CRCs, it's less error-prone to simply keep track of exact byte offsets
    // before each block checksum is populated into the buffer.
    // 记录每个数据块校验和在输出缓冲区中的起始偏移
    private final int[] blockChecksumPositions;

    BlockGroupNonStripedChecksumComputer(
        DataNode datanode,
        StripedBlockInfo stripedBlockInfo,
        long requestedNumBytes,
        BlockChecksumOptions blockChecksumOptions)
        throws IOException {
      super(datanode, blockChecksumOptions);
      this.blockGroup = stripedBlockInfo.getBlock