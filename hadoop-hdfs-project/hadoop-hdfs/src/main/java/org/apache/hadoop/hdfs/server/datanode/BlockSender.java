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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FsTracer;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager.LockLevel;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.tracing.TraceScope;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;
import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_SEQUENTIAL;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;

/**
 * 文件说明：DataNode端数据块发送器，负责从本地磁盘读取数据块并发送给客户端或其他DataNode
 * 
 * 数据发送格式如下:
 * <br><b>整体数据格式:</b> <pre>
 *    +--------------------------------------------------+
 *    | 校验头 | 数据包头序列...                            |
 *    +--------------------------------------------------+ 
 * </pre>   
 * <b>校验头格式:</b> <pre>
 *    +--------------------------------------------------+
 *    | 1字节 校验类型 | 4字节 每个校验块对应的数据字节数   |
 *    +--------------------------------------------------+ 
 * </pre>   
 * 发送一个空数据包标记块读取完成，结束数据传输。
 * 
 * 数据包包含包头、校验数据和实际数据，每个数据包携带的数据大小由缓冲区大小决定。
 * <pre>
 *   +-----------------------------------------------------+
 *   | 变长包头，详见 {@link PacketHeader}                  |
 *   +-----------------------------------------------------+
 *   | x字节校验数据，x计算方式见下文                       |
 *   +-----------------------------------------------------+
 *   | 实际数据 ......                                     |
 *   +-----------------------------------------------------+
 * 
 * 数据被划分为多个数据块（Chunk），每个Chunk长度不超过BYTES_PER_CHECKSUM，每个Chunk独立计算校验和。
 *  
 *   x = (数据长度 + BYTES_PER_CHECKSUM - 1)/BYTES_PER_CHECKSUM * CHECKSUM_SIZE
 *  
 *   CHECKSUM_SIZE取决于校验类型（通常CRC32为4字节）
 *  </pre>
 *  
 *  客户端持续读取数据直到收到LastPacketInBlock标记为true或长度为0的数据包。如果没有校验错误，客户端回复DataNode OP_STATUS_CHECKSUM_OK。
 */
/**
 * 从磁盘读取数据块并发送给接收方
 */
class BlockSender implements java.io.Closeable {
  static final Logger LOG = DataNode.LOG;
  static final Logger CLIENT_TRACE_LOG = DataNode.CLIENT_TRACE_LOG;
  private static final boolean is32Bit = 
      System.getProperty("sun.arch.data.model").equals("32");
  /**
   * 启用transferTo时使用的最小缓冲区，64KB兼顾了内存占用和传输效率
   */
  private static final int MIN_BUFFER_WITH_TRANSFERTO = 64*1024;
  private static final int IO_FILE_BUFFER_SIZE;
  static {
    HdfsConfiguration conf = new HdfsConfiguration();
    IO_FILE_BUFFER_SIZE = DFSUtilClient.getIoFileBufferSize(conf);
  }
  private static final int TRANSFERTO_BUFFER_SIZE = Math.max(
      IO_FILE_BUFFER_SIZE, MIN_BUFFER_WITH_TRANSFERTO);
  
  /** 当前要读取发送的数据块 */
  private final ExtendedBlock block;

  /** 数据块和校验文件的输入流与文件描述符封装 */
  private ReplicaInputStreams ris;
  /** 使用transferTo时，记录数据块文件当前读取位置 */
  private long blockInPosition = -1;
  /** 校验工具实例 */
  private final DataChecksum checksum;
  /** 初始读取偏移量 */
  private long initialOffset;
  /** 当前读取位置 */
  private long offset;
  /** 数据块文件读取结束位置 */
  private final long endOffset;
  /** 每个校验块对应的数据字节数 */
  private final int chunkSize;
  /** 每个校验块的校验字节数 */
  private final int checksumSize;
  /** 是否允许校验文件读取失败仍然发送数据 */
  private final boolean corruptChecksumOk;
  /** 当前发送数据包的序列号 */
  private long seqno;
  /** 是否允许使用transferTo零拷贝方式发送数据 */
  private final boolean transferToAllowed;
  /** 是否已经发送完整个请求的字节范围 */
  private boolean sentEntireByteRange;
  /** 是否在读数据时验证校验和 */
  private final boolean verifyChecksum;
  /** 客户端日志打印格式 */
  private final String clientTraceFmt;
  private volatile ChunkChecksum lastChunkChecksum = null;
  private DataNode datanode;

  /** 当前正在读取发送的数据块副本 */
  private final Replica replica;

  // 缓存管理相关字段
  private final long readaheadLength;

  private ReadaheadRequest curReadahead;

  private final boolean alwaysReadahead;
  
  private final boolean dropCacheBehindLargeReads;
  
  private final boolean dropCacheBehindAllReads;
  
  private long lastCacheDropOffset;
  private final FileIoProvider fileIoProvider;
  
  @VisibleForTesting
  static long CACHE_DROP_INTERVAL_BYTES = 1024 * 1024; // 1MB
  
  /**
   * 长读判定阈值，参见{{@link BlockSender#isLongRead()}
   */
  private static final long LONG_READ_THRESHOLD_BYTES = 256 * 1024;

  // 此处的chunk大小决定了读取对齐：即使使用NULL校验类型，我们也总是从校验块边界开始读取
  // 因此如果chunk太大，会导致发送不必要的多余数据，512字节（1个磁盘扇区）带来的额外IO最小
  private static final long CHUNK_SIZE = 512;

  private static final String EIO_ERROR = "Input/output error";
  /**
   * 构造函数
   * 
   * @param block 待读取发送的数据块
   * @param startOffset 读取起始偏移量
   * @param length 需要读取的数据长度
   * @param corruptChecksumOk 允许校验错误仍然发送数据
   * @param verifyChecksum 读取时验证校验和
   * @param sendChecksum 是否向客户端发送校验数据
   * @param datanode 所属DataNode实例
   * @param clientTraceFmt 客户端日志格式字符串
   * @param cachingStrategy 缓存策略配置
   * @throws IOException 初始化失败抛出IO异常
   */
  BlockSender(ExtendedBlock block, long startOffset, long length,
              boolean corruptChecksumOk, boolean verifyChecksum,
              boolean sendChecksum, DataNode datanode, String clientTraceFmt,
              CachingStrategy cachingStrategy)
      throws IOException {
    InputStream blockIn = null;
    DataInputStream checksumIn = null;
    FsVolumeReference volumeRef = null;
    this.fileIoProvider = datanode.getFileIoProvider();
    try {
      this.block = block;
      this.corruptChecksumOk = corruptChecksumOk;
      this.verifyChecksum = verifyChecksum;
      this.clientTraceFmt = clientTraceFmt;

      /*
       * 如果客户端明确要求是否丢缓存，则遵循客户端配置，否则使用DataNode默认配置
       * 默认配置下，仅对长读启用缓存丢弃
       */
      if (cachingStrategy.getDropBehind() == null) {
        this.dropCacheBehindAllReads = false;
        this.dropCacheBehindLargeReads =
            datanode.getDnConf().dropCacheBehindReads;
      } else {
        this.dropCacheBehindAllReads =
            this.dropCacheBehindLargeReads =
                 cachingStrategy.getDropBehind().booleanValue();
      }
      /*
       * 类似地，如果客户端明确要求预读则总是启用，否则使用DataNode默认配置，仅对长读启用预读
       */
      if (cachingStrategy.getReadahead() == null) {
        this.alwaysReadahead = false;
        this.readaheadLength = datanode.getDnConf().readaheadLength;
      } else {
        this.alwaysReadahead = true;
        this.readaheadLength = cachingStrategy.getReadahead().longValue();
      }
      this.datanode = datanode;
      
      if (verifyChecksum) {
        // 简化实现，验证校验和必须同时发送校验数据
        Preconditions.checkArgument(sendChecksum,
            "If verifying checksum, currently must also send it.");
      }

      // 如果BlockSender构造完成后立刻有追加写入，最后一个不完整校验块可能会被覆盖
      // BlockSender需要使用追加写入前的不完整校验块
      ChunkChecksum chunkChecksum = null;
      final long replicaVisibleLength;
      try (AutoCloseableLock lock = datanode.getDataSetLockManager().readLock(
          LockLevel.BLOCK_POOl, block.getBlockPoolId())) {
        replica = getReplica(block, datanode);
        replicaVisibleLength = replica.getVisibleLength();
      }
      if (replica.getState() == ReplicaState.RBW) {
        final ReplicaInPipeline rbw = (ReplicaInPipeline) replica;
        rbw.waitForMinLength(startOffset + length, 3, TimeUnit.SECONDS);
        chunkChecksum = rbw.getLastChecksumAndDataLen();
      }
      if (replica instanceof FinalizedReplica) {
        chunkChecksum = getPartialChunkChecksumForFinalized(
            (FinalizedReplica)replica);
      }

      if (replica.getGenerationStamp() < block.getGenerationStamp()) {
        throw new IOException("Replica gen stamp < block genstamp, block="
            + block + ", replica=" + replica);
      } else if (replica.getGenerationStamp() > block.getGenerationStamp()) {
        if (DataNode.LOG.isDebugEnabled()) {
          DataNode.LOG.debug("Bumping up the client provided"
              + " block's genstamp to latest " + replica.getGenerationStamp()
              + " for block " + block);
        }
        block.setGenerationStamp(replica.getGenerationStamp());
      }
      if (replicaVisibleLength < 0) {
        throw new IOException("Replica is not readable, block="
            + block + ", replica=" + replica);
      }
      if (DataNode.LOG.isDebugEnabled()) {
        DataNode.LOG.debug("block=" + block + ", replica=" + replica);
      }

      // 32位平台对>=2GB的块调用transferToFully会失败，因此在这种场景下禁用transferTo
      this.transferToAllowed = datanode.getDnConf().transferToAllowed &&
        (!is32Bit || length <= Integer.MAX_VALUE);

      // 读取数据前先获取卷引用，防止卷被卸载
      FsVolumeSpi volume = datanode.data.getVolume(block);
      if (volume == null) {
        LOG.warn("Cannot find FsVolumeSpi to obtain a reference for block: {}", block);
        throw new ReplicaNotFoundException(block);
      }
      volumeRef = volume.obtainReference();

      /* 
       * (corruptChecksumOK, 元文件存在): 操作说明
       * True,   True: 将验证校验和  
       * True,  False: 不验证，用于需要从损坏文件读取数据的场景
       * False,  True: 将验证校验和
       * False, False: 抛出文件未找到IO异常
       */
      DataChecksum csum = null;
      if (verifyChecksum || sendChecksum) {
        LengthInputStream metaIn = null;
        boolean keepMetaInOpen = false;
        try {
          DataNodeFaultInjector.get().throwTooManyOpenFiles();
          metaIn = datanode.data.getMetaDataInputStream(block);
          if (!corruptChecksumOk || metaIn != null) {
            if (metaIn == null) {
              //需要校验但元文件不存在
              throw new FileNotFoundException("Meta-data not found for " +
                  block);
            }

            // 如果使用NULL校验类型或副本存储在临时存储，元文件只会包含头部
            // 另外，如果管道传输只发送了包头就断开，元文件也可能只有头部，数据文件长度为0
            // 临时存储上的副本不执行校验和验证，当延迟持久化将块复制到非临时存储时，头部信息用于确定校验类型
            // 并重新计算校验和
            int expectedHeaderSize = BlockMetadataHeader.getHeaderSize();
            if (!replica.isOnTransientStorage() &&
                metaIn.getLength() >= expectedHeaderSize) {
              checksumIn = new DataInputStream(new BufferedInputStream(
                  metaIn, IO_FILE_BUFFER_SIZE));

              csum = BlockMetadataHeader.readDataChecksum(checksumIn, block);
              keepMetaInOpen = true;
            } else if (!replica.isOnTransientStorage() &&
                metaIn.getLength() < expectedHeaderSize) {
              LOG.warn("The meta file length {} is less than the expected " +
                  "header size {}, indicating the meta file is corrupt",
                  metaIn.getLength(), expectedHeaderSize);
              throw new CorruptMetaHeaderException("The meta file length "+
                  metaIn.getLength()+" is less than the expected length "+
                  expectedHeaderSize);
            }
          } else {
            LOG.warn("Could not find metadata file for " + block);
          }
        } catch (FileNotFoundException e) {
          if ((e.getMessage() != null) && !(e.getMessage()
              .contains("Too many open files"))) {
            datanode.data.invalidateMissingBlock(block.getBlockPoolId(),
                block.getLocalBlock());
          }
          throw e;
        } finally {
          if (!keepMetaInOpen) {
            IOUtils.closeStream(metaIn);
          }
        }
      }
      if (csum == null) {
        csum = DataChecksum.newDataChecksum(DataChecksum.Type.NULL,
            (int)CHUNK_SIZE);
      }

      /*
       * 如果chunkSize过大，说明元文件大部分已经损坏，当前直接将bytesPerchecksum截断为块长度
       */       
      int size = csum.getBytesPerChecksum();
      if (size > 10*1024*1024 && size > replicaVisibleLength) {
        csum = DataChecksum.newDataChecksum(csum.getChecksumType(),
            Math.max((int)replicaVisibleLength, 10*1024*1024));
        size = csum.getBytesPerChecksum();        
      }
      chunkSize = size;
      checksum = csum;
      checksumSize = checksum.getChecksumSize();
      length = length < 0 ? replicaVisibleLength : length;

      // 结束位置要么是磁盘上的最后一个字节，要么是我们有校验和的最后位置
      long end = chunkChecksum != null ? chunkChecksum.getDataLength()
          : replica.getBytesOnDisk();
      if (startOffset < 0 || startOffset > end
          || (length + startOffset) > end) {
        String msg = " Offset " + startOffset + " and length " + length
        + " don't match block " + block + " ( blockLen " + end + " )";
        LOG.warn(datanode.getDNRegistrationForBP(block.getBlockPoolId()) +
            ":sendBlock() : " + msg);
        throw new IOException(msg);
      }