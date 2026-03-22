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

import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSPacket;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodePeerMetrics;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.tracing.Span;
import org.apache.hadoop.tracing.Tracer;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;
import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.SYNC_FILE_RANGE_WRITE;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;

/**
 * HDFS DataNode 数据块接收处理器，负责在数据写入 pipeline 中接收数据块，写入本地磁盘，并转发数据给下游 DataNode。
 * 支持流量限流、校验和验证、缓存管理等功能，是数据块写入流程的核心组件。
 */
class BlockReceiver implements Closeable {
  public static final Logger LOG = DataNode.LOG;
  static final Logger CLIENT_TRACE_LOG = DataNode.CLIENT_TRACE_LOG;

  @VisibleForTesting
  static long CACHE_DROP_LAG_BYTES = 8 * 1024 * 1024;
  private final long datanodeSlowLogThresholdMs;
  private DataInputStream in = null; // 输入流，用于读取上游发送的数据
  private DataChecksum clientChecksum; // 客户端使用的校验和算法
  private DataChecksum diskChecksum; // 磁盘存储使用的校验和算法
  
  /**
   * 当客户端使用的校验和多项式与磁盘存储不同时，DataNode 需要重新计算校验和后再写入磁盘。
   */
  private final boolean needsChecksumTranslation;
  private DataOutputStream checksumOut = null; // 本地磁盘校验文件输出流
  private final int bytesPerChecksum;
  private final int checksumSize;
  
  private final PacketReceiver packetReceiver = new PacketReceiver(false);
  
  protected final String inAddr;
  protected final String myAddr;
  private String mirrorAddr;
  private String mirrorNameForMetrics;
  private DataOutputStream mirrorOut;
  private Daemon responder = null;
  private DataTransferThrottler throttler;
  private ReplicaOutputStreams streams;
  private DatanodeInfo srcDataNode = null;
  private DatanodeInfo[] downstreamDNs = DatanodeInfo.EMPTY_ARRAY;
  private final DataNode datanode;
  volatile private boolean mirrorError;

  // 操作系统缓存管理状态
  private boolean dropCacheBehindWrites;
  private long lastCacheManagementOffset = 0;
  private boolean syncBehindWrites;
  private boolean syncBehindWritesInBackground;

  /** 客户端名称，如果发送方是 DataNode 则为空 */
  private final String clientname;
  private final boolean isClient; 
  private final boolean isDatanode;

  /** 本次要接收的数据块 */
  private final ExtendedBlock block; 
  /** 待写入的副本信息 */
  private ReplicaInPipeline replicaInfo;
  /** 数据管道构建阶段 */
  private final BlockConstructionStage stage;
  private final boolean isTransfer;
  private boolean isPenultimateNode = false;

  private boolean syncOnClose;
  private volatile boolean dirSyncOnFinalize;
  private boolean dirSyncOnHSyncDone = false;
  private long restartBudget;
  /** 数据块写入目标卷的引用 */
  private ReplicaHandler replicaHandler;

  /**
   * 用于 replaceBlock 操作的响应间隔
   */
  private final long responseInterval;
  private long lastResponseTime = 0;
  private boolean isReplaceBlock = false;
  private DataOutputStream replyOut = null;
  private long maxWriteToDiskMs = 0;
  
  private boolean pinning;
  private final AtomicLong lastSentTime = new AtomicLong(0L);
  private long maxSendIdleTime;

  /**
   * 构造 BlockReceiver，初始化接收环境，打开本地磁盘输出流。
   * @param block 待接收的数据块
   * @param storageType 存储类型
   * @param in 上游输入流
   * @param inAddr 上游地址
   * @param myAddr 当前节点地址
   * @param stage 管道构建阶段
   * @param newGs 新的生成时间戳
   * @param minBytesRcvd 最小已接收字节数
   * @param maxBytesRcvd 最大已接收字节数
   * @param clientname 客户端名称
   * @param srcDataNode 源 DataNode 信息
   * @param datanode 当前 DataNode 实例
   * @param requestedChecksum 请求的校验和算法
   * @param cachingStrategy 缓存策略
   * @param allowLazyPersist 是否允许延迟持久化
   * @param pinning 是否固定块
   * @param storageId 存储ID
   * @throws IOException 初始化失败时抛出异常
   */
  BlockReceiver(final ExtendedBlock block, final StorageType storageType,
      final DataInputStream in,
      final String inAddr, final String myAddr,
      final BlockConstructionStage stage, 
      final long newGs, final long minBytesRcvd, final long maxBytesRcvd, 
      final String clientname, final DatanodeInfo srcDataNode,
      final DataNode datanode, DataChecksum requestedChecksum,
      CachingStrategy cachingStrategy,
      final boolean allowLazyPersist,
      final boolean pinning,
      final String storageId) throws IOException {
    try{
      this.block = block;
      this.in = in;
      this.inAddr = inAddr;
      this.myAddr = myAddr;
      this.srcDataNode = srcDataNode;
      this.datanode = datanode;

      this.clientname = clientname;
      this.isDatanode = clientname.length() == 0;
      this.isClient = !this.isDatanode;
      this.restartBudget = datanode.getDnConf().restartReplicaExpiry;
      this.datanodeSlowLogThresholdMs =
          datanode.getDnConf().getSlowIoWarningThresholdMs();
      // 对于 replaceBlock() 调用需要定期发送响应避免客户端 socket 超时，因此使用 0.5 * socketTimeout 作为间隔
      final long readTimeout = datanode.getDnConf().socketTimeout;
      this.responseInterval = (long) (readTimeout * 0.5);
      // DataNode 作为发送方满足：clientName 长度为0，stage 为 null 或 PIPELINE_SETUP_CREATE
      this.stage = stage;
      this.isTransfer = stage == BlockConstructionStage.TRANSFER_RBW
          || stage == BlockConstructionStage.TRANSFER_FINALIZED;

      this.pinning = pinning;
      this.lastSentTime.set(Time.monotonicNow());
      // 下游节点会在 readTimeout 后超时，如果无数据流量，每 0.5*readTimeout 发送心跳包，此处设置 0.9*readTimeout 作为拥塞检测阈值
      this.maxSendIdleTime = (long) (readTimeout * 0.9);
      if (LOG.isDebugEnabled()) {
        LOG.debug(getClass().getSimpleName() + ": " + block
            + "\n storageType=" + storageType + ", inAddr=" + inAddr
            + ", myAddr=" + myAddr + "\n stage=" + stage + ", newGs=" + newGs
            + ", minBytesRcvd=" + minBytesRcvd
            + ", maxBytesRcvd=" + maxBytesRcvd + "\n clientname=" + clientname
            + ", srcDataNode=" + srcDataNode
            + ", datanode=" + datanode.getDisplayName()
            + "\n requestedChecksum=" + requestedChecksum
            + "\n cachingStrategy=" + cachingStrategy
            + "\n allowLazyPersist=" + allowLazyPersist + ", pinning=" + pinning
            + ", isClient=" + isClient + ", isDatanode=" + isDatanode
            + ", responseInterval=" + responseInterval
            + ", storageID=" + (storageId != null ? storageId : "null")
        );
      }

      //
      // 打开本地磁盘输出流
      //
      if (isDatanode) { // 复制或移动数据块
        replicaHandler =
            datanode.data.createTemporary(storageType, storageId, block, false);
      } else {
        switch (stage) {
        case PIPELINE_SETUP_CREATE:
          replicaHandler = datanode.data.createRbw(storageType, storageId,
              block, allowLazyPersist, newGs);
          if (newGs != 0L) {
            block.setGenerationStamp(newGs);
          }
          datanode.notifyNamenodeReceivingBlock(
              block, replicaHandler.getReplica().getStorageUuid());
          break;
        case PIPELINE_SETUP_STREAMING_RECOVERY:
          replicaHandler = datanode.data.recoverRbw(
              block, newGs, minBytesRcvd, maxBytesRcvd);
          block.setGenerationStamp(newGs);
          break;
        case PIPELINE_SETUP_APPEND:
          replicaHandler = datanode.data.append(block, newGs, minBytesRcvd);
          block.setGenerationStamp(newGs);
          datanode.notifyNamenodeReceivingBlock(
              block, replicaHandler.getReplica().getStorageUuid());
          break;
        case PIPELINE_SETUP_APPEND_RECOVERY:
          replicaHandler = datanode.data.recoverAppend(block, newGs, minBytesRcvd);
          block.setGenerationStamp(newGs);
          datanode.notifyNamenodeReceivingBlock(
              block, replicaHandler.getReplica().getStorageUuid());
          break;
        case TRANSFER_RBW:
        case TRANSFER_FINALIZED:
          // 当前节点是传输目标节点
          replicaHandler = datanode.data.createTemporary(storageType, storageId,
              block, isTransfer);
          break;
        default: throw new IOException("Unsupported stage " + stage + 
              " while receiving block " + block + " from " + inAddr);
        }
      }
      replicaInfo = replicaHandler.getReplica();
      this.dropCacheBehindWrites = (cachingStrategy.getDropBehind() == null) ?
        datanode.getDnConf().dropCacheBehindWrites :
          cachingStrategy.getDropBehind();
      this.syncBehindWrites = datanode.getDnConf().syncBehindWrites;
      this.syncBehindWritesInBackground = datanode.getDnConf().
          syncBehindWritesInBackground;
      
      final boolean isCreate = isDatanode || isTransfer 
          || stage == BlockConstructionStage.PIPELINE_SETUP_CREATE;
      streams = replicaInfo.createStreams(isCreate, requestedChecksum);
      assert streams != null : "null streams!";

      // 读取校验和元信息
      this.clientChecksum = requestedChecksum;
      this.diskChecksum = streams.getChecksum();
      this.needsChecksumTranslation = !clientChecksum.equals(diskChecksum);
      this.bytesPerChecksum = diskChecksum.getBytesPerChecksum();
      this.checksumSize = diskChecksum.getChecksumSize();

      this.checksumOut = new DataOutputStream(new BufferedOutputStream(
          streams.getChecksumOut(), DFSUtilClient.getSmallBufferSize(
          datanode.getConf())));
      // 如果是创建新副本，写入校验块头信息
      if (isCreate) {
        BlockMetadataHeader.writeHeader(checksumOut, diskChecksum);
      } 
    } catch (ReplicaAlreadyExistsException | ReplicaNotFoundException
        | DiskOutOfSpaceException e) {
      throw e;
    } catch(IOException ioe) {
      if (replicaInfo != null) {
        replicaInfo.releaseAllBytesReserved();
      }
      IOUtils.closeStream(this);
      cleanupBlock();
      
      // 检查是否是磁盘错误
      IOException cause = DatanodeUtil.getCauseIfDiskError(ioe);
      DataNode.LOG
          .warn("IOException in BlockReceiver constructor :" + ioe.getMessage()
          + (cause == null ? "" : ". Cause is "), cause);
      if (cause != null) {
        ioe = cause;
        // 磁盘错误检查已移至 FileIoProvider
      }
      
      throw ioe;
    }
  }

  /**
   * 获取当前所属 DataNode 实例。
   * @return 当前 DataNode 实例
   */
  DataNode getDataNode() {return datanode;}

  /**
   * 获取当前接收的副本对象。
   * @return 副本对象
   */
  Replica getReplica() {
    return replicaInfo;
  }

  /**
   * 释放任何未释放的预留字节空间，在关闭时调用处理泄漏。
   */
  public void releaseAnyRemainingReservedSpace() {
    if (replicaInfo != null) {
      if (replicaInfo.getReplicaInfo().getBytesReserved() > 0) {
        LOG.warn("Block {} has not released the reserved bytes. "
                + "Releasing {} bytes as part of close.", replicaInfo.getBlockId(),
            replicaInfo.getReplicaInfo().getBytesReserved());
        replicaInfo.releaseAllBytesReserved();
      }
    }
  }

  /**
   * 关闭所有打开的文件，释放卷引用，刷新并同步数据到磁盘。
   * @throws IOException 关闭过程中发生IO异常
   */
  @Override
  public void close() throws IOException {
    Span span = Tracer.getCurrentSpan();
    if (span != null) {
      span.addKVAnnotation("maxWriteToDiskMs",
            Long.toString(maxWriteToDiskMs));
    }
    packetReceiver.close();

    IOException ioe = null;
    if (syncOnClose && (streams.getDataOut() != null || checksumOut != null)) {
      datanode.metrics.incrFsyncCount();      
    }
    long flushTotalNanos = 0;
    boolean measuredFlushTime = false;
    // 关闭校验文件
    try {
      if (checksumOut != null) {
        long flushStartNanos = System.nanoTime();
        checksumOut.flush();
        long flushEndNanos = System.nanoTime();
        if (syncOnClose) {
          long fsyncStartNanos = flushEndNanos;
          streams.syncChecksumOut();
          datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
        }
        flushTotalNanos += flushEndNanos - flushStartNanos;
        measuredFlushTime = true;
        checksumOut.close();
        checksumOut = null;
      }
    } catch(IOException e) {
      ioe = e;
    }
    finally {
      IOUtils.closeStream(checksumOut);
    }
    // 关闭数据块文件
    try {
      if (streams.getDataOut() != null) {
        long flushStart