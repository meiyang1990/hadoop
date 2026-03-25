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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import javax.crypto.SecretKey;
import org.apache.hadoop.fs.FsTracer;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.BlockChecksumOptions;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.StripedBlockInfo;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockPinningException;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.Receiver;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.InvalidMagicNumberException;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientReadStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReadOpChecksumInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReleaseShortCircuitAccessResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitShmResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.BlockChecksumHelper.BlockChecksumComputer;
import org.apache.hadoop.hdfs.server.datanode.BlockChecksumHelper.AbstractBlockChecksumComputer;
import org.apache.hadoop.hdfs.server.datanode.BlockChecksumHelper.ReplicatedBlockChecksumComputer;
import org.apache.hadoop.hdfs.server.datanode.BlockChecksumHelper.BlockGroupNonStripedChecksumComputer;
import org.apache.hadoop.hdfs.server.datanode.DataNode.ShortCircuitFdsUnsupportedException;
import org.apache.hadoop.hdfs.server.datanode.DataNode.ShortCircuitFdsVersionException;
import org.apache.hadoop.hdfs.server.datanode.ShortCircuitRegistry.NewShmInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitFdResponse.DO_NOT_USE_RECEIPT_VERIFICATION;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitFdResponse.USE_RECEIPT_VERIFICATION;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR_ACCESS_TOKEN;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR_INVALID;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR_UNSUPPORTED;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;
import static org.apache.hadoop.util.Time.monotonicNow;


/**
 * 文件说明：DataNode数据传输处理线程，负责处理客户端/其他DataNode发来的数据传输请求
 * 核心职责：处理读写块、拷贝块、替换块、短路读等各类数据传输操作，是DataNode处理数据IO请求的核心线程类
 */
class DataXceiver extends Receiver implements Runnable {
  public static final Logger LOG = DataNode.LOG;
  static final Logger CLIENT_TRACE_LOG = DataNode.CLIENT_TRACE_LOG;
  
  private Peer peer;
  private final String remoteAddress; // 对端地址
  private final String remoteAddressWithoutPort; // 去除端口的对端IP/主机名
  private final String localAddress;  // 本端监听地址
  private final DataNode datanode;
  private final DNConf dnConf;
  private final DataXceiverServer dataXceiverServer;
  private final boolean connectToDnViaHostname;
  private long opStartTime; // 当前操作开始时间
  private final InputStream socketIn;
  private OutputStream socketOut;
  private BlockReceiver blockReceiver = null;
  private final int ioFileBufferSize;
  private final int smallBufferSize;
  private Thread xceiver = null;

  /**
   * 上一次操作使用的客户端名称，长连接复用场景下会保存
   */
  private String previousOpClientName;
  
  /**
   * 创建DataXceiver实例工厂方法
   * @param peer 对端连接
   * @param dn 所属DataNode实例
   * @param dataXceiverServer 所属DataXceiverServer服务
   * @return 返回创建好的DataXceiver实例
   * @throws IOException 创建失败抛出IO异常
   */
  public static DataXceiver create(Peer peer, DataNode dn,
      DataXceiverServer dataXceiverServer) throws IOException {
    return new DataXceiver(peer, dn, dataXceiverServer);
  }
  
  /**
   * 私有构造函数，初始化DataXceiver实例
   * @param peer 对端连接
   * @param datanode 所属DataNode实例
   * @param dataXceiverServer 所属DataXceiverServer服务
   * @throws IOException 初始化失败抛出IO异常
   */
  private DataXceiver(Peer peer, DataNode datanode,
      DataXceiverServer dataXceiverServer) throws IOException {
    super(FsTracer.get(null));
    this.peer = peer;
    this.dnConf = datanode.getDnConf();
    this.socketIn = peer.getInputStream();
    this.socketOut = peer.getOutputStream();
    this.datanode = datanode;
    this.dataXceiverServer = dataXceiverServer;
    this.connectToDnViaHostname = datanode.getDnConf().connectToDnViaHostname;
    this.ioFileBufferSize = DFSUtilClient.getIoFileBufferSize(datanode.getConf());
    this.smallBufferSize = DFSUtilClient.getSmallBufferSize(datanode.getConf());
    remoteAddress = peer.getRemoteAddressString();
    final int colonIdx = remoteAddress.indexOf(':');
    remoteAddressWithoutPort =
        (colonIdx < 0) ? remoteAddress : remoteAddress.substring(0, colonIdx);
    localAddress = peer.getLocalAddressString();

    LOG.debug("Number of active connections is: {}",
        datanode.getXceiverCount());
  }

  /**
   * 更新当前处理线程的名称，包含当前处理状态信息，方便调试定位
   * 仅在线程启动后调用，不可在构造函数中调用
   * @param status 当前处理的操作状态描述
   */
  private void updateCurrentThreadName(String status) {
    StringBuilder sb = new StringBuilder();
    sb.append("DataXceiver for client ");
    if (previousOpClientName != null) {
      sb.append(previousOpClientName).append(" at ");
    }
    sb.append(remoteAddress);
    if (status != null) {
      sb.append(" [").append(status).append("]");
    }
    Thread.currentThread().setName(sb.toString());
  }

  /** 获取当前实例所属的DataNode对象 */
  DataNode getDataNode() {return datanode;}
  
  private OutputStream getOutputStream() {
    return socketOut;
  }

  /**
   * 发送OutOfBand带外信号给当前处理的块接收端
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public void sendOOB() throws IOException, InterruptedException {
    BlockReceiver br = getCurrentBlockReceiver();
    if (br == null) {
      return;
    }
    LOG.info("Sending OOB to peer: {}", peer);
    br.sendOOB();
  }

  /**
   * 停止当前写操作，中断处理线程
   */
  public void stopWriter() {
    synchronized(this) {
      if (getCurrentBlockReceiver() == null) {
        return;
      }
      xceiver.interrupt();
    }
    LOG.info("Stopped the writer: {}", peer);
  }

  /**
   * 同步设置当前块接收器，线程安全
   * @param br 块接收器实例
   */
  private synchronized void setCurrentBlockReceiver(BlockReceiver br) {
    blockReceiver = br;
  }

  /**
   * 同步获取当前块接收器，线程安全
   * @return 返回当前块接收器
   */
  private synchronized BlockReceiver getCurrentBlockReceiver() {
    return blockReceiver;
  }
  
  /**
   * DataXceiver线程主入口，循环处理对端发来的数据传输操作请求
   * 支持长连接复用，在keepalive超时前会持续处理新请求
   */
  @Override
  public void run() {
    int opsProcessed = 0;
    Op op = null;
    Op firstOp = null;

    try {
      synchronized(this) {
        xceiver = Thread.currentThread();
      }
      // 将当前连接加入DataXceiverServer的连接管理
      dataXceiverServer.addPeer(peer, Thread.currentThread(), this);
      // 设置socket写超时
      peer.setWriteTimeout(datanode.getDnConf().socketWriteTimeout);
      InputStream input = socketIn;
      try {
        // 完成SASL握手/数据加密协商
        IOStreamPair saslStreams = datanode.saslServer.receive(peer, socketOut,
          socketIn, datanode.getXferAddress().getPort(),
          datanode.getDatanodeId());
        input = new BufferedInputStream(saslStreams.in,
            smallBufferSize);
        socketOut = saslStreams.out;
      } catch (InvalidMagicNumberException imne) {
        // 握手失败，区分加密握手不兼容和SASL握手不兼容两种场景
        if (imne.isHandshake4Encryption()) {
          LOG.info("Failed to read expected encryption handshake from client " +
              "at {}. Perhaps the client " +
              "is running an older version of Hadoop which does not support " +
              "encryption", peer.getRemoteAddressString(), imne);
        } else {
          LOG.info("Failed to read expected SASL data transfer protection " +
              "handshake from client at {}" +
              ". Perhaps the client is running an older version of Hadoop " +
              "which does not support SASL data transfer protection",
              peer.getRemoteAddressString(), imne);
        }
        return;
      }
      
      // 初始化父类Receiver，设置输入流
      super.initialize(new DataInputStream(input));
      
      // 循环处理请求，支持长连接复用，keepalive超时后关闭连接
      do {
        updateCurrentThreadName("Waiting for operation #" + (opsProcessed + 1));

        try {
          // 根据是否是第一个请求设置不同读超时
          if (opsProcessed != 0) {
            assert dnConf.socketKeepaliveTimeout > 0;
            peer.setReadTimeout(dnConf.socketKeepaliveTimeout);
          } else {
            peer.setReadTimeout(dnConf.socketTimeout);
          }
          // 读取操作码
          op = readOp();
        } catch (InterruptedIOException ignored) {
          // 等待下一个请求超时，退出循环关闭连接
          break;
        } catch (EOFException | ClosedChannelException e) {
          // 对端关闭连接，正常退出
          LOG.debug("Cached {} closing after {} ops.  " +
              "This message is usually benign.", peer, opsProcessed);
          break;
        } catch (IOException err) {
          // 其他IO错误，统计错误后抛出
          incrDatanodeNetworkErrors();
          throw err;
        }

        // 恢复正常读超时
        if (opsProcessed != 0) {
          peer.setReadTimeout(dnConf.socketTimeout);
        }

        // 记录操作开始时间
        opStartTime = monotonicNow();
        // 记录第一个操作，用于连接关闭时指标统计
        if (firstOp == null) {
          firstOp = op;
          incrReadWriteOpMetrics(op);
        }
        // 处理当前操作
        processOp(op);
        ++opsProcessed;
      } while ((peer != null) &&
          (!peer.isClosed() && dnConf.socketKeepaliveTimeout > 0));
    } catch (Throwable t) {
      // 异常处理，根据不同错误类型输出不同级别的日志
      String s = datanode.getDisplayName() + ":DataXceiver error processing "
          + ((op == null) ? "unknown" : op.name()) + " operation "
          + " src: " + remoteAddress + " dst: " + localAddress
          + (previousOpClientName != null
              ? " clientName: " + previousOpClientName : "");
      if (op == Op.WRITE_BLOCK && t instanceof ReplicaAlreadyExistsException) {
        // 写块时副本已存在，客户端和副本可能同时写，属于正常场景不输出错误日志
        if (LOG.isTraceEnabled()) {
          LOG.trace(s, t);
        } else {
          LOG.info("{}; {}", s, t.toString());
        }
      } else if (op == Op.READ_BLOCK && t instanceof SocketTimeoutException) {
        // 读块时超时，大概率客户端已经停止读取，属于正常场景
        String s1 =
            "Likely the client has stopped reading, disconnecting it";
        s1 += " (" + s + ")";
        if (LOG.isTraceEnabled()) {
          LOG.trace(s1, t);
        } else {
          LOG.info("{}; {}", s1, t.toString());
        }
      } else if (t instanceof InvalidToken ||
          t.getCause() instanceof InvalidToken) {
        // 无效令牌已经在checkAccess中打过日志，这里只打trace
        LOG.trace(s, t);
      } else {
        // 其他异常打错误日志
        LOG.error(s, t);
      }
    } finally {
      // 清理线程局部状态，释放资源
      collectThreadLocalStates();
      LOG.debug("{}:Number of active connections is: {}",
          datanode.getDisplayName(), datanode.getXceiverCount());
      updateCurrentThreadName("Cleaning up");
      if (peer != null) {
        if (firstOp != null) {
          decrReadWriteOpMetrics(firstOp);
        }
        // 关闭连接
        dataXceiverServer.closePeer(peer);
        IOUtils.closeStream(in);
      }
    }
  }

  /**
   * 收集线程局部状态指标，在线程退出前调用
   */
  private void collectThreadLocalStates() {
    if (datanode.getDnConf().peerStatsEnabled && datanode.getPeerMetrics() != null) {
      datanode.getPeerMetrics().collectThreadLocalStates();
    }
  }

  @Override
  public void requestShortCircuitFds(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> token,
      SlotId slotId, int maxVersion, boolean supportsReceiptVerification)
        throws IOException {
    updateCurrentThreadName("Passing file descriptors for block " + blk);
    Data