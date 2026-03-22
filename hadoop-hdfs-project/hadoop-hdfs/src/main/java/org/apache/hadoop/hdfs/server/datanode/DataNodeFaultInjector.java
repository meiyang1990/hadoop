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

import org.apache.hadoop.classification.InterfaceAudience;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 数据节点故障注入工具，用于DFSClient和DFSOutputStream测试场景。
 * 生产环境中所有方法调用都是空操作，仅在测试时替换实现注入各类故障，验证系统容错能力。
 * 核心职责：为DataNode提供可扩展的故障注入点，支持测试各种异常场景下的系统行为。
 */
@VisibleForTesting
@InterfaceAudience.Private
public class DataNodeFaultInjector {
  private static DataNodeFaultInjector instance = new DataNodeFaultInjector();

  /**
   * 获取故障注入器单例实例。
   * @return 当前故障注入器实例
   */
  public static DataNodeFaultInjector get() {
    return instance;
  }

  /**
   * 设置自定义故障注入器实例，用于测试时替换默认实现。
   * @param injector 自定义故障注入器实例
   */
  public static void set(DataNodeFaultInjector injector) {
    instance = injector;
  }

  /**
   * 获取HDFS块元数据时注入故障。
   */
  public void getHdfsBlocksMetadata() {}

  /**
   * 块刷盘后写入时注入故障。
   * @throws IOException 注入IO异常
   */
  public void writeBlockAfterFlush() throws IOException {}

  /**
   * 发送短路读共享内存响应时注入故障。
   * @throws IOException 注入IO异常
   */
  public void sendShortCircuitShmResponse() throws IOException {}

  /**
   * 控制是否丢弃心跳包，用于测试节点掉线场景。
   * @return true表示丢弃心跳包，false正常发送
   */
  public boolean dropHeartbeatPacket() {
    return false;
  }

  /**
   * 停止向下游数据节点发送数据包，用于测试数据管道故障场景。
   * @param mirrAddr 下游节点地址
   * @throws IOException 注入IO异常
   */
  public void stopSendingPacketDownstream(final String mirrAddr)
      throws IOException {
  }

  /**
   * Used as a hook to intercept the latency of sending packet.
   */
  public void logDelaySendingPacketDownstream(
      final String mirrAddr,
      final long delayMs) throws IOException {
  }

  /**
   * 延迟向上游节点发送ACK响应，用于测试网络延迟场景。
   * @param upstreamAddr 上游节点地址
   * @throws IOException 注入IO异常
   */
  public void delaySendingAckToUpstream(final String upstreamAddr)
      throws IOException {
  }

  /**
   * Used as a hook to delay sending the response of the last packet.
   */
  public void delayAckLastPacket() throws IOException {
  }

  /**
   * Used as a hook to delay writing a packet to disk.
   */
  public void delayWriteToDisk() {}

  /**
   * Used as a hook to delay writing a packet to os cache.
   */
  public void delayWriteToOsCache() {}

  /**
   * Used as a hook to intercept the latency of sending ack.
   */
  public void logDelaySendingAckToUpstream(
      final String upstreamAddr,
      final long delayMs)
      throws IOException {
  }

  /**
   * 注入不向NameNode注册故障，模拟节点启动异常。
   * @throws IOException 注入IO异常
   */
  public void noRegistration() throws IOException { }

  /**
   * 注入镜像节点连接失败故障，用于测试数据管道重建。
   * @throws IOException 注入IO异常
   */
  public void failMirrorConnection() throws IOException { }

  /**
   * 注入数据管道失败故障。
   * @param replicaInfo 管道中的副本信息
   * @param mirrorAddr 镜像节点地址
   * @throws IOException 注入IO异常
   */
  public void failPipeline(ReplicaInPipeline replicaInfo,
      String mirrorAddr) throws IOException { }

  /**
   * 在OfferService启动流程中注入故障。
   * @throws Exception 注入任意异常
   */
  public void startOfferService() throws Exception {}

  /**
   * 在OfferService退出流程中注入故障。
   * @throws Exception 注入任意异常
   */
  public void endOfferService() throws Exception {}

  /**
   * 注入打开文件过多异常，测试文件句柄耗尽场景。
   * @throws FileNotFoundException 注入文件未找到/打开失败异常
   */
  public void throwTooManyOpenFiles() throws FileNotFoundException {
  }

  /**
   * Used as a hook to inject failure in erasure coding reconstruction
   * process.
   */
  public void stripedBlockReconstruction() throws IOException {}

  /**
   * Used as a hook to inject failure in erasure coding checksum reconstruction
   * process.
   */
  public void stripedBlockChecksumReconstruction() throws IOException {}

  /**
   * Used as a hook to inject latency when read block
   * in erasure coding reconstruction process.
   */
  public void delayBlockReader() {}

  /**
   * Used as a hook to inject intercept when free the block reader buffer.
   */
  public void interceptFreeBlockReaderBuffer() {}

  /**
   * Used as a hook to inject intercept When finish reading from block.
   */
  public void interceptBlockReader() {}

  /**
   * Used as a hook to inject intercept when BPOfferService hold lock.
   */
  public void delayWhenOfferServiceHoldLock() {}

  /**
   * Used as a hook to inject intercept when re-register.
   */
  public void blockUtilSendFullBlockReport() {}

  /**
   * Just delay a while.
   */
  public void delay() {}

  /**
   * Used as a hook to inject data pollution
   * into an erasure coding reconstruction.
   */
  public void badDecoding(ByteBuffer[] outputs) {}

  /**
   * 标记指定数据节点为慢节点，用于测试慢节点检测机制。
   * @param dnAddr 数据节点地址
   * @param replies 响应数组
   */
  public void markSlow(String dnAddr, int[] replies) {}

  /**
   * Just delay delete replica a while.
   */
  public void delayDeleteReplica() {}

  /**
   * Just delay run diff record a while.
   */
  public void delayDiffRecord() {}

  /**
   * Just delay getMetaDataInputStream a while.
   */
  public void delayGetMetaDataInputStream() {}

  /**
   * Used in {@link DirectoryScanner#reconcile()} to wait until a storage is removed,
   * leaving a stale copy of {@link DirectoryScanner#diffs}.
   */
  public void waitUntilStorageRemoved() {}
}