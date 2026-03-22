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

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.erasurecode.rawcoder.DecodingValidator;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.BlockReadStats;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.CompletionService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 条带化块组缺失数据修复基类，负责在HDFS纠删码存储模式下，对条带块组中丢失/损坏的一个或多个块进行重构恢复。
 * 重构流程分三步：分批次从可用数据节点读取所需数据 -> 纠删码解码计算出缺失块 -> 将重构结果发送到目标节点写入。
 * 要求可用存活块数量不少于纠删码策略配置的数据块数量才能完成重构。
 */
@InterfaceAudience.Private
abstract class StripedReconstructor {
  protected static final Logger LOG = DataNode.LOG;

  private final Configuration conf;
  private final DataNode datanode;
  private final ErasureCodingPolicy ecPolicy;
  private final ErasureCoderOptions coderOptions;
  private RawErasureDecoder decoder;
  private final ExtendedBlock blockGroup;
  private static final ByteBufferPool BUFFER_POOL = new ElasticByteBufferPool();

  private final boolean isValidationEnabled;
  private DecodingValidator validator;

  // 当前处理位置在条带内部块中的偏移
  private long positionInBlock;
  private StripedReader stripedReader;
  private ErasureCodingWorker erasureCodingWorker;
  private final CachingStrategy cachingStrategy;
  private long maxTargetLength = 0L;
  private final BitSet liveBitSet;
  private final BitSet excludeBitSet;

  // 重构任务度量指标
  private AtomicLong bytesRead = new AtomicLong(0);
  private AtomicLong bytesWritten = new AtomicLong(0);
  private AtomicLong remoteBytesRead = new AtomicLong(0);

  /**
   * 构造条带化重构器实例，初始化重构所需的基础信息。
   * @param worker 纠删码工作器，所属执行任务的工作器
   * @param stripedReconInfo 重构任务所需信息，包含块组、策略、存活块索引等
   */
  StripedReconstructor(ErasureCodingWorker worker,
      StripedReconstructionInfo stripedReconInfo) {
    this.erasureCodingWorker = worker;
    this.datanode = worker.getDatanode();
    this.conf = worker.getConf();
    this.ecPolicy = stripedReconInfo.getEcPolicy();
    liveBitSet = new BitSet(
        ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits());
    // 标记所有存活块索引
    for (int i = 0; i < stripedReconInfo.getLiveIndices().length; i++) {
      liveBitSet.set(stripedReconInfo.getLiveIndices()[i]);
    }
    excludeBitSet = new BitSet(
            ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits());
    // 标记需要排除、不需要重构的索引
    for (int i = 0; i < stripedReconInfo.getExcludeReconstructedIndices().length; i++) {
      excludeBitSet.set(stripedReconInfo.getExcludeReconstructedIndices()[i]);
    }

    blockGroup = stripedReconInfo.getBlockGroup();
    stripedReader = new StripedReader(this, datanode, conf, stripedReconInfo);
    cachingStrategy = CachingStrategy.newDefaultStrategy();

    positionInBlock = 0L;

    coderOptions = new ErasureCoderOptions(
        ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits());
    // 根据配置初始化解码验证开关，仅在不允许修改输入的编码器下开启
    isValidationEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_VALIDATION_KEY,
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_VALIDATION_VALUE)
        && !coderOptions.allowChangeInputs();
  }

  /**
   * 累加已读取字节数统计，区分本地/远程读取。
   * @param local 是否本地读取
   * @param delta 新增读取字节数
   */
  public void incrBytesRead(boolean local, long delta) {
    if (local) {
      bytesRead.addAndGet(delta);
    } else {
      bytesRead.addAndGet(delta);
      remoteBytesRead.addAndGet(delta);
    }
  }

  /**
   * 累加已写入字节数统计。
   * @param delta 新增写入字节数
   */
  public void incrBytesWritten(long delta) {
    bytesWritten.addAndGet(delta);
  }

  /**
   * 获取总读取字节数。
   * @return 总读取字节数
   */
  public long getBytesRead() {
    return bytesRead.get();
  }

  /**
   * 获取远程读取字节数。
   * @return 远程读取字节数
   */
  public long getRemoteBytesRead() {
    return remoteBytesRead.get();
  }

  /**
   * 获取总写入字节数。
   * @return 总写入字节数
   */
  public long getBytesWritten() {
    return bytesWritten.get();
  }

  /**
   * 执行条带块组缺失数据重构，由子类实现具体重构逻辑。
   * @throws IOException 重构过程IO异常
   */
  abstract void reconstruct() throws IOException;

  /**
   * 判断是否使用直接缓冲区。
   * @return 解码器是否偏好直接缓冲区
   */
  boolean useDirectBuffer() {
    return decoder.preferDirectBuffer();
  }

  /**
   * 从全局缓冲区池分配指定长度的缓冲区。
   * @param length 需要分配的缓冲区长度
   * @return 分配好的缓冲区
   */
  ByteBuffer allocateBuffer(int length) {
    return BUFFER_POOL.getBuffer(useDirectBuffer(), length);
  }

  /**
   * 将使用完毕的缓冲区归还到全局缓冲区池。
   * @param buffer 要释放的缓冲区
   */
  void freeBuffer(ByteBuffer buffer) {
    BUFFER_POOL.putBuffer(buffer);
  }

  /**
   * 根据条带内索引构造内部块对象。
   * @param i 条带内块索引
   * @return 构造好的内部块
   */
  ExtendedBlock getBlock(int i) {
    return StripedBlockUtil.constructInternalBlock(blockGroup, ecPolicy, i);
  }

  /**
   * 获取条带内指定索引块的长度。
   * @param i 条带内块索引
   * @return 块长度
   */
  long getBlockLen(int i) {
    return StripedBlockUtil.getInternalBlockLength(blockGroup.getNumBytes(),
        ecPolicy, i);
  }

  /**
   * 延迟初始化解码器，如果解码器尚未创建则创建。
   */
  // Initialize decoder
  protected void initDecoderIfNecessary() {
    if (decoder == null) {
      decoder = CodecUtil.createRawDecoder(conf, ecPolicy.getCodecName(),
          coderOptions);
    }
  }

  /**
   * 延迟初始化解码验证器，如果需要验证且验证器尚未创建则创建。
   */
  // Initialize decoding validator
  protected void initDecodingValidatorIfNecessary() {
    if (isValidationEnabled && validator == null) {
      validator = new DecodingValidator(decoder);
    }
  }

  /**
   * 获取当前处理位置在块内的偏移。
   * @return 当前偏移量
   */
  long getPositionInBlock() {
    return positionInBlock;
  }

  /**
   * 构造数据传输用的数据节点套接字地址。
   * @param dnInfo 数据节点信息
   * @return 可用于连接传输的套接字地址
   */
  InetSocketAddress getSocketAddress4Transfer(DatanodeInfo dnInfo) {
    return NetUtils.createSocketAddr(dnInfo.getXferAddr(
        datanode.getDnConf().getConnectToDnViaHostname()));
  }

  /**
   * 获取每次重构读取的缓冲区大小。
   * @return 缓冲区大小
   */
  int getBufferSize() {
    return stripedReader.getBufferSize();
  }

  /**
   * 获取块数据校验和信息。
   * @return 校验和对象
   */
  public DataChecksum getChecksum() {
    return stripedReader.getChecksum();
  }

  /**
   * 获取缓存策略。
   * @return 缓存策略
   */
  CachingStrategy getCachingStrategy() {
    return cachingStrategy;
  }

  /**
   * 创建块读取异步完成服务，由纠删码工作器提供。
   * @return 读取完成服务
   */
  CompletionService<BlockReadStats> createReadService() {
    return erasureCodingWorker.createReadService();
  }

  /**
   * 获取当前处理的条带块组。
   * @return 条带块组
   */
  ExtendedBlock getBlockGroup() {
    return blockGroup;
  }

  /**
   * 获取本次重构任务允许的最大读取重试次数。
   * @return 最大重试次数
   */
  int getXmits() {
    return stripedReader.getXmits();
  }

  /**
   * 获取标记所有存活块的BitSet。
   * @return 存活块BitSet
   */
  BitSet getLiveBitSet() {
    return liveBitSet;
  }

  /**
   * 获取标记需要排除重构的块的BitSet。
   * @return 排除块BitSet
   */
  BitSet getExcludeBitSet(){
    return excludeBitSet;
  }

  /**
   * 获取目标重构块的最大长度。
   * @return 最大目标长度
   */
  long getMaxTargetLength() {
    return maxTargetLength;
  }

  /**
   * 设置目标重构块的最大长度。
   * @param maxTargetLength 最大目标长度
   */
  void setMaxTargetLength(long maxTargetLength) {
    this.maxTargetLength = maxTargetLength;
  }

  /**
   * 更新当前处理位置，累加处理过的字节偏移。
   * @param positionInBlockArg 本次处理的字节长度
   */
  void updatePositionInBlock(long positionInBlockArg) {
    this.positionInBlock += positionInBlockArg;
  }

  /**
   * 获取纠删码原始解码器。
   * @return 原始解码器实例
   */
  RawErasureDecoder getDecoder() {
    return decoder;
  }

  /**
   * 获取当前块组存活块数量。
   * @return 存活块数量
   */
  int getNumLiveBlocks(){
    return liveBitSet.cardinality();
  }

  /**
   * 清理重构资源，释放解码器占用资源。
   */
  void cleanup() {
    if (decoder != null) {
      decoder.release();
    }
  }

  /**
   * 获取条带块读取器。
   * @return 条带块读取器实例
   */
  StripedReader getStripedReader() {
    return stripedReader;
  }

  /**
   * 获取Hadoop配置对象。
   * @return 配置对象
   */
  Configuration getConf() {
    return conf;
  }

  /**
   * 获取当前所属DataNode实例。
   * @return DataNode实例
   */
  DataNode getDatanode() {
    return datanode;
  }

  /**
   * 获取当前所属纠删码工作器。
   * @return 纠删码工作器实例
   */
  public ErasureCodingWorker getErasureCodingWorker() {
    return erasureCodingWorker;
  }

  /**
   * 获取全局缓冲区池，仅用于测试。
   * @return 全局缓冲区池实例
   */
  @VisibleForTesting
  static ByteBufferPool getBufferPool() {
    return BUFFER_POOL;
  }

  /**
   * 判断是否启用解码结果验证。
   * @return 是否启用验证
   */
  boolean isValidationEnabled() {
    return isValidationEnabled;
  }

  /**
   * 获取解码验证器实例。
   * @return 解码验证器
   */
  DecodingValidator getValidator() {
    return validator;
  }

  /**
   * 对所有非空缓冲区标记当前位置，用于后续重置。
   * @param buffers 缓冲区数组
   */
  protected static void markBuffers(ByteBuffer[] buffers) {
    for (ByteBuffer buffer: buffers) {
      if (buffer != null) {
        buffer.mark();
      }
    }
  }

  /**
   * 将所有非空缓冲区重置到之前标记的位置。
   * @param buffers 缓冲区数组
   */
  protected static void resetBuffers(ByteBuffer[] buffers) {
    for (ByteBuffer buffer: buffers) {
      if (buffer != null) {
        buffer.reset();
      }
    }
  }
}