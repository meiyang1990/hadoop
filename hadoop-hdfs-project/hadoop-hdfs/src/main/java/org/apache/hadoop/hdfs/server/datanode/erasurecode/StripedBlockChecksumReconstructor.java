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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.io.DataOutputBuffer;

/**
 * 文件级注释：本文件属于HDFS数据节点纠删码模块，提供条带化块校验和重构的抽象基础实现，
 * 用于在纠删码块组中恢复丢失的条带块并重新计算其校验和，支撑HDFS纠删码数据修复流程。
 *
 * 条带化块校验和重构器，在纠删码条带块组中重构一个或多个丢失的条带块，
 * 要求可用块数量不小于数据块数量，重构完成后重新计算目标块的校验和。
 */
@InterfaceAudience.Private
public abstract class StripedBlockChecksumReconstructor
    extends StripedReconstructor implements Closeable {
  private ByteBuffer targetBuffer;
  private final byte[] targetIndices;

  private byte[] checksumBuf;
  private DataOutputBuffer checksumWriter;
  private long checksumDataLen;
  private long requestedLen;

  /**
   * 构造条带化块校验和重构器，初始化基础依赖和配置
   * @param worker 纠删码工作线程，处理实际重构任务
   * @param stripedReconInfo 条带化重构信息，包含目标块索引等信息
   * @param checksumWriter 校验和输出缓冲区，用于写入重构后的校验和
   * @param requestedBlockLength 请求重构的块长度
   * @throws IOException 初始化失败时抛出IO异常
   */
  protected StripedBlockChecksumReconstructor(ErasureCodingWorker worker,
      StripedReconstructionInfo stripedReconInfo,
      DataOutputBuffer checksumWriter,
      long requestedBlockLength) throws IOException {
    super(worker, stripedReconInfo);
    this.targetIndices = stripedReconInfo.getTargetIndices();
    assert targetIndices != null;
    this.checksumWriter = checksumWriter;
    this.requestedLen = requestedBlockLength;
    init();
  }

  /**
   * 初始化重构所需资源：初始化解码器、验证器、分配缓冲区、计算最大目标长度和校验缓冲区大小
   * @throws IOException 初始化失败时抛出IO异常
   */
  private void init() throws IOException {
    initDecoderIfNecessary();
    initDecodingValidatorIfNecessary();
    getStripedReader().init();
    // 分配缓冲区存储重构出来的块数据
    targetBuffer = allocateBuffer(getBufferSize());
    long maxTargetLen = 0L;
    // 遍历所有目标块，计算最大目标长度
    for (int targetIndex : targetIndices) {
      maxTargetLen = Math.max(maxTargetLen, getBlockLen(targetIndex));
    }
    setMaxTargetLength(maxTargetLen);
    // 计算校验缓冲区大小
    int checksumSize = getChecksum().getChecksumSize();
    int bytesPerChecksum = getChecksum().getBytesPerChecksum();
    int tmpLen = checksumSize * (getBufferSize() / bytesPerChecksum);
    checksumBuf = new byte[tmpLen];
  }

  /**
   * 执行条带块重构和校验和计算的主流程
   * @throws IOException 重构或校验和计算失败时抛出IO异常
   */
  @Override
  public void reconstruct() throws IOException {
    prepareDigester();
    long maxTargetLength = getMaxTargetLength();
    // 分块迭代重构，直到完成所有请求长度
    while (requestedLen > 0 && getPositionInBlock() < maxTargetLength) {
      // 故障注入点，用于测试
      DataNodeFaultInjector.get().stripedBlockChecksumReconstruction();
      long remaining = maxTargetLength - getPositionInBlock();
      // 计算本次需要重构的长度，不超过缓冲区大小和剩余长度
      final int toReconstructLen = (int) Math
          .min(getStripedReader().getBufferSize(), remaining);
      // 步骤1：从最少需要的源DN读取数据，只读取重构需要的数据
      getStripedReader().readMinimumSources(toReconstructLen);

      // 步骤2：解码重构目标块
      reconstructTargets(toReconstructLen);

      // 步骤3：计算重构块的校验和并更新摘要
      checksumDataLen += checksumWithTargetOutput(
          getBufferArray(targetBuffer), toReconstructLen);

      // 更新块内当前位置，减少剩余请求长度，清空缓冲区
      updatePositionInBlock(toReconstructLen);
      requestedLen -= toReconstructLen;
      clearBuffers();
    }

    // 完成所有重构后提交最终摘要
    commitDigest();
  }

  /**
   * 返回完成重构后的摘要对象，用于调试打印
   * @return 适合调试打印的摘要对象
   */
  public abstract Object getDigestObject();

  /**
   * 开始重构前调用，用于初始化摘要处理器
   * @throws IOException 初始化失败时抛出IO异常
   */
  abstract void prepareDigester() throws IOException;

  /**
   * 增量更新摘要，传入重构过程中实时计算得到的分块校验和
   * @param checksumBytes 计算得到的校验和字节数组
   * @param dataBytesPerChecksum 每个校验和对应的数据字节数
   * @throws IOException 更新失败时抛出IO异常
   */
  abstract void updateDigester(byte[] checksumBytes, int dataBytesPerChecksum)
      throws IOException;

  /**
   * 完成所有请求长度的重构后调用，将最终摘要提交到实现类的输出字段
   * @throws IOException 提交失败时抛出IO异常
   */
  abstract void commitDigest() throws IOException;

  protected DataOutputBuffer getChecksumWriter() {
    return checksumWriter;
  }

  /**
   * 对重构得到的目标数据计算校验和，并更新摘要
   * @param outputData 重构得到的目标数据字节数组
   * @param toReconstructLen 本次重构的数据长度
   * @return 计算得到的校验和总长度
   * @throws IOException 计算或更新摘要失败时抛出IO异常
   */
  private long checksumWithTargetOutput(byte[] outputData, int toReconstructLen)
      throws IOException {
    long checksumDataLength = 0;
    // 处理请求长度不超过本次重构长度的情况，即最后一块数据
    if (requestedLen <= toReconstructLen) {
      int remainingLen = Math.toIntExact(requestedLen);
      // 只截取请求长度的数据
      outputData = Arrays.copyOf(outputData, remainingLen);

      int partialLength = remainingLen % getChecksum().getBytesPerChecksum();

      int checksumRemaining = (remainingLen
          / getChecksum().getBytesPerChecksum())
          * getChecksum().getChecksumSize();

      int dataOffset = 0;

      // 情况1：剩余长度可以分成若干完整校验块 + 一个不完整块，先处理完整校验块
      if (checksumRemaining > 0) {
        remainingLen = remainingLen - partialLength;
        checksumBuf = new byte[checksumRemaining];
        // 计算所有完整块的分块校验和
        getChecksum().calculateChunkedSums(outputData, dataOffset,
            remainingLen, checksumBuf, 0);
        updateDigester(checksumBuf, getChecksum().getBytesPerChecksum());
        checksumDataLength = checksumBuf.length;
        dataOffset = remainingLen;
      }

      // 情况2：处理最后剩余的不完整校验块
      if (partialLength > 0) {
        byte[] partialCrc = new byte[getChecksum().getChecksumSize()];
        getChecksum().reset();
        getChecksum().update(outputData, dataOffset, partialLength);
        getChecksum().writeValue(partialCrc, 0, true);
        updateDigester(partialCrc, partialLength);
        checksumDataLength += partialCrc.length;
      }

      clearBuffers();
      // 返回本次计算的校验和长度
      return checksumDataLength;
    }
    // 请求长度超过本次重构长度，直接计算整批数据的分块校验和
    getChecksum().calculateChunkedSums(outputData, 0,
        outputData.length, checksumBuf, 0);

    // 使用计算得到的校验和更新摘要
    updateDigester(checksumBuf, getChecksum().getBytesPerChecksum());
    return checksumBuf.length;
  }

  /**
   * 使用纠删码解码重构目标块
   * @param toReconstructLen 本次需要重构的数据长度
   * @throws IOException 解码或验证失败时抛出IO异常
   */
  private void reconstructTargets(int toReconstructLen) throws IOException {
    // 从读取器获取源数据输入缓冲区
    ByteBuffer[] inputs = getStripedReader().getInputBuffers(toReconstructLen);

    ByteBuffer[] outputs = new ByteBuffer[1];
    targetBuffer.limit(toReconstructLen);
    outputs[0] = targetBuffer;
    // 转换目标块索引格式，适配解码器接口
    int[] tarIndices = new int[targetIndices.length];
    for (int i = 0; i < targetIndices.length; i++) {
      tarIndices[i] = targetIndices[i];
    }

    // 如果启用解码验证，先标记缓冲区位置，解码后重置再验证结果正确性
    if (isValidationEnabled()) {
      markBuffers(inputs);
      getDecoder().decode(inputs, tarIndices, outputs);
      resetBuffers(inputs);

      getValidator().validate(inputs, tarIndices, outputs);
    } else {
      // 不启用验证，直接解码
      getDecoder().decode(inputs, tarIndices, outputs);
    }
  }

  /**
   * 清空所有相关缓冲区，准备下一次迭代
   */
  private void clearBuffers() {
    getStripedReader().clearBuffers();
    targetBuffer.clear();
  }

  public long getChecksumDataLen() {
    return checksumDataLen;
  }

  /**
   * 从ByteBuffer获取对应字节数组，兼容数组-backed和非数组-backed缓冲区
   * @param buffer 输入ByteBuffer
   * @return 缓冲区内容对应的字节数组
   */
  private static byte[] getBufferArray(ByteBuffer buffer) {
    byte[] buff = new byte[buffer.remaining()];
    if (buffer.hasArray()) {
      buff = buffer.array();
    } else {
      buffer.slice().get(buff);
    }
    return buff;
  }

  /**
   * 关闭重构器，释放相关资源
   * @throws IOException 关闭失败时抛出IO异常
   */
  @Override
  public void close() throws IOException {
    getStripedReader().close();
    cleanup();
  }
}