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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.io.erasurecode.rawcoder.InvalidDecodingException;
import org.apache.hadoop.util.Time;

/**
 * 文件：hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/erasurecode/StripedBlockReconstructor.java
 * <p>
 * 纠删码条带化块重构任务执行器，负责在DataNode上重建丢失损坏的条带块。
 * 当纠删码块组中部分块丢失时，通过已有存活块计算重建出丢失块，并写入目标节点。
 * 要求存活块数量不小于原始数据块数量才能完成重建。
 */
@InterfaceAudience.Private
class StripedBlockReconstructor extends StripedReconstructor
    implements Runnable {

  private StripedWriter stripedWriter;

  /**
   * 构造条带块重构任务实例，初始化写入器。
   * @param worker 纠删码工作线程池实例，提供上下文依赖
   * @param stripedReconInfo 条带重构任务信息，包含块组、目标节点等信息
   */
  StripedBlockReconstructor(ErasureCodingWorker worker,
      StripedReconstructionInfo stripedReconInfo) {
    super(worker, stripedReconInfo);

    stripedWriter = new StripedWriter(this, getDatanode(),
        getConf(), stripedReconInfo);
  }

  /**
   * 检查当前重构任务是否存在有效的目标节点。
   * @return 存在至少一个有效目标返回true，否则false
   */
  boolean hasValidTargets() {
    return stripedWriter.hasValidTargets();
  }

  /**
   * 重构任务主执行方法，由线程池调度执行。
   * 完成初始化、读取源数据、重构计算、写入目标全流程，并统计监控指标。
   */
  @Override
  public void run() {
    try {
      initDecoderIfNecessary();

      initDecodingValidatorIfNecessary();

      getStripedReader().init();

      stripedWriter.init();

      reconstruct();

      stripedWriter.endTargetBlocks();

      // Currently we don't check the acks for packets, this is similar as
      // block replication.
    } catch (Throwable e) {
      LOG.warn("Failed to reconstruct striped block: {}", getBlockGroup(), e);
      getDatanode().getMetrics().incrECFailedReconstructionTasks();
    } finally {
      float xmitWeight = getErasureCodingWorker().getXmitWeight();
      // if the xmits is smaller than 1, the xmitsSubmitted should be set to 1
      // because if it set to zero, we cannot to measure the xmits submitted
      int xmitsSubmitted = Math.max((int) (getXmits() * xmitWeight), 1);
      getDatanode().decrementXmitsInProgress(xmitsSubmitted);
      final DataNodeMetrics metrics = getDatanode().getMetrics();
      metrics.incrECReconstructionTasks();
      metrics.incrECReconstructionBytesRead(getBytesRead());
      metrics.incrECReconstructionRemoteBytesRead(getRemoteBytesRead());
      metrics.incrECReconstructionBytesWritten(getBytesWritten());
      getStripedReader().close();
      stripedWriter.close();
      cleanup();
    }
  }

  /**
   * 分段执行条带块重构，按缓冲区大小循环处理直到完成整个块。
   * 流程分为读取源数据、解码重构、写入目标三个步骤。
   * @throws IOException 重构过程中IO或计算错误抛出异常
   */
  @Override
  void reconstruct() throws IOException {
    while (getPositionInBlock() < getMaxTargetLength()) {
      DataNodeFaultInjector.get().stripedBlockReconstruction();
      // 计算当前批次剩余需要重构的字节数
      long remaining = getMaxTargetLength() - getPositionInBlock();
      final int toReconstructLen =
          (int) Math.min(getStripedReader().getBufferSize(), remaining);

      long start = Time.monotonicNow();
      // 计算本次需要读取的总字节数
      long bytesToRead = (long) toReconstructLen * getStripedReader().getMinRequiredSources();
      // 对重建读取进行流量控制
      if (getDatanode().getEcReconstuctReadThrottler() != null) {
        getDatanode().getEcReconstuctReadThrottler().throttle(bytesToRead);
      }
      // step1: 从重构所需的最少源节点读取对应偏移量的数据
      getStripedReader().readMinimumSources(toReconstructLen);
      long readEnd = Time.monotonicNow();

      // step2: 解码计算出丢失目标块的数据
      reconstructTargets(toReconstructLen);
      long decodeEnd = Time.monotonicNow();

      // step3: 将重构结果传输到目标节点
      // 计算本次需要写入的总字节数
      long bytesToWrite = (long) toReconstructLen * stripedWriter.getTargets();
      // 对重建写入进行流量控制
      if (getDatanode().getEcReconstuctWriteThrottler() != null) {
        getDatanode().getEcReconstuctWriteThrottler().throttle(bytesToWrite);
      }
      // 如果所有目标传输都失败则抛出异常
      if (stripedWriter.transferData2Targets() == 0) {
        String error = "Transfer failed for all targets.";
        throw new IOException(error);
      }
      long writeEnd = Time.monotonicNow();

      // 统计各阶段耗时，只记录成功的重构
      final DataNodeMetrics metrics = getDatanode().getMetrics();
      metrics.incrECReconstructionReadTime(readEnd - start);
      metrics.incrECReconstructionDecodingTime(decodeEnd - readEnd);
      metrics.incrECReconstructionWriteTime(writeEnd - decodeEnd);

      // 更新当前处理位置，准备下一批次
      updatePositionInBlock(toReconstructLen);

      // 清空缓冲区复用
      clearBuffers();
    }
  }

  /**
   * 对当前批次数据执行解码重构，根据配置可选进行解码结果校验。
   * @param toReconstructLen 当前批次需要重构的字节长度
   * @throws IOException 解码错误或校验失败抛出异常
   */
  private void reconstructTargets(int toReconstructLen) throws IOException {
    // 获取读取器准备好的输入缓冲区数组
    ByteBuffer[] inputs = getStripedReader().getInputBuffers(toReconstructLen);

    // 获取需要重构的块索引数组
    int[] erasedIndices = stripedWriter.getRealTargetIndices();
    // 获取写入器准备好的输出缓冲区数组
    ByteBuffer[] outputs = stripedWriter.getRealTargetBuffers(toReconstructLen);

    // 如果开启了解码校验，先解码再验证结果正确性
    if (isValidationEnabled()) {
      markBuffers(inputs);
      decode(inputs, erasedIndices, outputs);
      resetBuffers(inputs);

      // 故障注入：模拟解码错误场景
      DataNodeFaultInjector.get().badDecoding(outputs);
      long start = Time.monotonicNow();
      try {
        // 验证解码结果正确性
        getValidator().validate(inputs, erasedIndices, outputs);
        long validateEnd = Time.monotonicNow();
        getDatanode().getMetrics().incrECReconstructionValidateTime(
            validateEnd - start);
      } catch (InvalidDecodingException e) {
        // 校验失败统计指标并抛出异常
        long validateFailedEnd = Time.monotonicNow();
        getDatanode().getMetrics().incrECReconstructionValidateTime(
            validateFailedEnd - start);
        getDatanode().getMetrics().incrECInvalidReconstructionTasks();
        throw e;
      }
    } else {
      // 不开启校验直接解码
      decode(inputs, erasedIndices, outputs);
    }

    // 更新写入器的目标缓冲区位置，准备传输
    stripedWriter.updateRealTargetBuffers(toReconstructLen);
  }

  /**
   * 调用纠删码解码器执行解码计算，统计解码耗时。
   * @param inputs 输入缓冲区数组，存放存活块的数据
   * @param erasedIndices 需要重建的块索引数组
   * @param outputs 输出缓冲区数组，存放重建出来的丢失块数据
   * @throws IOException 解码过程错误抛出异常
   */
  private void decode(ByteBuffer[] inputs, int[] erasedIndices,
      ByteBuffer[] outputs) throws IOException {
    long start = System.nanoTime();
    getDecoder().decode(inputs, erasedIndices, outputs);
    long end = System.nanoTime();
    this.getDatanode().getMetrics().incrECDecodingTime(end - start);
  }

  /**
   * 清空读取器和写入器的缓冲区，供下一批次使用。
   */
  private void clearBuffers() {
    getStripedReader().clearBuffers();

    stripedWriter.clearBuffers();
  }
}