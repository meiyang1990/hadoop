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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.CrcComposer;

/**
 * 为纠删码重建后的条带化块，基于已有的块校验和计算组合CRC校验和。
 * 在HDFS纠删码数据重建场景中，用于恢复丢失块的完整校验和信息。
 */
@InterfaceAudience.Private
public class StripedBlockChecksumCompositeCrcReconstructor
    extends StripedBlockChecksumReconstructor {
  // 纠删码策略配置的单元格大小
  private final int ecPolicyCellSize;

  // 最终计算得到的组合CRC校验和字节数组
  private byte[] digestValue;
  // CRC组合计算器，用于分块计算组合校验和
  private CrcComposer digester;

  /**
   * 构造条带化块组合CRC校验和重建器实例。
   * @param worker 纠删码工作线程，提供重建所需上下文资源
   * @param stripedReconInfo 条带化重建信息，包含纠删码策略等配置
   * @param checksumWriter 校验和输出缓冲区，用于写出最终计算结果
   * @param requestedBlockLength 请求重建的目标块长度
   * @throws IOException 初始化失败时抛出IO异常
   */
  public StripedBlockChecksumCompositeCrcReconstructor(
      ErasureCodingWorker worker,
      StripedReconstructionInfo stripedReconInfo,
      DataOutputBuffer checksumWriter,
      long requestedBlockLength) throws IOException {
    super(worker, stripedReconInfo, checksumWriter, requestedBlockLength);
    this.ecPolicyCellSize = stripedReconInfo.getEcPolicy().getCellSize();
  }

  @Override
  public Object getDigestObject() {
    return digestValue;
  }

  @Override
  void prepareDigester() throws IOException {
    // 初始化条带化模式的CRC组合计算器
    digester = CrcComposer.newStripedCrcComposer(
        getChecksum().getChecksumType(),
        getChecksum().getBytesPerChecksum(),
        ecPolicyCellSize);
  }

  @Override
  void updateDigester(byte[] checksumBytes, int dataBytesPerChecksum)
      throws IOException {
    // 检查计算器是否已初始化
    if (digester == null) {
      throw new IOException(String.format(
          "Called updatedDigester with checksumBytes.length=%d, "
          + "dataBytesPerChecksum=%d but digester is null",
          checksumBytes.length, dataBytesPerChecksum));
    }
    // 更新组合CRC计算，添加当前块的校验和数据
    digester.update(
        checksumBytes, 0, checksumBytes.length, dataBytesPerChecksum);
  }

  @Override
  void commitDigest() throws IOException {
    // 检查计算器是否已初始化
    if (digester == null) {
      throw new IOException("Called commitDigest() but digester is null");
    }
    // 完成计算获取最终组合校验和
    digestValue = digester.digest();
    // 将结果写入输出缓冲区
    getChecksumWriter().write(digestValue, 0, digestValue.length);
  }
}