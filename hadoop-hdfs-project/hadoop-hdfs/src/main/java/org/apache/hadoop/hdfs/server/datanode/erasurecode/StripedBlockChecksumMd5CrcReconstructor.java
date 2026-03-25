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
import java.security.MessageDigest;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MD5Hash;

/**
 * 条纹块校验和重建过程中，计算所有重建块CRC的MD5摘要，用于校验数据完整性。
 * 在纠删码重建丢失块后，通过MD5-of-CRC方式验证重建结果的正确性。
 */
@InterfaceAudience.Private
public class StripedBlockChecksumMd5CrcReconstructor
    extends StripedBlockChecksumReconstructor {
  private MD5Hash md5;
  private MessageDigest digester;

  /**
   * 构造MD5-of-CRC校验和重建器，初始化父类重建上下文。
   * @param worker 纠删码工作线程，处理重建任务
   * @param stripedReconInfo 条纹重建任务信息，包含块位置、编码参数等
   * @param checksumWriter 输出缓冲区，用于写入最终计算得到的校验和
   * @param requestedBlockLength 请求重建的块长度
   * @throws IOException 初始化异常
   */
  public StripedBlockChecksumMd5CrcReconstructor(ErasureCodingWorker worker,
      StripedReconstructionInfo stripedReconInfo,
      DataOutputBuffer checksumWriter,
      long requestedBlockLength) throws IOException {
    super(worker, stripedReconInfo, checksumWriter, requestedBlockLength);
  }

  /**
   * 获取最终计算得到的MD5摘要对象，用于完整性校验。
   * @return 计算完成的MD5Hash对象
   */
  @Override
  public Object getDigestObject() {
    return md5;
  }

  /**
   * 初始化MD5消息摘要计算器。
   * @throws IOException 初始化异常
   */
  @Override
  void prepareDigester() throws IOException {
    digester = MD5Hash.getDigester();
  }

  /**
   * 更新MD5摘要，加入当前处理的CRC校验和字节数组。
   * @param checksumBytes 待加入的CRC校验和字节数组
   * @param dataBytesPerChecksum 每个校验和对应的数据字节数（本实现未使用该参数）
   * @throws IOException 摘要计算器为空时抛出异常
   */
  @Override
  void updateDigester(byte[] checksumBytes, int dataBytesPerChecksum)
      throws IOException {
    if (digester == null) {
      throw new IOException(String.format(
          "Called updatedDigester with checksumBytes.length=%d, "
          + "dataBytesPerChecksum=%d but digester is null",
          checksumBytes.length, dataBytesPerChecksum));
    }
    digester.update(checksumBytes, 0, checksumBytes.length);
  }

  /**
   * 完成MD5摘要计算，将结果写入输出缓冲区。
   * @throws IOException 摘要计算器为空时抛出异常
   */
  @Override
  void commitDigest() throws IOException {
    if (digester == null) {
      throw new IOException("Called commitDigest() but digester is null");
    }
    byte[] digest = digester.digest();
    md5 = new MD5Hash(digest);
    md5.write(getChecksumWriter());
  }
}