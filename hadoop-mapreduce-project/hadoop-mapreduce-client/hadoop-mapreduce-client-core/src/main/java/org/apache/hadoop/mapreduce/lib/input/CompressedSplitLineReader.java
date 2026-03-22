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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;

/**
 * 针对压缩分片的按行读取器
 * <p>
 * 从压缩分片中读取行记录需要处理特殊边界情况：压缩输入流通常仅在访问分片后的第一个压缩块后，才会更新实际字节位置，
 * 容易导致分片之间丢失或重复记录。该类专门处理不同边界场景，保证数据一致性。
 * </p>
 * <p>
 * 核心解决压缩块结束位置相对于行分隔符的四种场景，处理自定义分隔符和默认CRLF分隔符的特殊情况，避免数据丢失或重复。
 * 假设压缩输入流单次读取不会返回来自多个压缩块的字节，保证该类的缓冲逻辑正常工作。
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CompressedSplitLineReader extends SplitLineReader {

  SplitCompressionInputStream scin;
  private boolean usingCRLF;
  private boolean needAdditionalRecord = false;
  private boolean finished = false;

  /**
   * 构造压缩分片行读取器
   * @param in 分片压缩输入流
   * @param conf Hadoop配置对象
   * @param recordDelimiterBytes 自定义行分隔符字节数组，null表示使用默认CR/LF/CRLF分隔符
   * @throws IOException 初始化时IO异常
   */
  public CompressedSplitLineReader(SplitCompressionInputStream in,
                                   Configuration conf,
                                   byte[] recordDelimiterBytes)
                                       throws IOException {
    super(in, conf, recordDelimiterBytes);
    scin = in;
    usingCRLF = (recordDelimiterBytes == null);
  }

  /**
   * 从输入流填充缓冲区，处理跨压缩块的分隔符边界情况
   * @param in 输入流
   * @param buffer 目标缓冲区
   * @param inDelimiter 当前是否处于分隔符中间
   * @return 读取到的字节数
   * @throws IOException 读取IO异常
   */
  @Override
  protected int fillBuffer(InputStream in, byte[] buffer, boolean inDelimiter)
      throws IOException {
    boolean alreadyReadAfterSplit = didReadAfterSplit();

    int bytesRead = in.read(buffer);

    // 判断本次读取是否刚越过分片结束位置
    boolean justReadAfterSplit = !alreadyReadAfterSplit && didReadAfterSplit();

    // 如果刚越过分片边界且当前正在处理分隔符，并且读取到了数据，需要判断是否要读取额外记录避免数据丢失
    if (justReadAfterSplit && inDelimiter && bytesRead > 0) {
      if (usingCRLF) {
        // 默认分隔符场景：只有首字节不是\n才需要额外读取，否则下一个分片会自己处理
        needAdditionalRecord = (buffer[0] != '\n');
      } else {
        // 自定义分隔符场景：必须读取额外记录，否则会丢失记录
        needAdditionalRecord = true;
      }
    }
    return bytesRead;
  }

  /**
   * 读取一行数据到Text对象，处理分片越界后的额外记录读取限制
   * @param str 存储读取结果的Text对象
   * @param maxLineLength 最大行长度
   * @param maxBytesToConsume 最大消耗字节数
   * @return 读取到的字节数，0表示到达末尾
   * @throws IOException 读取IO异常
   */
  @Override
  public int readLine(Text str, int maxLineLength, int maxBytesToConsume)
      throws IOException {
    int bytesRead = 0;
    if (!finished) {
      // 如果已经越过分片结束位置，最多只能再读取一条记录后就结束
      if (didReadAfterSplit()) {
        finished = true;
      }

      bytesRead = super.readLine(str, maxLineLength, maxBytesToConsume);
    }
    return bytesRead;
  }

  /**
   * 检查是否需要在分片结束后额外读取一条记录
   * @return true表示需要读取额外记录，false不需要
   */
  @Override
  public boolean needAdditionalRecordAfterSplit() {
    return !finished && needAdditionalRecord;
  }

  /**
   * 清除需要额外读取记录的标记
   */
  @Override
  protected void unsetNeedAdditionalRecordAfterSplit() {
    needAdditionalRecord = false;
  }

  /**
   * 检查当前读取位置是否已经越过分片的结束位置
   * @return true表示已越界，false未越界
   * @throws IOException 获取位置时IO异常
   */
  private boolean didReadAfterSplit() throws IOException {
    return scin.getPos() > scin.getAdjustedEnd();
  }
}