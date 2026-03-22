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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;

/**
 * 文件级注释：非压缩文本分片行读取器，用于在MapReduce输入分片读取非压缩文本文件的行。
 * 支持多字节分隔符正确分片，处理分片边界被截断的行记录，保证相邻分片不会漏读或多读记录。
 *
 * 非压缩文件分片行读取器实现类，正确处理分片边界的行分隔符，即使分隔符是多字节也能正确分片。
 * 当分片结束在分隔符中间时，会读取额外一条完整记录，避免下一个分片无法识别部分分隔符导致记录错误。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class UncompressedSplitLineReader extends SplitLineReader {
  private boolean needAdditionalRecord = false;
  private long splitLength;
  /** Total bytes read from the input stream. */
  private long totalBytesRead = 0;
  private boolean finished = false;
  private boolean usingCRLF;

  /**
   * 构造非压缩分片行读取器，初始化分片参数和分隔符配置。
   * @param in HDFS文件输入流
   * @param conf 作业配置对象
   * @param recordDelimiterBytes 自定义行分隔符字节数组，null表示使用默认(\n、\r\n、\r)分隔符
   * @param splitLength 分片总字节长度
   * @throws IOException 读取文件时抛出IO异常
   */
  public UncompressedSplitLineReader(FSDataInputStream in, Configuration conf,
      byte[] recordDelimiterBytes, long splitLength) throws IOException {
    super(in, conf, recordDelimiterBytes);
    this.splitLength = splitLength;
    usingCRLF = (recordDelimiterBytes == null);
  }

  /**
   * 向缓冲区填充数据，根据分片长度控制读取字节数，判断是否需要读取额外记录。
   * @param in 输入流
   * @param buffer 存储读取数据的缓冲区
   * @param inDelimiter 当前是否正在读取分隔符过程中（即上次读取结束在分隔符中间）
   * @return 实际读取到的字节数
   * @throws IOException 读取输入流时抛出IO异常
   */
  @Override
  protected int fillBuffer(InputStream in, byte[] buffer, boolean inDelimiter)
      throws IOException {
    int maxBytesToRead = buffer.length;
    // 未读完分片长度，限制本次读取不超过分片剩余长度
    if (totalBytesRead < splitLength) {
      long bytesLeftInSplit = splitLength - totalBytesRead;

      if (bytesLeftInSplit < maxBytesToRead) {
        maxBytesToRead = (int)bytesLeftInSplit;
      }
    }
    int bytesRead = in.read(buffer, 0, maxBytesToRead);

    // If the split ended in the middle of a record delimiter then we need
    // to read one additional record, as the consumer of the next split will
    // not recognize the partial delimiter as a record.
    // However if using the default delimiter and the next character is a
    // linefeed then next split will treat it as a delimiter all by itself
    // and the additional record read should not be performed.
    // 刚好读完分片且结束在分隔符中间，判断是否需要读取额外完整记录
    if (totalBytesRead == splitLength && inDelimiter && bytesRead > 0) {
      if (usingCRLF) {
        // 默认换行模式下，只有首字节不是\n才需要额外读取（如果是\n下一个分片会自己处理）
        needAdditionalRecord = (buffer[0] != '\n');
      } else {
        // 自定义分隔符场景，必须读取额外记录
        needAdditionalRecord = true;
      }
    }
    // 累计已读取字节数
    if (bytesRead > 0) {
      totalBytesRead += bytesRead;
    }
    return bytesRead;
  }

  /**
   * 读取一行数据到Text对象，控制分片读取范围，允许读取分片外的额外一条记录。
   * @param str 存储读取结果的Text对象
   * @param maxLineLength 单行最大字节数限制
   * @param maxBytesToConsume 本次读取最大消耗字节数
   * @return 读取到的字节数，0表示已读到分片末尾
   * @throws IOException 读取输入流时抛出IO异常
   */
  @Override
  public int readLine(Text str, int maxLineLength, int maxBytesToConsume)
      throws IOException {
    int bytesRead = 0;
    if (!finished) {
      // 超过分片长度后，只允许再读取一条记录，之后标记完成
      if (totalBytesRead > splitLength) {
        finished = true;
      }

      bytesRead = super.readLine(str, maxLineLength, maxBytesToConsume);
    }
    return bytesRead;
  }

  /**
   * 判断分片结束后是否需要读取额外一条完整记录。
   * @return true表示需要读取额外记录，false表示不需要
   */
  @Override
  public boolean needAdditionalRecordAfterSplit() {
    return !finished && needAdditionalRecord;
  }

  /**
   * 清除分片后需要额外读取记录的标记。
   */
  @Override
  protected void unsetNeedAdditionalRecordAfterSplit() {
    needAdditionalRecord = false;
  }
}