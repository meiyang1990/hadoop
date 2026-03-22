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
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.functional.FutureIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级输入分片的定长记录读取器，以记录偏移量为键、原始字节记录为值读取输入数据
 * 用于读取每条记录长度固定的二进制输入文件，适配MapReduce分块处理逻辑
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FixedLengthRecordReader
    extends RecordReader<LongWritable, BytesWritable> {
  private static final Logger LOG
      = LoggerFactory.getLogger(FixedLengthRecordReader.class);

  // 单条记录的固定长度（字节数）
  private int recordLength;
  // 分片起始偏移量
  private long start;
  // 当前读取位置偏移量
  private long pos;
  // 分片结束偏移量
  private long end;
  // 分片内剩余待读取记录数
  private long  numRecordsRemainingInSplit;
  // 文件系统输入流
  private FSDataInputStream fileIn;
  // 用于获取文件当前位置的可查找对象（支持压缩输入流）
  private Seekable filePosition;
  // 当前记录键（偏移量）
  private LongWritable key;
  // 当前记录值（字节数据）
  private BytesWritable value;
  // 是否为压缩输入标识
  private boolean isCompressedInput;
  // 解压缩器对象，从压缩编解码器池获取
  private Decompressor decompressor;
  // 实际读取数据的输入流（原始流或解压流）
  private InputStream inputStream;

  /**
   * 构造函数，指定定长记录的长度
   * @param recordLength 单条记录的固定字节长度
   */
  public FixedLengthRecordReader(int recordLength) {
    this.recordLength = recordLength;
  }

  @Override
  /**
   * 初始化定长记录读取器，根据输入分片和任务上下文准备读取
   * @param genericSplit 输入分片
   * @param context 任务尝试上下文
   * @throws IOException 初始化失败时抛出IO异常
   */
  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    final Path file = split.getPath();
    initialize(job, split.getStart(), split.getLength(), file);
  }

  // This is also called from the old FixedLengthRecordReader API implementation
  /**
   * 内部初始化方法，兼容旧API调用，完成输入流打开、偏移调整和记录数计算
   * @param job 作业配置对象
   * @param splitStart 分片起始偏移量
   * @param splitLength 分片长度
   * @param file 输入文件路径
   * @throws IOException 初始化IO失败时抛出异常
   */
  public void initialize(Configuration job, long splitStart, long splitLength,
                         Path file) throws IOException {
    start = splitStart;
    end = start + splitLength;
    // 计算分片起始位置相对于记录边界的偏移
    long partialRecordLength = start % recordLength;
    long numBytesToSkip = 0;
    // 如果分片起始不落在记录边界，需要跳过部分字节对齐到下一个记录起始
    if (partialRecordLength != 0) {
      numBytesToSkip = recordLength - partialRecordLength;
    }

    // 打开输入文件
    final FutureDataInputStreamBuilder builder =
        file.getFileSystem(job).openFile(file);
    FutureIO.propagateOptions(builder, job,
        MRJobConfig.INPUT_FILE_OPTION_PREFIX,
        MRJobConfig.INPUT_FILE_MANDATORY_PREFIX);
    fileIn = FutureIO.awaitFuture(builder.build());

    // 检查文件是否压缩，获取对应压缩编解码器
    CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
    if (null != codec) {
      // 处理压缩输入
      isCompressedInput = true;	
      // 从编解码器池获取解压缩器
      decompressor = CodecPool.getDecompressor(codec);
      CompressionInputStream cIn
          = codec.createInputStream(fileIn, decompressor);
      filePosition = cIn;
      inputStream = cIn;
      // 压缩输入无法提前计算分片内记录数，设置为最大值
      numRecordsRemainingInSplit = Long.MAX_VALUE;
      LOG.info(
          "Compressed input; cannot compute number of records in the split");
    } else {
      // 处理非压缩输入，跳转到分片起始位置
      fileIn.seek(start);
      filePosition = fileIn;
      inputStream = fileIn;
      // 计算分片有效大小，减去需要跳过的字节
      long splitSize = end - start - numBytesToSkip;
      // 向上取整计算分片内总记录数
      numRecordsRemainingInSplit = (splitSize + recordLength - 1)/recordLength;
      if (numRecordsRemainingInSplit < 0) {
        numRecordsRemainingInSplit = 0;
      }
      LOG.info("Expecting " + numRecordsRemainingInSplit
          + " records each with a length of " + recordLength
          + " bytes in the split with an effective size of "
          + splitSize + " bytes");
    }
    // 如果需要跳过字节对齐，执行跳过操作
    if (numBytesToSkip != 0) {
      start += inputStream.skip(numBytesToSkip);
    }
    this.pos = start;
  }

  @Override
  /**
   * 读取下一条定长记录，更新当前键值对
   * @return 是否成功读取到有效记录
   * @throws IOException 读取IO异常或遇到不完整记录时抛出
   */
  public synchronized boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable();
    }
    if (value == null) {
      value = new BytesWritable(new byte[recordLength]);
    }
    boolean dataRead = false;
    value.setSize(recordLength);
    byte[] record = value.getBytes();
    // 还有剩余记录待读取
    if (numRecordsRemainingInSplit > 0) {
      // 设置当前记录的键为当前读取位置偏移量
      key.set(pos);
      int offset = 0;
      int numBytesToRead = recordLength;
      int numBytesRead = 0;
      // 循环读取直到凑齐一条完整记录长度
      while (numBytesToRead > 0) {
        numBytesRead = inputStream.read(record, offset, numBytesToRead);
        if (numBytesRead == -1) {
          // 已到文件末尾，停止读取
          break;
        }
        offset += numBytesRead;
        numBytesToRead -= numBytesRead;
      }
      numBytesRead = recordLength - numBytesToRead;
      // 更新当前读取位置
      pos += numBytesRead;
      // 成功读取到有效数据
      if (numBytesRead > 0) {
        dataRead = true;
        // 读取到完整记录，减少剩余计数
        if (numBytesRead >= recordLength) {
          if (!isCompressedInput) {
            numRecordsRemainingInSplit--;
          }
        } else {
          // 分片末尾读到不完整记录，抛出异常
          throw new IOException("Partial record(length = " + numBytesRead
              + ") found at the end of split.");
        }
      } else {
        // 未读到数据，标记输入结束
        numRecordsRemainingInSplit = 0L;
      }
    }
    return dataRead;
  }

  @Override
  /**
   * 获取当前读取记录的键（即记录偏移量）
   * @return 当前记录的偏移量键
   */
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  /**
   * 获取当前读取记录的值（即定长字节数据）
   * @return 当前记录的字节值
   */
  public BytesWritable getCurrentValue() {
    return value;
  }

  @Override
  /**
   * 获取当前分片的读取进度，用于MapReduce任务进度汇报
   * @return 读取进度比例，范围[0.0, 1.0]
   * @throws IOException 获取文件位置失败时抛出IO异常
   */
  public synchronized float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
    }
  }
  
  @Override
  /**
   * 关闭读取器，释放资源，包括输入流和解压编解码器
   * @throws IOException 关闭IO失败时抛出异常
   */
  public synchronized void close() throws IOException {
    try {
      if (inputStream != null) {
        inputStream.close();
        inputStream = null;
      }
    } finally {
      // 将解压缩器归还到编解码器池复用
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }
    }
  }

  // This is called from the old FixedLengthRecordReader API implementation.
  /**
   * 获取当前读取位置偏移量，兼容旧MapReduce API调用
   * @return 当前读取位置偏移量
   */
  public long getPos() {
    return pos;
  }

  /**
   * 获取文件实际当前位置，处理压缩和非压缩场景差异
   * @return 文件当前实际位置
   * @throws IOException 获取位置失败时抛出IO异常
   */
  private long getFilePosition() throws IOException {
    long retVal;
    // 压缩输入使用解压流的当前位置
    if (isCompressedInput && null != filePosition) {
      retVal = filePosition.getPos();
    } else {
      // 非压缩输入直接使用当前记录偏移
      retVal = pos;
    }
    return retVal;
  }

}