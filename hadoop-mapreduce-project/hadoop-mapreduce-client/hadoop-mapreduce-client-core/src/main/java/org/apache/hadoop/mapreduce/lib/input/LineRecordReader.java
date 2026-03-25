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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.functional.FutureIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_SPLIT_END;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_SPLIT_START;

/**
 * 按行读取文本输入的RecordReader实现，将行偏移量作为Key，行内容作为Value输出给Map任务
 * 适用于普通文本文件的行式读取，支持压缩文件和自定义行分隔符
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
public class LineRecordReader extends RecordReader<LongWritable, Text> {
  private static final Logger LOG =
      LoggerFactory.getLogger(LineRecordReader.class);
  /** 配置项：单行最大长度，超出部分会被跳过 */
  public static final String MAX_LINE_LENGTH = 
    "mapreduce.input.linerecordreader.line.maxlength";

  private long start;
  private long pos;
  private long end;
  private SplitLineReader in;
  private FSDataInputStream fileIn;
  private Seekable filePosition;
  private int maxLineLength;
  private LongWritable key;
  private Text value;
  private boolean isCompressedInput;
  private Decompressor decompressor;
  private byte[] recordDelimiterBytes;

  /**
   * 默认构造器，使用默认换行符作为行分隔符
   */
  public LineRecordReader() {
  }

  /**
   * 带自定义行分隔符的构造器
   * @param recordDelimiter 自定义行分隔符字节数组
   */
  public LineRecordReader(byte[] recordDelimiter) {
    this.recordDelimiterBytes = recordDelimiter;
  }

  /**
   * 初始化RecordReader，打开输入文件并定位到分片起始位置，处理压缩逻辑
   * @param genericSplit 输入分片
   * @param context 任务尝试上下文
   * @throws IOException 初始化过程IO异常
   */
  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();

    // 打开文件构建输入流，传入分片起止位置供存储层优化读取
    final FutureDataInputStreamBuilder builder =
        file.getFileSystem(job).openFile(file);
    // 分片起止位置可用于存储层构建优化的输入策略
    builder.optLong(FS_OPTION_OPENFILE_SPLIT_START, start);
    builder.optLong(FS_OPTION_OPENFILE_SPLIT_END, end);
    FutureIO.propagateOptions(builder, job,
        MRJobConfig.INPUT_FILE_OPTION_PREFIX,
        MRJobConfig.INPUT_FILE_MANDATORY_PREFIX);
    fileIn = FutureIO.awaitFuture(builder.build());

    try {
      // 检测文件是否启用压缩
      CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
      if (null!=codec) {
        isCompressedInput = true;
        decompressor = CodecPool.getDecompressor(codec);
        // 支持分片分割的压缩格式处理
        if (codec instanceof SplittableCompressionCodec) {
          final SplitCompressionInputStream cIn =
                  ((SplittableCompressionCodec)codec).createInputStream(
                          fileIn, decompressor, start, end,
                          SplittableCompressionCodec.READ_MODE.BYBLOCK);
          in = new CompressedSplitLineReader(cIn, job,
                  this.recordDelimiterBytes);
          // 获取压缩流调整后的分片起止位置
          start = cIn.getAdjustedStart();
          end = cIn.getAdjustedEnd();
          filePosition = cIn;
        } else {
          // 不可分割压缩格式，只允许从文件头开始读取整个文件
          if (start != 0) {
            throw new IOException("Cannot seek in " +
                    codec.getClass().getSimpleName() + " compressed stream");
          }

          in = new SplitLineReader(codec.createInputStream(fileIn,
                  decompressor), job, this.recordDelimiterBytes);
          filePosition = fileIn;
        }
      } else {
        // 非压缩文件，直接定位到分片起始位置
        fileIn.seek(start);
        in = new UncompressedSplitLineReader(
                fileIn, job, this.recordDelimiterBytes, split.getLength());
        filePosition = fileIn;
      }
      // 非第一个分片需要跳过第一行，因为第一行已经被前一个分片读取过了
      if (start != 0) {
        start += in.readLine(new Text(), 0, maxBytesToConsume(start));
      }
      this.pos = start;
    } catch (Exception e) {
      fileIn.close();
      throw e;
    }
  }


  /**
   * 计算当前位置本次读取最多可以消费多少字节
   * @param pos 当前读取位置
   * @return 最大可消费字节数
   */
  private int maxBytesToConsume(long pos) {
    return isCompressedInput
      ? Integer.MAX_VALUE
      : (int) Math.max(Math.min(Integer.MAX_VALUE, end - pos), maxLineLength);
  }

  /**
   * 获取当前读取位置在文件中的绝对偏移量
   * @return 文件绝对偏移量
   * @throws IOException 获取位置时IO异常
   */
  private long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput && null != filePosition) {
      retVal = filePosition.getPos();
    } else {
      retVal = pos;
    }
    return retVal;
  }

  /**
   * 跳过文件开头的UTF-8 BOM头（如果存在）
   * @return 本次读取的字节数
   * @throws IOException 读取过程IO异常
   */
  private int skipUtfByteOrderMark() throws IOException {
    // 仅在文件流开头检查UTF-8 BOM(0xEF,0xBB,0xBF)并移除
    int newMaxLineLength = (int) Math.min(3L + (long) maxLineLength,
        Integer.MAX_VALUE);
    int newSize = in.readLine(value, newMaxLineLength, maxBytesToConsume(pos));
    // 即使多读取3字节也不会改变原有行为，不涉及兼容性问题
    pos += newSize;
    int textLength = value.getLength();
    byte[] textBytes = value.getBytes();
    if ((textLength >= 3) && (textBytes[0] == (byte)0xEF) &&
        (textBytes[1] == (byte)0xBB) && (textBytes[2] == (byte)0xBF)) {
      // 找到UTF-8 BOM，移除它
      LOG.info("Found UTF-8 BOM and skipped it");
      textLength -= 3;
      newSize -= 3;
      if (textLength > 0) {
        // 去除BOM后重置value内容
        textBytes = value.copyBytes();
        value.set(textBytes, 3, textLength);
      } else {
        value.clear();
      }
    }
    return newSize;
  }

  /**
   * 读取下一个键值对，即下一行文本
   * @return 是否成功读取到下一行
   * @throws IOException 读取过程IO异常
   */
  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable();
    }
    key.set(pos);
    if (value == null) {
      value = new Text();
    }
    int newSize = 0;
    // 一直读取直到超出分片边界或找到完整行，处理超长行分割场景
    while (getFilePosition() <= end || in.needAdditionalRecordAfterSplit()) {
      if (pos == 0) {
        // 文件开头检查并跳过UTF-8 BOM
        newSize = skipUtfByteOrderMark();
      } else {
        // 读取一行
        newSize = in.readLine(value, maxLineLength, maxBytesToConsume(pos));
        pos += newSize;
      }

      // 读取到末尾或完整行，退出循环
      if ((newSize == 0) || (newSize < maxLineLength)) {
        break;
      }

      // 行超长，跳过当前行继续读取下一行
      LOG.info("Skipped line of size " + newSize + " at pos " + 
               (pos - newSize));
    }
    if (newSize == 0) {
      // 读取结束，清空键值对返回false
      key = null;
      value = null;
      return false;
    } else {
      // 成功读取到一行，返回true
      return true;
    }
  }

  @Override
  /**
   * 获取当前读取到的键（行偏移量）
   * @return 当前行的偏移量LongWritable对象
   */
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  /**
   * 获取当前读取到的值（行内容）
   * @return 当前行的文本Text对象
   */
  public Text getCurrentValue() {
    return value;
  }

  /**
   * 获取当前分片读取进度，范围0.0到1.0
   * @return 读取进度比例
   * @throws IOException 获取进度时IO异常
   */
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
    }
  }
  
  /**
   * 关闭Reader，释放资源，归还解压器到缓存池
   * @throws IOException 关闭过程IO异常
   */
  public synchronized void close() throws IOException {
    try {
      if (in != null) {
        in.close();
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }
    }
  }
}