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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.InputStream;

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
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.UncompressedSplitLineReader;
import org.apache.hadoop.util.functional.FutureIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_SPLIT_END;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_SPLIT_START;

/**
 * 文件按行读取的RecordReader实现，以行偏移量为键，行内容为值
 * 用于MapReduce任务按行切割输入文件分片
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Unstable
public class LineRecordReader implements RecordReader<LongWritable, Text> {
  private static final Logger LOG =
      LoggerFactory.getLogger(LineRecordReader.class.getName());

  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private SplitLineReader in;
  private FSDataInputStream fileIn;
  private final Seekable filePosition;
  int maxLineLength;
  private CompressionCodec codec;
  private Decompressor decompressor;

  /**
   * 已废弃的行读取器类，推荐使用{@link org.apache.hadoop.util.LineReader}
   */
  @Deprecated
  public static class LineReader extends org.apache.hadoop.util.LineReader {
    /**
     * 构造方法，委托父类实现
     * @param in 输入流
     */
    LineReader(InputStream in) {
      super(in);
    }

    /**
     * 构造方法，委托父类实现
     * @param in 输入流
     * @param bufferSize 缓冲区大小
     */
    LineReader(InputStream in, int bufferSize) {
      super(in, bufferSize);
    }

    /**
     * 构造方法，委托父类实现
     * @param in 输入流
     * @param conf 配置对象
     * @throws IOException 读取异常
     */
    public LineReader(InputStream in, Configuration conf) throws IOException {
      super(in, conf);
    }

    /**
     * 构造方法，委托父类实现，支持自定义行分隔符
     * @param in 输入流
     * @param recordDelimiter 行分隔符字节数组
     */
    LineReader(InputStream in, byte[] recordDelimiter) {
      super(in, recordDelimiter);
    }

    /**
     * 构造方法，委托父类实现，支持自定义行分隔符和缓冲区大小
     * @param in 输入流
     * @param bufferSize 缓冲区大小
     * @param recordDelimiter 行分隔符字节数组
     */
    LineReader(InputStream in, int bufferSize, byte[] recordDelimiter) {
      super(in, bufferSize, recordDelimiter);
    }

    /**
     * 构造方法，委托父类实现，支持自定义行分隔符
     * @param in 输入流
     * @param conf 配置对象
     * @param recordDelimiter 行分隔符字节数组
     * @throws IOException 读取异常
     */
    public LineReader(InputStream in, Configuration conf,
        byte[] recordDelimiter) throws IOException {
      super(in, conf, recordDelimiter);
    }
  }

  /**
   * 构造方法，使用默认行分隔符
   * @param job 作业配置
   * @param split 文件分片
   * @throws IOException 初始化异常
   */
  public LineRecordReader(Configuration job, 
                          FileSplit split) throws IOException {
    this(job, split, null);
  }

  /**
   * 构造方法，支持自定义行分隔符，初始化行读取器
   * @param job 作业配置
   * @param split 文件分片
   * @param recordDelimiter 自定义行分隔符，null表示默认换行符
   * @throws IOException 初始化异常
   */
  public LineRecordReader(Configuration job, FileSplit split,
      byte[] recordDelimiter) throws IOException {
    this.maxLineLength = job.getInt(org.apache.hadoop.mapreduce.lib.input.
      LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    codec = compressionCodecs.getCodec(file);

    // 打开文件并定位到分片起始位置
    final FutureDataInputStreamBuilder builder =
        file.getFileSystem(job).openFile(file);
    // 传入分片起止位置给文件系统构建输入策略
    builder.optLong(FS_OPTION_OPENFILE_SPLIT_START, start)
        .optLong(FS_OPTION_OPENFILE_SPLIT_END, end);
    FutureIO.propagateOptions(builder, job,
        MRJobConfig.INPUT_FILE_OPTION_PREFIX,
        MRJobConfig.INPUT_FILE_MANDATORY_PREFIX);
    fileIn = FutureIO.awaitFuture(builder.build());
    if (isCompressedInput()) {
      decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        // 可切分压缩格式，创建分片输入流
        final SplitCompressionInputStream cIn =
          ((SplittableCompressionCodec)codec).createInputStream(
            fileIn, decompressor, start, end,
            SplittableCompressionCodec.READ_MODE.BYBLOCK);
        in = new CompressedSplitLineReader(cIn, job, recordDelimiter);
        start = cIn.getAdjustedStart();
        end = cIn.getAdjustedEnd();
        filePosition = cIn; // 从压缩流获取当前位置
      } else {
        // 不可切分压缩格式，非起始分片直接抛异常
        if (start != 0) {
          throw new IOException("Cannot seek in " +
              codec.getClass().getSimpleName() + " compressed stream");
        }

        in = new SplitLineReader(codec.createInputStream(fileIn,
            decompressor), job, recordDelimiter);
        filePosition = fileIn;
      }
    } else {
      // 未压缩文件，直接定位到分片起始
      fileIn.seek(start);
      in = new UncompressedSplitLineReader(
          fileIn, job, recordDelimiter, split.getLength());
      filePosition = fileIn;
    }
    // 非第一个分片，需要丢弃第一行，因为前一个分片已经读取了跨行的这部分
    if (start != 0) {
      try {
        start += in.readLine(new Text(), 0, maxBytesToConsume(start));
      } catch (Exception e) {
        close();
        throw e;
      }
    }
    this.pos = start;
  }

  /**
   * 构造方法，使用已有输入流构造行读取器
   * @param in 输入流
   * @param offset 分片起始偏移
   * @param endOffset 分片结束偏移
   * @param maxLineLength 最大行长度限制
   */
  public LineRecordReader(InputStream in, long offset, long endOffset,
                          int maxLineLength) {
    this(in, offset, endOffset, maxLineLength, null);
  }

  /**
   * 构造方法，支持自定义分隔符，使用已有输入流构造行读取器
   * @param in 输入流
   * @param offset 分片起始偏移
   * @param endOffset 分片结束偏移
   * @param maxLineLength 最大行长度限制
   * @param recordDelimiter 自定义行分隔符
   */
  public LineRecordReader(InputStream in, long offset, long endOffset,
      int maxLineLength, byte[] recordDelimiter) {
    this.maxLineLength = maxLineLength;
    this.in = new SplitLineReader(in, recordDelimiter);
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;    
    filePosition = null;
  }

  /**
   * 构造方法，从配置读取最大行长度，使用已有输入流构造
   * @param in 输入流
   * @param offset 分片起始偏移
   * @param endOffset 分片结束偏移
   * @param job 作业配置
   * @throws IOException 读取异常
   */
  public LineRecordReader(InputStream in, long offset, long endOffset,
                          Configuration job)
    throws IOException{
    this(in, offset, endOffset, job, null);
  }

  /**
   * 构造方法，支持自定义分隔符，从配置读取最大行长度
   * @param in 输入流
   * @param offset 分片起始偏移
   * @param endOffset 分片结束偏移
   * @param job 作业配置
   * @param recordDelimiter 自定义行分隔符
   * @throws IOException 读取异常
   */
  public LineRecordReader(InputStream in, long offset, long endOffset, 
                          Configuration job, byte[] recordDelimiter)
    throws IOException{
    this.maxLineLength = job.getInt(org.apache.hadoop.mapreduce.lib.input.
      LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
    this.in = new SplitLineReader(in, job, recordDelimiter);
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;    
    filePosition = null;
  }

  /**
   * 创建键对象，键为存储行偏移的LongWritable
   * @return 新的键对象
   */
  public LongWritable createKey() {
    return new LongWritable();
  }
  
  /**
   * 创建值对象，值为存储行内容的Text
   * @return 新的值对象
   */
  public Text createValue() {
    return new Text();
  }
  
  /**
   * 判断当前输入是否为压缩格式
   * @return 是否压缩输入
   */
  private boolean isCompressedInput() {
    return (codec != null);
  }

  /**
   * 计算本次读取最多可消费的字节数
   * @param pos 当前位置
   * @return 最大可消费字节数
   */
  private int maxBytesToConsume(long pos) {
    return isCompressedInput()
      ? Integer.MAX_VALUE
      : (int) Math.max(Math.min(Integer.MAX_VALUE, end - pos), maxLineLength);
  }

  /**
   * 获取当前文件的实际读取位置
   * @return 当前文件位置
   * @throws IOException 获取位置异常
   */
  private long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput() && null != filePosition) {
      retVal = filePosition.getPos();
    } else {
      retVal = pos;
    }
    return retVal;
  }

  /**
   * 跳过文件开头的UTF-8 BOM头
   * @param value 存储读取结果的Text对象
   * @return 实际读取的字节数
   * @throws IOException 读取异常
   */
  private int skipUtfByteOrderMark(Text value) throws IOException {
    // 增加3字节缓冲区空间用于检测BOM
    int newMaxLineLength = (int) Math.min(3L + (long) maxLineLength,
        Integer.MAX_VALUE);
    int newSize = in.readLine(value, newMaxLineLength, maxBytesToConsume(pos));
    pos += newSize;
    int textLength = value.getLength();
    byte[] textBytes = value.getBytes();
    if ((textLength >= 3) && (textBytes[0] == (byte)0xEF) &&
        (textBytes[1] == (byte)0xBB) && (textBytes[2] == (byte)0xBF)) {
      // 检测到UTF-8 BOM，移除该三个字节
      LOG.info("Found UTF-8 BOM and skipped it");
      textLength -= 3;
      newSize -= 3;
      if (textLength > 0) {
        // 移除BOM后重置行内容
        textBytes = value.copyBytes();
        value.set(textBytes, 3, textLength);
      } else {
        value.clear();
      }
    }
    return newSize;
  }

  /**
   * 读取下一行，设置键（偏移量）和值（行内容）
   * @param key 存储行偏移的键对象
   * @param value 存储行内容的值对象
   * @return 是否成功读取到下一行
   * @throws IOException 读取异常
   */
  public synchronized boolean next(LongWritable key, Text value)
    throws IOException {

    // 只要未超过分片结束或者还需要读取额外跨行记录，就继续读取
    while (getFilePosition() <= end || in.needAdditionalRecordAfterSplit()) {
      key.set(pos);

      int newSize = 0;
      if (pos == 0) {
        // 分片起始位置，检查并跳过UTF-8 BOM
        newSize = skipUtfByteOrderMark(value);
      } else {
        // 读取一行
        newSize = in.readLine(value, maxLineLength, maxBytesToConsume(pos));
        pos += newSize;
      }

      // 读取到文件末尾，返回false
      if (newSize == 0) {
        return false;
      }
      // 读取完整行，返回true
      if (newSize < maxLineLength) {
        return true;
      }

      // 行超过最大长度限制，跳过整行继续下一次循环
      LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
    }

    return false;
  }

  /**
   * 获取当前分片读取进度
   * @return 进度百分比[0.0, 1.0]
   * @throws IOException 获取位置异常
   */
  public synchronized float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
    }
  }
  
  /**
   * 获取当前读取位置
   * @return 当前位置偏移
   * @throws IOException 获取位置异常
   */
  public  synchronized long getPos() throws IOException {
    return pos;
  }

  /**
   * 关闭读取器，释放资源
   * @throws IOException 关闭异常
   */
  public synchronized void close() throws IOException {
    try {
      if (in != null) {
        in.close();
      } else if (fileIn != null) {
        fileIn.close();
      }
    } finally {
      // 归还解压缩器到缓存池
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }
    }
  }
}