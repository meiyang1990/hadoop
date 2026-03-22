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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IFile 是 MapReduce 中用于存储Map任务中间输出的存储格式，格式为 <key-len, value-len, key, value>。
 * 提供了Writer类写入Map中间输出、Reader类读取该格式文件的能力。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class IFile {
  private static final Logger LOG = LoggerFactory.getLogger(IFile.class);
  public static final int EOF_MARKER = -1; // 文件结束标记
  private static final int ARRAY_MAX_SIZE = Integer.MAX_VALUE - 8;
  
  /**
   * IFile 写入器，用于将Map任务的中间输出写入到IFile格式文件中。
   * @param <K> 键类型
   * @param <V> 值类型
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class Writer<K extends Object, V extends Object> {
    FSDataOutputStream out;
    boolean ownOutputStream = false;
    long start = 0;
    FSDataOutputStream rawOut;
    
    CompressionOutputStream compressedOut;
    Compressor compressor;
    boolean compressOutput = false;
    
    long decompressedBytesWritten = 0;
    long compressedBytesWritten = 0;

    // 统计写入磁盘的记录数
    private long numRecordsWritten = 0;
    private final Counters.Counter writtenRecordsCounter;

    IFileOutputStream checksumOut;

    Class<K> keyClass;
    Class<V> valueClass;
    Serializer<K> keySerializer;
    Serializer<V> valueSerializer;
    
    DataOutputBuffer buffer = new DataOutputBuffer();

    /**
     * 构造IFile写入器，使用指定压缩编码写入键值对。
     * @param conf Hadoop配置
     * @param out 输出流
     * @param keyClass 键类对象
     * @param valueClass 值类对象
     * @param codec 压缩编码，null表示不压缩
     * @param writesCounter 写入记录计数器
     * @throws IOException IO异常
     */
    public Writer(Configuration conf, FSDataOutputStream out,
        Class<K> keyClass, Class<V> valueClass,
        CompressionCodec codec, Counters.Counter writesCounter)
        throws IOException {
      this(conf, out, keyClass, valueClass, codec, writesCounter, false);
    }
    
    /**
     * 仅初始化计数器的构造方法，供子类使用。
     * @param writesCounter 写入记录计数器
     */
    protected Writer(Counters.Counter writesCounter) {
      writtenRecordsCounter = writesCounter;
    }

    /**
     * 构造IFile写入器，支持指定是否持有输出流所有权。
     * @param conf Hadoop配置
     * @param out 输出流
     * @param keyClass 键类对象
     * @param valueClass 值类对象
     * @param codec 压缩编码，null表示不压缩
     * @param writesCounter 写入记录计数器
     * @param ownOutputStream 是否持有输出流所有权，true则关闭时会关闭流
     * @throws IOException IO异常
     */
    public Writer(Configuration conf, FSDataOutputStream out, 
        Class<K> keyClass, Class<V> valueClass,
        CompressionCodec codec, Counters.Counter writesCounter,
        boolean ownOutputStream)
        throws IOException {
      this.writtenRecordsCounter = writesCounter;
      this.checksumOut = new IFileOutputStream(out);
      this.rawOut = out;
      this.start = this.rawOut.getPos();
      if (codec != null) {
        // 从压缩编码池获取压缩器
        this.compressor = CodecPool.getCompressor(codec);
        if (this.compressor != null) {
          this.compressor.reset();
          this.compressedOut = codec.createOutputStream(checksumOut, compressor);
          this.out = new FSDataOutputStream(this.compressedOut,  null);
          this.compressOutput = true;
        } else {
          LOG.warn("Could not obtain compressor from CodecPool");
          this.out = new FSDataOutputStream(checksumOut,null);
        }
      } else {
        this.out = new FSDataOutputStream(checksumOut,null);
      }
      
      this.keyClass = keyClass;
      this.valueClass = valueClass;

      if (keyClass != null) {
        // 初始化序列化器
        SerializationFactory serializationFactory = 
          new SerializationFactory(conf);
        this.keySerializer = serializationFactory.getSerializer(keyClass);
        this.keySerializer.open(buffer);
        this.valueSerializer = serializationFactory.getSerializer(valueClass);
        this.valueSerializer.open(buffer);
      }
      this.ownOutputStream = ownOutputStream;
    }

    /**
     * 关闭写入器，完成IFile写入，释放资源并更新计数器。
     * @throws IOException IO异常
     */
    public void close() throws IOException {

      // BackupStore创建的writer不会设置键值类，因此需要判断后关闭序列化器
      if (keyClass != null) {
        keySerializer.close();
        valueSerializer.close();
      }

      // 写入文件结束标记
      WritableUtils.writeVInt(out, EOF_MARKER);
      WritableUtils.writeVInt(out, EOF_MARKER);
      decompressedBytesWritten += (long) 2 * WritableUtils.getVIntSize(EOF_MARKER);
      
      // 刷新输出流
      out.flush();
  
      if (compressOutput) {
        // 完成压缩并重置压缩状态
        compressedOut.finish();
        compressedOut.resetState();
      }
      
      // 如果持有输出流所有权则关闭流，否则写入校验和
      if (ownOutputStream) {
        out.close();
      }
      else {
        // 写入校验和
        checksumOut.finish();
      }

      // 计算压缩后字节总数
      compressedBytesWritten = rawOut.getPos() - start;

      if (compressOutput) {
        // 归还压缩器到编码池
        CodecPool.returnCompressor(compressor);
        compressor = null;
      }

      out = null;
      // 更新写入记录计数器
      if(writtenRecordsCounter != null) {
        writtenRecordsCounter.increment(numRecordsWritten);
      }
    }

    /**
     * 追加一个键值对到IFile中。
     * @param key 键对象
     * @param value 值对象
     * @throws IOException IO异常或类型不匹配异常
     */
    public void append(K key, V value) throws IOException {
      if (key.getClass() != keyClass)
        throw new IOException("wrong key class: "+ key.getClass()
                              +" is not "+ keyClass);
      if (value.getClass() != valueClass)
        throw new IOException("wrong value class: "+ value.getClass()
                              +" is not "+ valueClass);

      // 序列化键到缓冲区
      keySerializer.serialize(key);
      int keyLength = buffer.getLength();
      if (keyLength < 0) {
        throw new IOException("Negative key-length not allowed: " + keyLength + 
                              " for " + key);
      }

      // 序列化值到缓冲区
      valueSerializer.serialize(value);
      int valueLength = buffer.getLength() - keyLength;
      if (valueLength < 0) {
        throw new IOException("Negative value-length not allowed: " + 
                              valueLength + " for " + value);
      }
      
      // 写入长度和数据到输出流
      WritableUtils.writeVInt(out, keyLength);                  // 写入键长度
      WritableUtils.writeVInt(out, valueLength);                // 写入值长度
      out.write(buffer.getData(), 0, buffer.getLength());       // 写入键值数据

      // 重置缓冲区准备下一次写入
      buffer.reset();
      
      // 更新未压缩字节统计
      decompressedBytesWritten += (long) keyLength + valueLength +
                                  WritableUtils.getVIntSize(keyLength) + 
                                  WritableUtils.getVIntSize(valueLength);
      // 增加记录计数
      ++numRecordsWritten;
    }
    
    /**
     * 追加已经序列化好的键值对（从DataInputBuffer中直接读取）。
     * @param key 已序列化的键缓冲区
     * @param value 已序列化的值缓冲区
     * @throws IOException IO异常
     */
    public void append(DataInputBuffer key, DataInputBuffer value)
    throws IOException {
      int keyLength = key.getLength() - key.getPosition();
      if (keyLength < 0) {
        throw new IOException("Negative key-length not allowed: " + keyLength + 
                              " for " + key);
      }
      
      int valueLength = value.getLength() - value.getPosition();
      if (valueLength < 0) {
        throw new IOException("Negative value-length not allowed: " + 
                              valueLength + " for " + value);
      }

      // 写入长度和原始字节数据
      WritableUtils.writeVInt(out, keyLength);
      WritableUtils.writeVInt(out, valueLength);
      out.write(key.getData(), key.getPosition(), keyLength); 
      out.write(value.getData(), value.getPosition(), valueLength); 

      // 更新字节统计和记录计数
      decompressedBytesWritten += (long) keyLength + valueLength +
                      WritableUtils.getVIntSize(keyLength) + 
                      WritableUtils.getVIntSize(valueLength);
      ++numRecordsWritten;
    }
    
    // 为mark/reset功能提供输出流
    public DataOutputStream getOutputStream () {
      return out;
    }
    
    // 为外部append更新计数器，用于mark/reset场景
    public void updateCountersForExternalAppend(long length) {
      ++numRecordsWritten;
      decompressedBytesWritten += length;
    }
    
    /**
     * 获取写入的未压缩数据总长度。
     * @return 未压缩字节数
     */
    public long getRawLength() {
      return decompressedBytesWritten;
    }
    
    /**
     * 获取写入到磁盘的压缩后总长度。
     * @return 压缩后字节数
     */
    public long getCompressedLength() {
      return compressedBytesWritten;
    }
  }

  /**
   * IFile 读取器，用于从IFile格式文件中读取Map任务的中间输出键值对。
   * @param <K> 键类型
   * @param <V> 值类型
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class Reader<K extends Object, V extends Object> {
    private static final int DEFAULT_BUFFER_SIZE = 128*1024;
    private static final int MAX_VINT_SIZE = 9;

    // 统计从磁盘读取的记录数
    private long numRecordsRead = 0;
    private final Counters.Counter readRecordsCounter;

    final InputStream in;        // 解压后的输入流
    Decompressor decompressor;
    public long bytesRead = 0;
    protected final long fileLength;
    protected boolean eof = false;
    final IFileInputStream checksumIn;
    
    protected byte[] buffer = null;
    protected int bufferSize = DEFAULT_BUFFER_SIZE;
    protected DataInputStream dataIn;

    protected int recNo = 1;
    protected int currentKeyLength;
    protected int currentValueLength;
    byte keyBytes[] = new byte[0];
    
    
    /**
     * 构造IFile读取器，从指定文件路径打开读取。
     * @param conf Hadoop配置
     * @param fs 文件系统
     * @param file 要读取的IFile路径
     * @param codec 压缩编码，null表示不压缩
     * @param readsCounter 读取记录计数器
     * @throws IOException IO异常
     */
    public Reader(Configuration conf, FileSystem fs, Path file,
                  CompressionCodec codec,
                  Counters.Counter readsCounter) throws IOException {
      this(conf, fs.open(file), 
           fs.getFileStatus(file).getLen(),
           codec, readsCounter);
    }

    /**
     * 构造IFile读取器，从指定输入流读取。
     * @param conf Hadoop配置
     * @param in 输入流
     * @param length 输入流总长度（包含校验和）
     * @param codec 压缩编码，null表示不压缩
     * @param readsCounter 读取记录计数器
     * @throws IOException IO异常
     */
    public Reader(Configuration conf, FSDataInputStream in, long length, 
                  CompressionCodec codec,
                  Counters.Counter readsCounter) throws IOException {
      readRecordsCounter = readsCounter;
      // 创建带校验和验证的输入流
      checksumIn = new IFileInputStream(in,length, conf);
      if (codec != null) {
        // 从压缩编码池获取解压器
        decompressor = CodecPool.getDecompressor(codec);
        if (decompressor != null) {
          this.in = codec.createInputStream(checksumIn, decompressor);
        } else {
          LOG.warn("Could not obtain decompressor from CodecPool");
          this.in = checksumIn;
        }
      } else {
        this.in = checksumIn;
      }
      this.dataIn = new DataInputStream(this.in);
      this.fileLength = length;
      
      // 从配置读取IO缓冲区大小
      if (conf != null) {
        bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
      }
    }
    
    /**
     * 获取IFile中数据部分长度（减去校验和大小）。
     * @return 数据部分字节长度
     */
    public long getLength() { 
      return fileLength - checksumIn.getSize();
    }
    
    /**
     * 获取当前读取位置。
     * @return 当前字节位置
     * @throws IOException IO异常
     */
    public long getPosition() throws IOException {    
      return checksumIn.getPosition(); 
    }
    
    /**
     * 读取指定长度数据到缓冲区，处理压缩数据的分段读取。
     * @param buf 目标缓冲区
     * @param off 缓冲区起始偏移
     * @param len 需要读取的字节数
     * @return 实际读取的字节数
     * @throws IOException IO异常
     */
    private int readData(byte[] buf, int off, int len) throws IOException {
      int bytesRead = 0;
      // 循环读取直到满足需要的长度或到达流末尾
      while (bytesRead < len) {
        int n = IOUtils.wrappedReadForCompressedData(in, buf, off + bytesRead,
            len - bytesRead);
        if (n < 0) {
          return bytesRead;
        }
        bytesRead += n;
      }
      return len;
    }
    
    /**
     * 定位到下一条记录，读取键和值的长度，检查EOF标记。
     * @param dIn 数据输入流
     * @return 是否找到下一条有效记录，false表示到达文件末尾
     * @throws IOException IO异常
     */
    protected boolean positionToNextRecord(DataInput dIn) throws IOException {
      // 已经EOF则抛出异常
      if (eof) {
        throw new EOFException("Completed reading " + bytesRead);
      }
      
      // 读取键和值的长度
      currentKeyLength = WritableUtils.readVInt(dIn);
      currentValueLength = WritableUtils.readVInt(dIn);
      bytesRead += (long) WritableUtils.getVIntSize(currentKeyLength) +
                   WritableUtils.getVIntSize(currentValueLength);
      
      // 检查是否到达文件结束标记
      if (currentKeyLength == EOF_MARKER && currentValueLength == EOF_MARKER) {
        eof = true;
        return false;
      }
      
      //