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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.OutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator;
import org.apache.hadoop.util.Progressable;

/**
 * 文件：org.apache.hadoop.mapred.BasicTypeSorterBase.java
 * <p>
 * MapReduce Map端排序基类，使用基本int数组作为底层数据结构实现排序接口，为Map输出的内存排序提供基础能力。
 * 子类继承该类实现具体的排序逻辑，完成Map输出键值对的内存排序。
 */
abstract class BasicTypeSorterBase implements BufferSorter {
  
  // 存储key/value原始字节数据的缓冲区
  protected OutputBuffer keyValBuffer;
  // 存储每个key在keyValBuffer中的起始偏移量
  protected int[] startOffsets;
  // 存储每个key的字节长度
  protected int[] keyLengths;
  // 存储每个value的字节长度
  protected int[] valueLengths;
  // 存储startOffsets数组的索引，最终排序后得到按key顺序排列的索引数组
  protected int[] pointers;
  // Map输出key的比较器，用于排序
  protected RawComparator comparator;
  // 当前已缓存的key/value对数量
  protected int count;
  // 每个键值对占用的内存开销（四个int数组，合计16字节）
  static private final int BUFFERED_KEY_VAL_OVERHEAD = 16;
  // 数组初始大小
  static private final int INITIAL_ARRAY_SIZE = 5;
  // 记录遇到的最大key长度，用于准确估算内存占用
  private int maxKeyLength = 0;
  // 记录遇到的最大value长度，用于准确估算内存占用
  private int maxValLength = 0;

  // 进度上报对象，用于发送心跳保持任务活跃
  protected Progressable reporter;

  @Override
  public void configure(JobConf conf) {
    comparator = conf.getOutputKeyComparator();
  }
  
  @Override
  public void setProgressable(Progressable reporter) {
    this.reporter = reporter;  
  }

  /**
   * 添加一个键值对元数据到排序缓冲区
   * @param recordOffset key在输入缓冲区中的起始偏移
   * @param keyLength key的字节长度
   * @param valLength value的字节长度
   */
  public void addKeyValue(int recordOffset, int keyLength, int valLength) {
    // 如果数组已满，扩容数组
    if (startOffsets == null || count == startOffsets.length)
      grow();
    // 写入key偏移量和长度
    startOffsets[count] = recordOffset;
    keyLengths[count] = keyLength;
    // 更新最大key长度
    if (keyLength > maxKeyLength) {
      maxKeyLength = keyLength;
    }
    // 更新最大value长度
    if (valLength > maxValLength) {
      maxValLength = valLength;
    }
    // 写入value长度，设置索引指针
    valueLengths[count] = valLength;
    pointers[count] = count;
    count++;
  }

  @Override
  public void setInputBuffer(OutputBuffer buffer) {
    // 保存存储键值对原始数据的缓冲区引用
    this.keyValBuffer = buffer;
  }

  @Override
  public long getMemoryUtilized() {
    // 计算已使用内存：数组占用 + 最大键值长度（迭代时分配缓冲区使用）
    if (startOffsets != null) {
      return (startOffsets.length) * BUFFERED_KEY_VAL_OVERHEAD + 
              maxKeyLength + maxValLength;
    }
    else { // 未缓存任何数据
      return 0;
    }
  }

  /**
   * 对缓存的键值对进行排序，返回排序后的迭代器
   * @return 排序后的键值对迭代器
   */
  public abstract RawKeyValueIterator sort();
  
  @Override
  public void close() {
    // 重置计数，释放所有数组引用，方便GC回收
    count = 0;
    startOffsets = null;
    keyLengths = null;
    valueLengths = null;
    pointers = null;
    maxKeyLength = 0;
    maxValLength = 0;
    
    // 释放键值对缓冲区引用，方便GC回收
    keyValBuffer = null;
  }
  
  /**
   * 扩容所有存储元数据的int数组，按当前长度的1.1倍扩容
   */
  private void grow() {
    int currLength = 0;
    if (startOffsets != null) {
      currLength = startOffsets.length;
    }
    int newLength = (int)(currLength * 1.1) + 1;
    startOffsets = grow(startOffsets, newLength);
    keyLengths = grow(keyLengths, newLength);
    valueLengths = grow(valueLengths, newLength);
    pointers = grow(pointers, newLength);
  }
  
  /**
   * 扩容单个int数组，复制旧数组数据到新数组
   * @param old 旧数组
   * @param newLength 新数组长度
   * @return 扩容后的新数组
   */
  private int[] grow(int[] old, int newLength) {
    int[] result = new int[newLength];
    if(old != null) { 
      System.arraycopy(old, 0, result, 0, old.length);
    }
    return result;
  }
} //BasicTypeSorterBase

/**
 * 排序结果迭代器实现，遍历排序后的内存键值对，实现RawKeyValueIterator接口。
 * 排序完成后通过该迭代器依次访问排序后的键值对。
 */
class MRSortResultIterator implements RawKeyValueIterator {
  
  private int count;
  private int[] pointers;
  private int[] startOffsets;
  private int[] keyLengths;
  private int[] valLengths;
  private int currStartOffsetIndex;
  private int currIndexInPointers;
  private OutputBuffer keyValBuffer;
  private DataOutputBuffer key = new DataOutputBuffer();
  private InMemUncompressedBytes value = new InMemUncompressedBytes();
  
  /**
   * 构造排序结果迭代器
   * @param keyValBuffer 存储键值对原始数据的缓冲区
   * @param pointers 排序后的索引数组，指向元数据数组
   * @param startOffsets key起始偏移数组
   * @param keyLengths key长度数组
   * @param valLengths value长度数组
   */
  public MRSortResultIterator(OutputBuffer keyValBuffer, 
                              int []pointers, int []startOffsets,
                              int []keyLengths, int []valLengths) {
    this.count = pointers.length;
    this.pointers = pointers;
    this.startOffsets = startOffsets;
    this.keyLengths = keyLengths;
    this.valLengths = valLengths;
    this.keyValBuffer = keyValBuffer;
  }
  
  @Override
  public Progress getProgress() {
    return null;
  }
  
  @Override
  public DataOutputBuffer getKey() throws IOException {
    // 获取当前key的偏移和长度
    int currKeyOffset = startOffsets[currStartOffsetIndex];
    int currKeyLength = keyLengths[currStartOffsetIndex];
    // 重置缓冲区，复制key数据并返回
    key.reset();
    key.write(keyValBuffer.getData(), currKeyOffset, currKeyLength);
    return key;
  }

  @Override
  public ValueBytes getValue() throws IOException {
    // value存储位置：key偏移 + key长度，长度为valLengths对应值
    value.reset(keyValBuffer,
                startOffsets[currStartOffsetIndex] + keyLengths[currStartOffsetIndex],
                valLengths[currStartOffsetIndex]);
    return value;
  }

  @Override
  public boolean next() throws IOException {
    // 判断是否已经遍历完所有键值对
    if (count == currIndexInPointers)
      return false;
    // 获取当前键值对元数据索引，递增指针
    currStartOffsetIndex = pointers[currIndexInPointers];
    currIndexInPointers++;
    return true;
  }
  
  @Override
  public void close() {
    return;
  }
  
  /**
   * ValueBytes接口实现，用于内存中存储的未压缩value，提供序列化输出能力。
   */
  private static class InMemUncompressedBytes implements ValueBytes {
    private byte[] data;
    int start;
    int dataSize;
    /**
     * 重置value数据引用，指向缓冲区中的对应位置
     * @param d 存储原始数据的输出缓冲区
     * @param start value起始偏移
     * @param length value字节长度
     * @throws IOException
     */
    private void reset(OutputBuffer d, int start, int length) 
      throws IOException {
      data = d.getData();
      this.start = start;
      dataSize = length;
    }
            
    @Override
    public int getSize() {
      return dataSize;
    }
            
    @Override
    public void writeUncompressedBytes(DataOutputStream outStream)
      throws IOException {
      // 将未压缩value写出到输出流
      outStream.write(data, start, dataSize);
    }

    @Override
    public void writeCompressedBytes(DataOutputStream outStream) 
      throws IllegalArgumentException, IOException {
      // 本实现不支持压缩写出，抛出异常
      throw
        new IllegalArgumentException("UncompressedBytes cannot be compressed!");
    }
  
  } // InMemUncompressedBytes

} //MRSortResultIterator