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
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.security.IntermediateEncryptedStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级：MapReduce Reduce阶段值迭代器的标记-重置备份存储
 * <code>BackupStore</code> 是一个工具类，用于支持值迭代器的标记-重置（mark-reset）功能
 *
 * <p>它包含两级缓存：内存缓存和文件缓存。标记之后，迭代过程中遇到的键值对会被依次存入缓存。
 * 重置时，会从这些缓存中重新读取键值对。当内存缓存容量不足时，框架会自动将后续数据溢出到文件缓存。
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class BackupStore<K,V> {

  private static final Logger LOG =
      LoggerFactory.getLogger(BackupStore.class.getName());
  private static final int MAX_VINT_SIZE = 9;
  private static final int EOF_MARKER_SIZE = 2 * MAX_VINT_SIZE;
  private final TaskAttemptID tid;
 
  private MemoryCache memCache;
  private FileCache fileCache;

  List<Segment<K,V>> segmentList = new LinkedList<Segment<K,V>>();
  private int readSegmentIndex = 0;
  private int firstSegmentOffset = 0;

  private int currentKVOffset = 0;
  private int nextKVOffset = -1;

  private DataInputBuffer currentKey = null;
  private DataInputBuffer currentValue = new DataInputBuffer();
  private DataInputBuffer currentDiskValue = new DataInputBuffer();
 
  private boolean hasMore = false;
  private boolean inReset = false;
  private boolean clearMarkFlag = false;
  private boolean lastSegmentEOF = false;
  
  private Configuration conf;

  /**
   * 构造备份存储，初始化内存缓存和文件缓存
   * @param conf 作业配置
   * @param taskid 当前任务尝试ID
   * @throws IOException 初始化失败时抛出异常
   */
  public BackupStore(Configuration conf, TaskAttemptID taskid)
  throws IOException {
    
    final float bufferPercent =
      conf.getFloat(JobContext.REDUCE_MARKRESET_BUFFER_PERCENT, 0f);

    // 检查百分比配置合法性
    if (bufferPercent > 1.0 || bufferPercent < 0.0) {
      throw new IOException(JobContext.REDUCE_MARKRESET_BUFFER_PERCENT +
          bufferPercent);
    }

    // 根据堆内存百分比计算最大缓存大小
    int maxSize = (int)Math.min(
        Runtime.getRuntime().maxMemory() * bufferPercent, Integer.MAX_VALUE);

    // 支持绝对大小配置，若配置了绝对值则覆盖百分比计算结果
    int tmp = conf.getInt(JobContext.REDUCE_MARKRESET_BUFFER_SIZE, 0);
    if (tmp >  0) {
      maxSize = tmp;
    }

    memCache = new MemoryCache(maxSize);
    fileCache = new FileCache(conf);
    tid = taskid;
    
    this.conf = conf;
    
    LOG.info("Created a new BackupStore with a memory of " + maxSize);

  }

  /**
   * 将给定键值对写入缓存。
   * 内存缓存有空间则写入内存，否则写入磁盘文件缓存
   * @param key 输入键缓冲区
   * @param value 输入值缓冲区
   * @throws IOException 写入失败时抛出异常
   */
  public void write(DataInputBuffer key, DataInputBuffer value)
  throws IOException {

    assert (key != null && value != null);

    if (fileCache.isActive()) {
      fileCache.write(key, value);
      return;
    }

    if (memCache.reserveSpace(key, value)) {
      memCache.write(key, value);
    } else {
      fileCache.activate();
      fileCache.write(key, value);
    }
  }

  /**
   * 标记当前迭代位置，清理已消费的段，保存重置起始位置
   * @throws IOException 操作失败时抛出异常
   */
  public void mark() throws IOException {

    // hasNext会提前读取下一个KV，如果新分段已被预读但用户还没调用next()，需要回退分段索引

    if (nextKVOffset == 0) {
      assert (readSegmentIndex != 0);
      assert (currentKVOffset != 0);
      readSegmentIndex --;
    }

    // 删除当前活跃分段之前的所有已消费分段，释放资源

    int i = 0;
    Iterator<Segment<K,V>> itr = segmentList.iterator();
    while (itr.hasNext()) {
      Segment<K,V> s = itr.next();
      if (i == readSegmentIndex) {
        break;
      }
      s.close();
      itr.remove();
      i++;
      LOG.debug("Dropping a segment");
    }

    // 记录重置后需要开始读取的起始偏移量

    firstSegmentOffset = currentKVOffset;
    readSegmentIndex = 0;

    LOG.debug("Setting the FirsSegmentOffset to " + currentKVOffset);
  }

  /**
   * 重置迭代到上一次标记的位置，重新准备从缓存读取数据
   * @throws IOException 重置失败时抛出异常
   */
  public void reset() throws IOException {

    // 仅在首次进入重置模式时，为已写入的记录创建可读分段

    if (!inReset) {
      if (fileCache.isActive) {
        fileCache.createInDiskSegment();
      } else {
        memCache.createInMemorySegment();
      }
    } 

    inReset = true;
    
    // 将所有分段重置到正确的读取起始位置
    for (int i = 0; i < segmentList.size(); i++) {
      Segment<K,V> s = segmentList.get(i);
      if (s.inMemory()) {
        int offset = (i == 0) ? firstSegmentOffset : 0;
        s.getReader().reset(offset);
      } else {
        s.closeReader();
        if (i == 0) {
          s.reinitReader(firstSegmentOffset);
          s.getReader().disableChecksumValidation();
        }
      }
    }
    
    // 重置迭代状态变量
    currentKVOffset = firstSegmentOffset;
    nextKVOffset = -1;
    readSegmentIndex = 0;
    hasMore = false;
    lastSegmentEOF = false;

    LOG.debug("Reset - First segment offset is " + firstSegmentOffset +
        " Segment List Size is " + segmentList.size());
  }

  /**
   * 检查是否还有下一个键值对可供读取
   * @return 如果有下一个键值对返回true，否则返回false
   * @throws IOException 读取失败时抛出异常
   */
  public boolean hasNext() throws IOException {
    
    if (lastSegmentEOF) {
      return false;
    }
    
    // 提前预读下一个KV，hasMore用于避免hasNext多次调用导致重复预读

    if (hasMore) {
      return true;
    }

    Segment<K,V> seg = segmentList.get(readSegmentIndex);
    // 记录当前预读位置，用户调用next后会更新currentKVOffset
    nextKVOffset = (int) seg.getActualPosition();
    if (seg.nextRawKey()) {
      currentKey = seg.getKey();
      seg.getValue(currentValue);
      hasMore = true;
      return true;
    } else {
      if (!seg.inMemory()) {
        seg.closeReader();
      }
    }

    // 当前分段已经读完，如果已是最后一个分段则标记结束
    if (readSegmentIndex == segmentList.size() - 1) {
      nextKVOffset = -1;
      lastSegmentEOF = true;
      return false;
    }

    // 切换到下一个分段
    nextKVOffset = 0;
    readSegmentIndex ++;

    Segment<K,V> nextSegment = segmentList.get(readSegmentIndex);
    
    // 从内存分段切换到磁盘分段时，重置值缓冲区避免数据损坏，参见HADOOP-5494
    
    if (!nextSegment.inMemory()) {
      currentValue.reset(currentDiskValue.getData(), 
          currentDiskValue.getLength());
      nextSegment.init(null);
    }
 
    // 从新分段预读第一个KV
    if (nextSegment.nextRawKey()) {
      currentKey = nextSegment.getKey();
      nextSegment.getValue(currentValue);
      hasMore = true;
      return true;
    } else {
      throw new IOException("New segment did not have even one K/V");
    }
  }

  /**
   * 移动到下一个键值对，消费预读的结果
   * @throws IOException 移动失败时抛出异常
   */
  public void next() throws IOException {
    if (!hasNext()) {
      throw new NoSuchElementException("iterate past last value");
    }
    // 重置预读标记，参见hasNext中的注释
    hasMore = false;
    currentKVOffset = nextKVOffset;
    nextKVOffset = -1;
  }

  /**
   * 获取当前迭代位置的值
   * @return 当前值的输入缓冲区
   */
  public DataInputBuffer nextValue() {
    return  currentValue;
  }

  /**
   * 获取当前迭代位置的键
   * @return 当前键的输入缓冲区
   */
  public DataInputBuffer nextKey() {
    return  currentKey;
  }

  /**
   * 重新初始化备份存储，清空所有分段和缓存，重置所有状态
   * @throws IOException 初始化失败时抛出异常
   */
  public void reinitialize() throws IOException {
    if (segmentList.size() != 0) {
      clearSegmentList();
    }
    memCache.reinitialize(true);
    fileCache.reinitialize();
    readSegmentIndex = firstSegmentOffset = 0;
    currentKVOffset = 0;
    nextKVOffset = -1;
    hasMore = inReset = clearMarkFlag = false;
  }

  /**
   * 退出重置模式，清理不需要的缓存数据
   * 当在重置模式外调用mark时，会触发该方法
   * @throws IOException 退出失败时抛出异常
   */
  public void exitResetMode() throws IOException { 
    inReset = false;
    if (clearMarkFlag ) {
      // 如果在重置模式下设置了清除标记，退出时执行重新初始化，参见clearMark()
      reinitialize();
      return;
    }
    if (!fileCache.isActive) {
      memCache.reinitialize(false);
    }
  }

  /**
   * 获取输出流，用于直接写入指定长度的键值对原始字节
   * @param length 即将写入的字节长度
   * @return 可写入的输出流，可能是内存缓存流或文件输出流
   * @throws IOException 获取流失败时抛出异常
   */
  public DataOutputStream getOutputStream(int length) throws IOException {
    if (memCache.reserveSpace(length)) {
      return memCache.dataOut;
    } else {
      fileCache.activate();
      return fileCache.writer.getOutputStream();
    }
  }

  /**
   * 更新已使用空间计数器，用于直接写入原始字节后的统计
   * @param length 写入的字节长度
   */
  public void updateCounters(int length) {
    if (fileCache.isActive) {
      fileCache.writer.updateCountersForExternalAppend(length);
    } else {
      memCache.usedSize += length;
    }
  }

  /**
   * 清除当前标记，如果处于重置模式则延迟清除到退出重置时执行
   * @throws IOException 清除失败时抛出异常
   */
  public void clearMark() throws IOException {
    if (inReset) {
      // 如果当前处于重置模式，仅设置标记，退出重置模式后再执行重新初始化
      clearMarkFlag = true;
    } else {
      reinitialize();
    }
  }
  
  /**
   * 清空所有分段，关闭并释放每个分段占用的资源
   * @throws IOException 关闭分段失败时抛出异常
   */
  private void clearSegmentList() throws IOException {
    for (Segment<K,V> segment: segmentList) {
      long len = segment.getLength();
      segment.close();
      if (segment.inMemory()) {
       memCache.unreserve(len);
      }
    }
    segmentList.clear();
  }

  /**
   * 内存缓存实现类，负责在内存中存储备份的键值对数据
   */
  class MemoryCache {
    private DataOutputBuffer dataOut;
    private int blockSize;
    private int usedSize;
    private final BackupRamManager ramManager;

    // 内存缓存分块存储，默认块大小1MB
    private int defaultBlockSize = 1024 * 1024;

    /**
     * 构造内存缓存，初始化内存管理器
     * @param maxSize 最大可使用内存大小
     */
    public MemoryCache(int maxSize) {
      ramManager = new BackupRamManager(maxSize);
      if (maxSize < defaultBlockSize) {
        defaultBlockSize = maxSize;
      }
    }

    /**
     * 释放指定大小的内存空间
     * @param len 需要释放的字节数
     */
    public void unreserve(long len) {
      ramManager.unreserve((int)len);
    }

    /**
     * 重新初始化内存缓存，分配新的内存块
     * @param clearAll 如果为true，同时重置内存管理器
     */
    void reinitialize(boolean clearAll) {
      if (clearAll) {
        ramManager.reinitialize();
      }
      int allocatedSize = createNewMemoryBlock(defaultBlockSize, 
          defaultBlockSize);
      assert(allocatedSize == defaultBlockSize || allocatedSize == 0);
      LOG.debug("Created a new mem block of " + allocatedSize);
    }

    /**
     * 创建新的内存块，分配指定大小的内存
     * @param requestedSize 请求分配的大小
     * @param minSize 最小需要分配的大小
     * @return 实际分配的大小，0表示分配失败
     */
    private int createNewMemoryBlock(int requestedSize, int minSize) {
      int allocatedSize = ramManager.reserve(requestedSize, minSize);
      usedSize = 0;
      if (allocatedSize == 0) {
        dataOut = null;
        blockSize = 0;
      } else {
        dataOut = new DataOutputBuffer(allocatedSize);
        blockSize = allocatedSize;
      }
      return allocatedSize;
    }

    /**
     * 检查是否有足够剩余空间容纳指定长度的数据加上EOF标记
     * @param length 请求写入的数据长度
     * @return true表示空间足够，false表示空间不足
     * @throws IOException 空间不足且创建新块失败时抛出异常
     */
    boolean reserveSpace(int length) throws IOException {
      int availableSize = blockSize - usedSize;
      if (availableSize >= length + EOF_MARKER_SIZE) {
        return true;
      }
      // 当前块空间不足，将当前块转为可读分段，必须不在重置模式
      assert (!inReset); 

      createInMemorySegment();
      
      // 创建新的内存块
      int tmp = Math.max(length + EOF_MARKER_SIZE, defaultBlockSize);
      availableSize = createNewMemoryBlock(tmp, 
          (length + EOF_MARKER_SIZE));
      
      return (availableSize == 0) ? false : true;
    }

    /**
     * 检查是否有足够空间存储给定键值对
     * @param key 键缓冲区
     * @param value 值缓冲区
     * @return true表示空间足够，false表示空间不足
     * @throws IOException 空间不足且创建新块失败时抛出异常
     */
    boolean