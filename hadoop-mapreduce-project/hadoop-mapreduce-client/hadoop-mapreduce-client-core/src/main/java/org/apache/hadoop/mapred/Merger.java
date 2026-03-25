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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.mapreduce.security.IntermediateEncryptedStream;
import org.apache.hadoop.util.PriorityQueue;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级说明：MapReduce排序合并工具类，为Map任务和Reduce任务提供内存段与磁盘段的多路归并排序功能
 * 核心功能：将多个已经排序好的键值对分段合并为一个全局排序的输出，是MapReduce Shuffle过程的核心组件
 */
/**
 * Merger is an utility class used by the Map and Reduce tasks for merging
 * both their memory and disk segments
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Merger {  
  private static final Logger LOG = LoggerFactory.getLogger(Merger.class);

  // 本地目录分配器，用于分配临时合并文件的存储路径
  // Local directories
  private static LocalDirAllocator lDirAlloc = 
    new LocalDirAllocator(MRConfig.LOCAL_DIR);

  /**
   * 合并多个输入文件中的已排序分段，返回归并后的迭代器
   * @param conf 作业配置对象
   * @param fs 文件系统对象
   * @param keyClass 键类型
   * @param valueClass 值类型
   * @param codec 压缩编解码器
   * @param inputs 输入文件路径数组
   * @param deleteInputs 合并后是否删除输入文件
   * @param mergeFactor 归并因子，每次最多合并多少个分段
   * @param tmpDir 临时文件目录
   * @param comparator 键排序比较器
   * @param reporter 进度汇报器
   * @param readsCounter 读操作计数器
   * @param writesCounter 写操作计数器
   * @param mergePhase 进度对象
   * @return 归并后的键值对迭代器
   * @throws IOException IO异常
   */
  public static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class<K> keyClass, Class<V> valueClass, 
                            CompressionCodec codec,
                            Path[] inputs, boolean deleteInputs, 
                            int mergeFactor, Path tmpDir,
                            RawComparator<K> comparator, Progressable reporter,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter,
                            Progress mergePhase)
  throws IOException {
    return 
      new MergeQueue<K, V>(conf, fs, inputs, deleteInputs, codec, comparator, 
                           reporter, null,
                           TaskType.REDUCE).merge(keyClass, valueClass,
                                           mergeFactor, tmpDir,
                                           readsCounter, writesCounter, 
                                           mergePhase);
  }

  /**
   * 合并多个输入文件中的已排序分段，支持统计合并Map输出个数，返回归并后的迭代器
   * @param conf 作业配置对象
   * @param fs 文件系统对象
   * @param keyClass 键类型
   * @param valueClass 值类型
   * @param codec 压缩编解码器
   * @param inputs 输入文件路径数组
   * @param deleteInputs 合并后是否删除输入文件
   * @param mergeFactor 归并因子，每次最多合并多少个分段
   * @param tmpDir 临时文件目录
   * @param comparator 键排序比较器
   * @param reporter 进度汇报器
   * @param readsCounter 读操作计数器
   * @param writesCounter 写操作计数器
   * @param mergedMapOutputsCounter 已合并Map输出计数器
   * @param mergePhase 进度对象
   * @return 归并后的键值对迭代器
   * @throws IOException IO异常
   */
  public static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class<K> keyClass, Class<V> valueClass, 
                            CompressionCodec codec,
                            Path[] inputs, boolean deleteInputs, 
                            int mergeFactor, Path tmpDir,
                            RawComparator<K> comparator,
                            Progressable reporter,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter,
                            Counters.Counter mergedMapOutputsCounter,
                            Progress mergePhase)
  throws IOException {
    return 
      new MergeQueue<K, V>(conf, fs, inputs, deleteInputs, codec, comparator, 
                           reporter, mergedMapOutputsCounter,
                           TaskType.REDUCE).merge(
                                           keyClass, valueClass,
                                           mergeFactor, tmpDir,
                                           readsCounter, writesCounter,
                                           mergePhase);
  }
  
  /**
   * 合并多个已准备好的分段，返回归并后的迭代器
   * @param conf 作业配置对象
   * @param fs 文件系统对象
   * @param keyClass 键类型
   * @param valueClass 值类型
   * @param segments 待合并分段列表
   * @param mergeFactor 归并因子，每次最多合并多少个分段
   * @param tmpDir 临时文件目录
   * @param comparator 键排序比较器
   * @param reporter 进度汇报器
   * @param readsCounter 读操作计数器
   * @param writesCounter 写操作计数器
   * @param mergePhase 进度对象
   * @return 归并后的键值对迭代器
   * @throws IOException IO异常
   */
  public static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs, 
                            Class<K> keyClass, Class<V> valueClass, 
                            List<Segment<K, V>> segments, 
                            int mergeFactor, Path tmpDir,
                            RawComparator<K> comparator, Progressable reporter,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter,
                            Progress mergePhase)
      throws IOException {
    return merge(conf, fs, keyClass, valueClass, segments, mergeFactor, tmpDir,
                 comparator, reporter, false, readsCounter, writesCounter,
                 mergePhase);
  }

  /**
   * 合并多个已准备好的分段，支持分段排序，返回归并后的迭代器
   * @param conf 作业配置对象
   * @param fs 文件系统对象
   * @param keyClass 键类型
   * @param valueClass 值类型
   * @param segments 待合并分段列表
   * @param mergeFactor 归并因子，每次最多合并多少个分段
   * @param tmpDir 临时文件目录
   * @param comparator 键排序比较器
   * @param reporter 进度汇报器
   * @param sortSegments 是否按长度对分段排序
   * @param readsCounter 读操作计数器
   * @param writesCounter 写操作计数器
   * @param mergePhase 进度对象
   * @return 归并后的键值对迭代器
   * @throws IOException IO异常
   */
  public static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class<K> keyClass, Class<V> valueClass,
                            List<Segment<K, V>> segments,
                            int mergeFactor, Path tmpDir,
                            RawComparator<K> comparator, Progressable reporter,
                            boolean sortSegments,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter,
                            Progress mergePhase)
      throws IOException {
    return new MergeQueue<K, V>(conf, fs, segments, comparator, reporter,
                           sortSegments,
                           TaskType.REDUCE).merge(keyClass, valueClass,
                                               mergeFactor, tmpDir,
                                               readsCounter, writesCounter,
                                               mergePhase);
  }

  /**
   * 合并多个已准备好的分段，支持自定义任务类型和压缩，返回归并后的迭代器
   * @param conf 作业配置对象
   * @param fs 文件系统对象
   * @param keyClass 键类型
   * @param valueClass 值类型
   * @param codec 压缩编解码器
   * @param segments 待合并分段列表
   * @param mergeFactor 归并因子，每次最多合并多少个分段
   * @param tmpDir 临时文件目录
   * @param comparator 键排序比较器
   * @param reporter 进度汇报器
   * @param sortSegments 是否按长度对分段排序
   * @param readsCounter 读操作计数器
   * @param writesCounter 写操作计数器
   * @param mergePhase 进度对象
   * @param taskType 任务类型（MAP/REDUCE）
   * @return 归并后的键值对迭代器
   * @throws IOException IO异常
   */
  public static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class<K> keyClass, Class<V> valueClass,
                            CompressionCodec codec,
                            List<Segment<K, V>> segments,
                            int mergeFactor, Path tmpDir,
                            RawComparator<K> comparator, Progressable reporter,
                            boolean sortSegments,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter,
                            Progress mergePhase,
                            TaskType taskType)
      throws IOException {
    return new MergeQueue<K, V>(conf, fs, segments, comparator, reporter,
                           sortSegments, codec,
                           taskType).merge(keyClass, valueClass,
                                               mergeFactor, tmpDir,
                                               readsCounter, writesCounter,
                                               mergePhase);
  }

  /**
   * 合并多个分段，区分内存分段和磁盘分段，返回归并后的迭代器
   * @param conf 作业配置对象
   * @param fs 文件系统对象
   * @param keyClass 键类型
   * @param valueClass 值类型
   * @param segments 待合并分段列表
   * @param mergeFactor 归并因子，每次最多合并多少个分段
   * @param inMemSegments 内存分段数量
   * @param tmpDir 临时文件目录
   * @param comparator 键排序比较器
   * @param reporter 进度汇报器
   * @param sortSegments 是否按长度对分段排序
   * @param readsCounter 读操作计数器
   * @param writesCounter 写操作计数器
   * @param mergePhase 进度对象
   * @return 归并后的键值对迭代器
   * @throws IOException IO异常
   */
  public static <K extends Object, V extends Object>
    RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class<K> keyClass, Class<V> valueClass,
                            List<Segment<K, V>> segments,
                            int mergeFactor, int inMemSegments, Path tmpDir,
                            RawComparator<K> comparator, Progressable reporter,
                            boolean sortSegments,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter,
                            Progress mergePhase)
      throws IOException {
    return new MergeQueue<K, V>(conf, fs, segments, comparator, reporter,
                           sortSegments,
                           TaskType.REDUCE).merge(keyClass, valueClass,
                                               mergeFactor, inMemSegments,
                                               tmpDir,
                                               readsCounter, writesCounter,
                                               mergePhase);
  }


  /**
   * 合并多个分段，区分内存分段和磁盘分段，支持压缩，返回归并后的迭代器
   * @param conf 作业配置对象
   * @param fs 文件系统对象
   * @param keyClass 键类型
   * @param valueClass 值类型
   * @param codec 压缩编解码器
   * @param segments 待合并分段列表
   * @param mergeFactor 归并因子，每次最多合并多少个分段
   * @param inMemSegments 内存分段数量
   * @param tmpDir 临时文件目录
   * @param comparator 键排序比较器
   * @param reporter 进度汇报器
   * @param sortSegments 是否按长度对分段排序
   * @param readsCounter 读操作计数器
   * @param writesCounter 写操作计数器
   * @param mergePhase 进度对象
   * @return 归并后的键值对迭代器
   * @throws IOException IO异常
   */
  public static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                          Class<K> keyClass, Class<V> valueClass,
                          CompressionCodec codec,
                          List<Segment<K, V>> segments,
                          int mergeFactor, int inMemSegments, Path tmpDir,
                          RawComparator<K> comparator, Progressable reporter,
                          boolean sortSegments,
                          Counters.Counter readsCounter,
                          Counters.Counter writesCounter,
                          Progress mergePhase)
    throws IOException {
  return new MergeQueue<K, V>(conf, fs, segments, comparator, reporter,
                         sortSegments, codec,
                         TaskType.REDUCE).merge(keyClass, valueClass,
                                             mergeFactor, inMemSegments,
                                             tmpDir,
                                             readsCounter, writesCounter,
                                             mergePhase);
}

  /**
   * 将归并迭代器中的所有键值对写入IFile输出流
   * @param records 归并后的键值对迭代器
   * @param writer IFile写入器
   * @param progressable 进度汇报对象
   * @param conf 作业配置
   * @throws IOException IO异常
   */
  public static <K extends Object, V extends Object>
  void writeFile(RawKeyValueIterator records, Writer<K, V> writer, 
                 Progressable progressable, Configuration conf) 
  throws IOException {
    long progressBar = conf.getLong(JobContext.RECORDS_BEFORE_PROGRESS,
        10000);
    long recordCtr = 0;
    while(records.next()) {
      writer.append(records.getKey(), records.getValue());
      
      if (((recordCtr++) % progressBar) == 0) {
        progressable.progress();
      }
    }
}

  /**
   * 类说明：表示一个待合并的键值对分段，可以是内存中的分段也可以是磁盘上的文件分段
   * 核心职责：封装分段的读取、初始化、关闭等操作，统一内存和磁盘分段的访问接口
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class Segment<K extends Object, V extends Object> {
    Reader<K, V> reader = null;
    final DataInputBuffer key = new DataInputBuffer();
    
    Configuration conf = null;
    FileSystem fs = null;
    Path file = null;
    boolean preserve = false;
    CompressionCodec codec = null;
    long segmentOffset = 0;
    long segmentLength = -1;
    long rawDataLength = -1;
    
    Counters.Counter mapOutputsCounter = null;

    public Segment(Configuration conf, FileSystem fs, Path file,
                   CompressionCodec codec, boolean preserve)
    throws IOException {
      this(conf, fs, file, codec, preserve, null);
    }

    public Segment(Configuration conf, FileSystem fs, Path file,
                   CompressionCodec codec, boolean preserve,
                   Counters.Counter mergedMapOutputsCounter)
  throws IOException {
      this(conf, fs, file, 0, fs.getFileStatus(file).getLen(), codec, preserve, 
           mergedMapOutputsCounter);
    }
    
    public Segment(Configuration conf, FileSystem fs, Path file,
        CompressionCodec codec, boolean preserve,
        Counters.Counter mergedMapOutputsCounter, long rawDataLength)
            throws IOException {
      this(conf, fs, file, 0, fs.getFileStatus(file).getLen(), codec, preserve, 
          mergedMapOutputsCounter);
      this.rawDataLength = rawDataLength;
    }

    public Segment(Configuration conf, FileSystem fs, Path file,
                   long segmentOffset, long segmentLength,
                   CompressionCodec codec,
                   boolean preserve) throws IOException {
      this(conf, fs, file, segmentOffset, segmentLength, codec, preserve, null);
    }

    public Segment(Configuration conf, FileSystem fs, Path file,
        long segmentOffset, long segmentLength, CompressionCodec codec,
        boolean preserve, Counters.Counter mergedMapOutput