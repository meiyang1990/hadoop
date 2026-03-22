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

package org.apache.hadoop.mapreduce.lib.partition;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件：全排序分区器，通过读取外部生成的分割点实现键的全局有序分区
 * 核心作用：为全排序作业提供分区策略，使得每个Reduce分区处理一段有序的键范围，输出结果整体全局有序
 * 支持两种查找分区方式：二分查找和Trie树快速查找，对二进制可比键默认启用Trie树优化
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TotalOrderPartitioner<K,V>
    extends Partitioner<K,V> implements Configurable {

  private Node partitions;
  public static final String DEFAULT_PATH = "_partition.lst";
  public static final String PARTITIONER_PATH = 
    "mapreduce.totalorderpartitioner.path";
  public static final String MAX_TRIE_DEPTH = 
    "mapreduce.totalorderpartitioner.trie.maxdepth"; 
  public static final String NATURAL_ORDER = 
    "mapreduce.totalorderpartitioner.naturalorder";
  Configuration conf;
  private static final Logger LOG =
      LoggerFactory.getLogger(TotalOrderPartitioner.class);

  /**
   * 构造函数，初始化空的全排序分区器
   */
  public TotalOrderPartitioner() { }

  /**
   * 读取分区文件构建分区索引数据结构，完成分区器初始化
   * 如果键类型是BinaryComparable且启用自然排序，则构建Trie树加速分区查找
   * 否则使用二分查找在分区键集合中定位分区。分区文件必须包含R-1个已排序键（R为Reduce数量）
   */
  @SuppressWarnings("unchecked") // keytype from conf not static
  public void setConf(Configuration conf) {
    try {
      this.conf = conf;
      String parts = getPartitionFile(conf);
      final Path partFile = new Path(parts);
      final FileSystem fs = (DEFAULT_PATH.equals(parts))
        ? FileSystem.getLocal(conf)     // 默认路径在分布式缓存中，使用本地文件系统读取
        : partFile.getFileSystem(conf); // 自定义路径从对应文件系统读取

      Job job = Job.getInstance(conf);
      Class<K> keyClass = (Class<K>)job.getMapOutputKeyClass();
      // 从分区文件中读取所有分割点
      K[] splitPoints = readPartitions(fs, partFile, keyClass, conf);
      // 校验分割点数量是否符合要求（必须等于Reduce数量减1）
      if (splitPoints.length != job.getNumReduceTasks() - 1) {
        throw new IOException("Wrong number of partitions in keyset");
      }
      RawComparator<K> comparator =
        (RawComparator<K>) job.getSortComparator();
      // 校验分割点是否有序
      for (int i = 0; i < splitPoints.length - 1; ++i) {
        if (comparator.compare(splitPoints[i], splitPoints[i+1]) >= 0) {
          throw new IOException("Split points are out of order");
        }
      }
      boolean natOrder =
        conf.getBoolean(NATURAL_ORDER, true);
      // 符合条件则构建Trie树加速分区查找
      if (natOrder && BinaryComparable.class.isAssignableFrom(keyClass)) {
        partitions = buildTrie((BinaryComparable[])splitPoints, 0,
            splitPoints.length, new byte[0],
            conf.getInt(MAX_TRIE_DEPTH, 200));
      } else {
        // 否则使用二分查找实现
        partitions = new BinarySearchNode(splitPoints, comparator);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Can't read partitions file", e);
    }
  }

  /**
   * 获取分区器配置
   * @return 分区器配置对象
   */
  public Configuration getConf() {
    return conf;
  }
  
  /**
   * 根据键获取对应分区编号，实现分区逻辑
   * @param key Map输出键
   * @param value Map输出值
   * @param numPartitions 分区总数
   * @return 该键对应的分区编号
   */
  @SuppressWarnings("unchecked") // is memcmp-able and uses the trie
  public int getPartition(K key, V value, int numPartitions) {
    return partitions.findPartition(key);
  }

  /**
   * 设置存储排序分区键集合的SequenceFile路径
   * @param conf 作业配置
   * @param p 分区文件路径
   */
  public static void setPartitionFile(Configuration conf, Path p) {
    conf.set(PARTITIONER_PATH, p.toString());
  }

  /**
   * 获取存储排序分区键集合的SequenceFile路径
   * @param conf 作业配置
   * @return 分区文件路径字符串
   */
  public static String getPartitionFile(Configuration conf) {
    return conf.get(PARTITIONER_PATH, DEFAULT_PATH);
  }

  /**
   * 分区查找节点接口，定义查找键所在分区的抽象方法
   * @param <T> 键类型
   */
  interface Node<T> {
    /**
     * 在分区键集合中定位给定键对应的分区编号
     * @param key 待查找的键
     * @return 分区编号
     */
    int findPartition(T key);
  }

  /**
   * Trie节点抽象基类，为二进制可比键构建Trie树提供基础
   * 根据键的前缀字节构建多层Trie树，加速分区查找
   */
  static abstract class TrieNode implements Node<BinaryComparable> {
    private final int level;
    TrieNode(int level) {
      this.level = level;
    }
    int getLevel() {
      return level;
    }
  }

  /**
   * 二分查找分区节点，针对非BinaryComparable类型或禁用自然排序时使用
   * 通过二分查找在分区分割点数组中定位键所在分区
   */
  class BinarySearchNode implements Node<K> {
    private final K[] splitPoints;
    private final RawComparator<K> comparator;
    BinarySearchNode(K[] splitPoints, RawComparator<K> comparator) {
      this.splitPoints = splitPoints;
      this.comparator = comparator;
    }
    public int findPartition(K key) {
      final int pos = Arrays.binarySearch(splitPoints, key, comparator) + 1;
      return (pos < 0) ? -pos : pos;
    }
  }

  /**
   * Trie内部节点，每个节点包含256个子节点（对应一个字节的所有可能值）
   * 根据当前层级字节值选择对应子节点继续向下查找
   */
  class InnerTrieNode extends TrieNode {
    private TrieNode[] child = new TrieNode[256];

    InnerTrieNode(int level) {
      super(level);
    }
    public int findPartition(BinaryComparable key) {
      int level = getLevel();
      // 键长度小于当前层级，使用第0个子节点处理
      if (key.getLength() <= level) {
        return child[0].findPartition(key);
      }
      // 根据当前层级字节值选择子节点继续查找
      return child[0xFF & key.getBytes()[level]].findPartition(key);
    }
  }
  
  /**
   * 根据当前节点包含的分割点数量，创建对应类型的叶子Trie节点
   * @param level 当前节点在Trie中的深度
   * @param splitPoints 全部分割点数组
   * @param lower 当前区间包含的第一个分割点索引
   * @param upper 当前区间不包含的第一个分割点索引
   * @return 对应类型的叶子Trie节点
   */
  private TrieNode LeafTrieNodeFactory
             (int level, BinaryComparable[] splitPoints, int lower, int upper) {
      switch (upper - lower) {
      case 0:
          // 无分割点，返回无分割节点
          return new UnsplitTrieNode(level, lower);
          
      case 1:
          // 一个分割点，返回单分割点节点
          return new SinglySplitTrieNode(level, splitPoints, lower);
          
      default:
          // 多个分割点，返回通用叶子节点（内部使用二分查找）
          return new LeafTrieNode(level, splitPoints, lower, upper);
      }
  }

  /**
   * 通用叶子Trie节点，当当前层级仍包含多个分割点时使用，内部通过二分查找定位分区
   */
  private class LeafTrieNode extends TrieNode {
    final int lower;
    final int upper;
    final BinaryComparable[] splitPoints;
    LeafTrieNode(int level, BinaryComparable[] splitPoints, int lower, int upper) {
      super(level);
      this.lower = lower;
      this.upper = upper;
      this.splitPoints = splitPoints;
    }
    public int findPartition(BinaryComparable key) {
      final int pos = Arrays.binarySearch(splitPoints, lower, upper, key) + 1;
      return (pos < 0) ? -pos : pos;
    }
  }
  
  /**
   * 无分割点叶子Trie节点，所有命中该节点的键都固定返回同一个分区编号
   */
  private class UnsplitTrieNode extends TrieNode {
      final int result;
      
      UnsplitTrieNode(int level, int value) {
          super(level);
          this.result = value;
      }
      
      public int findPartition(BinaryComparable key) {
          return result;
      }
  }
  
  /**
   * 单分割点叶子Trie节点，仅包含一个分割点，直接比较即可得到分区编号
   */
  private class SinglySplitTrieNode extends TrieNode {
      final int               lower;
      final BinaryComparable  mySplitPoint;
      
      SinglySplitTrieNode(int level, BinaryComparable[] splitPoints, int lower) {
          super(level);
          this.lower = lower;
          this.mySplitPoint = splitPoints[lower];
      }
      
      public int findPartition(BinaryComparable key) {
          return lower + (key.compareTo(mySplitPoint) < 0 ? 0 : 1);
      }
  }


  /**
   * 从指定SequenceFile中读取所有分割点
   * @param fs 分区文件所在文件系统
   * @param p 分区文件路径
   * @param keyClass Map输出键类型
   * @param conf 作业配置
   * @return 分割点数组
   * @throws IOException 读取文件时发生IO异常
   */
                                 // matching key types enforced by passing in
  @SuppressWarnings("unchecked") // map output key class
  private K[] readPartitions(FileSystem fs, Path p, Class<K> keyClass,
      Configuration conf) throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(
        conf,
        SequenceFile.Reader.file(p));
    ArrayList<K> parts = new ArrayList<K>();
    K key = ReflectionUtils.newInstance(keyClass, conf);
    try {
      // 遍历SequenceFile读取所有分割点
      while ((key = (K) reader.next(key)) != null) {
        parts.add(key);
        key = ReflectionUtils.newInstance(keyClass, conf);
      }
      reader.close();
      reader = null;
    } finally {
      // 确保资源关闭
      IOUtils.cleanupWithLogger(LOG, reader);
    }
    return parts.toArray((K[])Array.newInstance(keyClass, parts.size()));
  }
  
  /**
   * 携带可复用Trie节点引用的辅助类，用于复用语义相同的无分割节点，减少内存占用
   * 相邻的无分割节点语义相同，可以复用同一个对象实例
   */  
  private class CarriedTrieNodeRef
  {
      TrieNode   content;
      
      CarriedTrieNodeRef() {
          content = null;
      }
  }

  
  /**
   * 根据已排序分割点集合构建Trie树，用于快速分区查找
   * @param splits 已排序分割点数组
   * @param lower 当前区间的下界（包含）
   * @param upper 当前区间的上界（不包含）
   * @param prefix 当前已处理的键前缀
   * @param maxDepth Trie树最大深度
   * @return 构建完成的根Trie节点
   */
  private TrieNode buildTrie(BinaryComparable[] splits, int lower,
          int upper, byte[] prefix, int maxDepth) {
      return buildTrieRec
               (splits, lower, upper, prefix, maxDepth, new CarriedTrieNodeRef());
  }
  
  /**
   * 递归构建Trie树的核心方法，支持复用无分割节点节省内存
   * 按深度优先顺序构建，相邻无分割节点语义相同，复用同一个对象实例
   * @param splits 已排序分割点数组
   * @param lower 当前区间的下界（包含）
   * @param upper 当前区间的上界（不包含）
   * @param prefix 当前已处理的键前缀
   * @param maxDepth Trie树最大深度
   * @param ref 携带可复用无分割节点的引用
   * @return 当前层级构建完成的Trie节点
   */
  private TrieNode buildTrieRec(BinaryComparable[] splits, int lower,
      int upper, byte[] prefix, int maxDepth, CarriedTrieNodeRef ref) {
    final int depth = prefix.length;
    // 达到最大深度或区间分割点少于2个，生成叶子节点
    if (depth >= maxDepth || lower >= upper - 1) {
        // 可复用已有无分割节点
        if (lower == upper && ref.content != null) {
            return ref.content;
        }
        TrieNode  result = LeafTrieNodeFactory(depth, splits, lower, upper);
        // 如果是无分割节点，保存供后续复用
        ref.content = lower == upper ? result : null;
        return result;
    }
    // 创建当前层级内部节点
    InnerTrieNode result = new InnerTrieNode(depth);
    byte[] trial = Arrays.copyOf(prefix, prefix.length + 1);
    // 按字节值遍历所有可能的子节点
    int         currentBound = lower;
    for(int ch = 0; ch < 0xFF; ++ch) {
      trial[depth] = (byte) (ch + 1);
      lower = currentBound;
      // 找到当前字节范围对应的分割点区间
      while (currentBound < upper) {
        if (splits[currentBound].compareTo(trial, 0, trial.length) >= 0) {
          break;
        }
        currentBound += 1;
      }
      trial[depth] = (byte) ch;
      // 递归构建当前字节对应子节点
      result.child[0xFF & ch]
                   = buildTrieRec(splits, lower, currentBound, trial, maxDepth, ref);
    }
    // 处理最后一个字节0xFF的情况
    trial[depth] = (byte)0xFF;
    result.child[0xFF] 
                 = buildTrieRec(splits, lower, currentBound, trial, maxDepth, ref);
    
    return result;
  }
}