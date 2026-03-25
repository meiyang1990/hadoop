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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.NetworkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.HashMultiset;
import org.apache.hadoop.thirdparty.com.google.common.collect.Multiset;

/**
 * 文件输入格式抽象实现，通过将多个小文件/小块合并为大输入分片，减少Map任务数量提升小文件处理效率
 * 重写getSplits方法返回CombineFileSplit，支持数据本地化优化，优先将同一节点/同一机架的块合并到同一个分片中
 * 
 * 核心分片规则：
 * 1. 同一个分片中不能包含来自不同池的文件
 * 2. 如果指定了maxSplitSize，同一节点上的块会优先合并成单个分片，剩余不足最小节点分片会合并同一机架的剩余块
 * 3. 如果未指定maxSplitSize，则直接合并同一机架的所有块，不强制创建节点本地分片
 * 4. 如果maxSplitSize等于块大小，行为和Hadoop默认分片行为一致
 * 子类需要实现createRecordReader方法为CombineFileSplit提供记录读取器
 * 
 * @see CombineFileSplit
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class CombineFileInputFormat<K, V>
  extends FileInputFormat<K, V> {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(CombineFileInputFormat.class);
  
  public static final String SPLIT_MINSIZE_PERNODE = 
    "mapreduce.input.fileinputformat.split.minsize.per.node";
  public static final String SPLIT_MINSIZE_PERRACK = 
    "mapreduce.input.fileinputformat.split.minsize.per.rack";
  // ability to limit the size of a single split
  private long maxSplitSize = 0;
  private long minSplitSizeNode = 0;
  private long minSplitSizeRack = 0;

  // A pool of input paths filters. A split cannot have blocks from files
  // across multiple pools.
  private ArrayList<MultiPathFilter> pools = new  ArrayList<MultiPathFilter>();

  // mapping from a rack name to the set of Nodes in the rack 
  private HashMap<String, Set<String>> rackToNodes = 
                            new HashMap<String, Set<String>>();
  /**
   * 设置单个分片的最大字节数
   * @param maxSplitSize 分片最大字节数
   */
  protected void setMaxSplitSize(long maxSplitSize) {
    this.maxSplitSize = maxSplitSize;
  }

  /**
   * 设置每个节点上分片最小字节数，节点本地合并后剩余块总大小超过该值则单独生成分片，否则放回全局合并
   * @param minSplitSizeNode 节点分片最小字节数
   */
  protected void setMinSplitSizeNode(long minSplitSizeNode) {
    this.minSplitSizeNode = minSplitSizeNode;
  }

  /**
   * 设置每个机架上分片最小字节数，机架合并后剩余块总大小超过该值则单独生成分片
   * @param minSplitSizeRack 机架分片最小字节数
   */
  protected void setMinSplitSizeRack(long minSplitSizeRack) {
    this.minSplitSizeRack = minSplitSizeRack;
  }

  /**
   * 创建新的输入池，使用指定过滤器列表过滤路径
   * 同一个分片中不会包含不同池中的文件
   * @param filters 过滤器列表
   */
  protected void createPool(List<PathFilter> filters) {
    pools.add(new MultiPathFilter(filters));
  }

  /**
   * 创建新的输入池，使用可变参数指定过滤器
   * 同一个分片中不会包含不同池中的文件，路径只要满足任意一个过滤器即可加入池
   * @param filters 过滤器可变参数
   */
  protected void createPool(PathFilter... filters) {
    MultiPathFilter multi = new MultiPathFilter();
    for (PathFilter f: filters) {
      multi.add(f);
    }
    pools.add(multi);
  }
  
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    final CompressionCodec codec =
      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }

  /**
   * 默认构造函数
   */
  public CombineFileInputFormat() {
  }

  @Override
  /**
   * 为作业生成合并后的输入分片列表，遵循数据本地化优先的分片规则
   * @param job 作业上下文
   * @return 生成的合并输入分片列表
   * @throws IOException IO异常
   */
  public List<InputSplit> getSplits(JobContext job) 
    throws IOException {
    long minSizeNode = 0;
    long minSizeRack = 0;
    long maxSize = 0;
    Configuration conf = job.getConfiguration();

    // the values specified by setxxxSplitSize() takes precedence over the
    // values that might have been specified in the config
    // 代码设置的分片大小配置优先级高于配置文件
    if (minSplitSizeNode != 0) {
      minSizeNode = minSplitSizeNode;
    } else {
      minSizeNode = conf.getLong(SPLIT_MINSIZE_PERNODE, 0);
    }
    if (minSplitSizeRack != 0) {
      minSizeRack = minSplitSizeRack;
    } else {
      minSizeRack = conf.getLong(SPLIT_MINSIZE_PERRACK, 0);
    }
    if (maxSplitSize != 0) {
      maxSize = maxSplitSize;
    } else {
      maxSize = conf.getLong("mapreduce.input.fileinputformat.split.maxsize", 0);
      // If maxSize is not configured, a single split will be generated per
      // node.
    }
    // 参数合法性检查：节点最小分片不能大于最大分片
    if (minSizeNode != 0 && maxSize != 0 && minSizeNode > maxSize) {
      throw new IOException("Minimum split size pernode " + minSizeNode +
                            " cannot be larger than maximum split size " +
                            maxSize);
    }
    // 参数合法性检查：机架最小分片不能大于最大分片
    if (minSizeRack != 0 && maxSize != 0 && minSizeRack > maxSize) {
      throw new IOException("Minimum split size per rack " + minSizeRack +
                            " cannot be larger than maximum split size " +
                            maxSize);
    }
    // 参数合法性检查：节点最小分片不能大于机架最小分片
    if (minSizeRack != 0 && minSizeNode > minSizeRack) {
      throw new IOException("Minimum split size per node " + minSizeNode +
                            " cannot be larger than minimum split " +
                            "size per rack " + minSizeRack);
    }

    // 获取所有输入文件状态
    List<FileStatus> stats = listStatus(job);
    List<InputSplit> splits = new ArrayList<InputSplit>();
    if (stats.size() == 0) {
      return splits;    
    }

    // 逐池处理，保证一个分片仅包含同一个池的文件
    // In one single iteration, process all the paths in a single pool.
    // Processing one pool at a time ensures that a split contains paths
    // from a single pool only.
    for (MultiPathFilter onepool : pools) {
      ArrayList<FileStatus> myPaths = new ArrayList<FileStatus>();
      
      // pick one input path. If it matches all the filters in a pool,
      // add it to the output set
      for (Iterator<FileStatus> iter = stats.iterator(); iter.hasNext();) {
        FileStatus p = iter.next();
        if (onepool.accept(p.getPath())) {
          myPaths.add(p); // add it to my output set
          iter.remove();
        }
      }
      // 为当前池生成分片
      getMoreSplits(job, myPaths, maxSize, minSizeNode, minSizeRack, splits);
    }

    // 为不属于任何池的剩余文件生成分片
    getMoreSplits(job, stats, maxSize, minSizeNode, minSizeRack, splits);

    // 清空机架节点映射，释放内存
    rackToNodes.clear();
    return splits;    
  }

  /**
   * 为指定路径集合生成合并分片，初始化块位置信息，调用createSplits执行分片逻辑
   * @param job 作业上下文
   * @param stats 当前池的文件状态列表
   * @param maxSize 分片最大大小
   * @param minSizeNode 节点分片最小大小
   * @param minSizeRack 机架分片最小大小
   * @param splits 输出分片列表
   * @throws IOException IO异常
   */
  private void getMoreSplits(JobContext job, List<FileStatus> stats,
                             long maxSize, long minSizeNode, long minSizeRack,
                             List<InputSplit> splits)
    throws IOException {
    Configuration conf = job.getConfiguration();

    // all blocks for all the files in input set
    OneFileInfo[] files;
  
    // mapping from a rack name to the list of blocks it has
    HashMap<String, List<OneBlockInfo>> rackToBlocks = 
                              new HashMap<String, List<OneBlockInfo>>();

    // mapping from a block to the nodes on which it has replicas
    HashMap<OneBlockInfo, String[]> blockToNodes = 
                              new HashMap<OneBlockInfo, String[]>();

    // mapping from a node to the list of blocks that it contains
    HashMap<String, Set<OneBlockInfo>> nodeToBlocks = 
                              new HashMap<String, Set<OneBlockInfo>>();
    
    files = new OneFileInfo[stats.size()];
    if (stats.size() == 0) {
      return; 
    }

    // 遍历所有文件，收集块信息并构建各级索引
    long totLength = 0;
    int i = 0;
    for (FileStatus stat : stats) {
      files[i] = new OneFileInfo(stat, conf, isSplitable(job, stat.getPath()),
                                 rackToBlocks, blockToNodes, nodeToBlocks,
                                 rackToNodes, maxSize);
      totLength += files[i].getLength();
      i++;
    }
    // 执行分片生成
    createSplits(nodeToBlocks, blockToNodes, rackToBlocks, totLength, 
                 maxSize, minSizeNode, minSizeRack, splits);
  }

  /**
   * 按节点本地优先的策略生成合并分片，先节点本地合并，再机架合并，最后处理溢出块
   * 循环遍历节点分散生成分片，平衡节点间分片数量，实现数据本地性优化
   * @param nodeToBlocks 节点到该节点包含块集合的映射
   * @param blockToNodes 块到该块所在节点数组的映射
   * @param rackToBlocks 机架到该机架包含块列表的映射
   * @param totLength 输入文件总长度
   * @param maxSize 每个分片最大大小，为0表示每个节点生成一个分片
   * @param minSizeNode 节点分片最小大小
   * @param minSizeRack 机架分片最小大小
   * @param splits 输出分片列表
   */
  @VisibleForTesting
  void createSplits(Map<String, Set<OneBlockInfo>> nodeToBlocks,
                     Map<OneBlockInfo, String[]> blockToNodes,
                     Map<String, List<OneBlockInfo>> rackToBlocks,
                     long totLength,
                     long maxSize,
                     long minSizeNode,
                     long minSizeRack,
                     List<InputSplit> splits                     
                    ) {
    ArrayList<OneBlockInfo> validBlocks = new ArrayList<OneBlockInfo>();
    long curSplitSize = 0;
    
    int totalNodes = nodeToBlocks.size();
    long totalLength = totLength;

    // 记录每个节点已生成的分片数量，用于负载均衡
    Multiset<String> splitsPerNode = HashMultiset.create();
    // 标记已处理完成的节点
    Set<String> completedNodes = new HashSet<String>();
    
    // 外层循环不断遍历节点，直到所有节点处理完成
    while(true) {
      for (Iterator<Map.Entry<String, Set<OneBlockInfo>>> iter = nodeToBlocks
          .entrySet().iterator(); iter.hasNext();) {
        Map.Entry<String, Set<OneBlockInfo>> one = iter.next();
        
        String node = one.getKey();
        
        // 跳过已处理完成的节点
        if (completedNodes.contains(node)) {
          continue;
        }

        Set<OneBlockInfo> blocksInCurrentNode = one.getValue();

        // for each block, copy it into validBlocks. Delete it from
        // blockToNodes so that the same block does not appear in
        // two different splits.
        Iterator<OneBlockInfo> oneBlockIter = blocksInCurrentNode.iterator();
        // 遍历当前节点未分配块
        while (oneBlockIter.hasNext()) {
          OneBlockInfo oneblock = oneBlockIter.next();
          
          // 移除已分配给其他分片的块
          if(!blockToNodes.containsKey(oneblock)) {
            oneBlockIter.remove();
            continue;
          }
        
          // 添加块到当前分片，从全局映射中移除，避免重复分配
          validBlocks.add(oneblock);
          blockToNodes.remove(oneblock);
          curSplitSize += oneblock.length;

          // 累加大小达到最大分片限制，生成新分片
          if (maxSize != 0 && curSplitSize >= maxSize) {
            // 创建节点本地分片并加入结果
            addCreatedSplit(splits, Collections.singleton(node), validBlocks);
            totalLength -= curSplitSize;
            curSplitSize = 0;

            splitsPerNode.add(node);

            // 从节点移除已分配块，清空当前分片缓存
            blocksInCurrentNode.removeAll(validBlocks);
            validBlocks.clear();

            // 处理完一个分片后切换到下一个节点，平衡分片分布
            break;
          }

        }
        // 当前轮次该节点还有未分配块
        if (validBlocks.size() != 0) {
          // 检查剩余块大小是否满足节点分片最小要求，且该节点还未生成过分片
          if (minSizeNode != 0 && curSplitSize >= minSizeNode
              && splitsPerNode.count(node) == 0) {
            // 生成节点本地分片
            addCreatedSplit(splits, Collections.singleton(node), validBlocks);
            totalLength -= curSplitSize;
            splitsPerNode.add(node);
            blocksInCurrentNode.removeAll(validBlocks);
          } else {
            // 不满足要求，将块放回全局映射，后续进行机架级合并
            for (OneBlockInfo oneblock : validBlocks) {
              blockToNodes.put(oneblock, oneblock.hosts);
            }
          }
          // 清空当前分片缓存，标记该节点处理完成
          validBlocks.clear();
          curSplitSize = 0;
          completedNodes.add(node);
        } else { // No in-flight blocks.
          // 当前节点没有未分配块，标记完成
          if (blocksInCurrentNode.size() == 0) {
            completedNodes.add(node);
          } // else 未处理完，下一轮继续处理
        }
      }

      // 节点本地分配完成，退出循环，进入机架级分配
      if (completedNodes.size() == totalNodes || totalLength == 0) {
        LOG.debug("Terminated node allocation with : CompletedNodes: {}, size left: {}",
            completedNodes.size(), totalLength);
        break;