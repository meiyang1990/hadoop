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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * 文件输入格式的抽象基类，用于生成多文件分片（MultiFileSplit）。
 * 该类将多个输入文件组合成大小相近的输入分片，每个分片可包含多个完整文件，
 * 适用于处理大量小文件的场景，减少分片数量提升Map阶段执行效率。
 * 子类需要实现getRecordReader方法为多文件分片提供记录读取器。
 * @see MultiFileSplit
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class MultiFileInputFormat<K, V>
  extends FileInputFormat<K, V> {

  /**
   * 生成多文件输入分片，将输入文件组合为大小近似均衡的分片。
   * @param job 作业配置对象
   * @param numSplits 期望生成的分片数量
   * @return 生成的输入分片数组
   * @throws IOException 读取文件信息时发生IO异常
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) 
    throws IOException {
    
    // 获取所有输入文件路径
    Path[] paths = FileUtil.stat2Paths(listStatus(job));
    // 初始化分片列表，容量取期望分片数和文件数的较小值
    List<MultiFileSplit> splits = new ArrayList<MultiFileSplit>(Math.min(numSplits, paths.length));
    if (paths.length != 0) {
      // 仅当存在输入文件时才生成分片
      // 存储每个文件的长度
      long[] lengths = new long[paths.length];
      // 总输入数据长度
      long totLength = 0;
      for(int i=0; i<paths.length; i++) {
        // 获取文件所在文件系统
        FileSystem fs = paths[i].getFileSystem(job);
        // 获取文件长度并累加到总长度
        lengths[i] = fs.getContentSummary(paths[i]).getLength();
        totLength += lengths[i];
      }
      // 计算每个分片期望的平均长度
      double avgLengthPerSplit = ((double)totLength) / numSplits;
      // 当前累计已分配的数据总长度
      long cumulativeLength = 0;

      // 当前分片起始文件索引
      int startIndex = 0;

      for(int i=0; i<numSplits; i++) {
        // 计算当前分片需要包含多少个文件
        int splitSize = findSize(i, avgLengthPerSplit, cumulativeLength
            , startIndex, lengths);
        if (splitSize != 0) {
          // 仅当分片非空时添加到分片列表
          // 初始化分片所需路径数组
          Path[] splitPaths = new Path[splitSize];
          // 初始化分片所需文件长度数组
          long[] splitLengths = new long[splitSize];
          // 从总路径数组拷贝当前分片的路径
          System.arraycopy(paths, startIndex, splitPaths , 0, splitSize);
          // 从总长度数组拷贝当前分片的文件长度
          System.arraycopy(lengths, startIndex, splitLengths , 0, splitSize);
          // 创建多文件分片并添加到分片列表
          splits.add(new MultiFileSplit(job, splitPaths, splitLengths));
          // 更新下一个分片的起始索引
          startIndex += splitSize;
          // 更新累计已分配数据长度
          for(long l: splitLengths) {
            cumulativeLength += l;
          }
        }
      }
    }
    // 返回分片数组
    return splits.toArray(new MultiFileSplit[splits.size()]);    
  }

  /**
   * 计算当前分片需要包含的文件数量，使分片长度尽可能接近平均长度。
   * @param splitIndex 当前分片索引
   * @param avgLengthPerSplit 每个分片的期望平均长度
   * @param cumulativeLength 当前已累计分配的数据总长度
   * @param startIndex 当前分片起始文件在总文件数组中的索引
   * @param lengths 所有输入文件的长度数组
   * @return 当前分片包含的文件数量
   */
  private int findSize(int splitIndex, double avgLengthPerSplit
      , long cumulativeLength , int startIndex, long[] lengths) {
    // 如果是最后一个分片，返回剩余所有文件
    if(splitIndex == lengths.length - 1)
      return lengths.length - startIndex;
    
    // 当前分片结束位置的目标总长度
    long goalLength = (long)((splitIndex + 1) * avgLengthPerSplit);
    // 当前分片累计长度
    long partialLength = 0;
    // 累加文件直到达到目标长度，返回当前分片包含的文件数
    for(int i = startIndex; i < lengths.length; i++) {
      partialLength += lengths[i];
      if(partialLength + cumulativeLength >= goalLength) {
        return i - startIndex + 1;
      }
    }
    // 未达到目标长度时返回剩余所有文件
    return lengths.length - startIndex;
  }
  
  /**
   * 为多文件分片创建记录读取器，由子类实现具体读取逻辑。
   * @param split 输入分片
   * @param job 作业配置
   * @param reporter 进度汇报器
   * @return 多文件分片的记录读取器
   * @throws IOException 创建读取器时发生IO异常
   */
  @Override
  public abstract RecordReader<K, V> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter)
      throws IOException;
}