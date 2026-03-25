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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 文件级注释：旧MapReduce API的组合文件输入格式抽象实现，支持将多个小文件合并为单个输入分片，
 * 减少Map任务数量，提升小文件场景下的处理效率。支持按节点、机架位置聚合分片，保持数据本地性。
 * 每个分片只能包含同一个输入池中的文件，可跨多个文件合并块。
 * 子类需要实现getRecordReader方法为合并分片提供记录读取器。
 * 
 * An abstract {@link org.apache.hadoop.mapred.InputFormat} that returns {@link CombineFileSplit}'s
 * in {@link org.apache.hadoop.mapred.InputFormat#getSplits(JobConf, int)} method. 
 * Splits are constructed from the files under the input paths. 
 * A split cannot have files from different pools.
 * Each split returned may contain blocks from different files.
 * If a maxSplitSize is specified, then blocks on the same node are
 * combined to form a single split. Blocks that are left over are
 * then combined with other blocks in the same rack. 
 * If maxSplitSize is not specified, then blocks from the same rack
 * are combined in a single split; no attempt is made to create
 * node-local splits.
 * If the maxSplitSize is equal to the block size, then this class
 * is similar to the default spliting behaviour in Hadoop: each
 * block is a locally processed split.
 * Subclasses implement {@link org.apache.hadoop.mapred.InputFormat#getRecordReader(InputSplit, JobConf, Reporter)}
 * to construct <code>RecordReader</code>'s for <code>CombineFileSplit</code>'s.
 * @see CombineFileSplit
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class CombineFileInputFormat<K, V>
  extends org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat<K, V> 
  implements InputFormat<K, V>{

  /**
   * 构造函数：初始化组合文件输入格式实例
   * default constructor
   */
  public CombineFileInputFormat() {
  }

  /**
   * 为作业生成输入分片，调用新API父类生成分片后转换为旧API的CombineFileSplit格式
   * @param job 作业配置对象
   * @param numSplits 期望分片数量
   * @return 生成的输入分片数组
   * @throws IOException 读取文件信息时抛出IO异常
   */
  public InputSplit[] getSplits(JobConf job, int numSplits) 
    throws IOException {
    // 调用新API父类生成分片列表
    List<org.apache.hadoop.mapreduce.InputSplit> newStyleSplits =
      super.getSplits(Job.getInstance(job));
    // 创建旧API分片数组存储转换结果
    InputSplit[] ret = new InputSplit[newStyleSplits.size()];
    // 遍历转换每个新API分片为旧API格式
    for(int pos = 0; pos < newStyleSplits.size(); ++pos) {
      org.apache.hadoop.mapreduce.lib.input.CombineFileSplit newStyleSplit = 
        (org.apache.hadoop.mapreduce.lib.input.CombineFileSplit) newStyleSplits.get(pos);
      ret[pos] = new CombineFileSplit(job, newStyleSplit.getPaths(),
        newStyleSplit.getStartOffsets(), newStyleSplit.getLengths(),
        newStyleSplit.getLocations());
    }
    return ret;
  }
  
  /**
   * 创建新的输入池，添加路径过滤器到池中。一个分片不能包含不同池的文件。
   * @deprecated 请使用{@link #createPool(List)}方法
   * @param conf 作业配置
   * @param filters 路径过滤器列表
   */
  @Deprecated
  protected void createPool(JobConf conf, List<PathFilter> filters) {
    createPool(filters);
  }

  /**
   * 创建新的输入池，添加路径过滤器到池中。路径只要满足任意一个过滤器即可加入该池。
   * 一个分片不能包含不同池的文件。
   * @deprecated 请使用{@link #createPool(PathFilter...)}方法
   * @param conf 作业配置
   * @param filters 路径过滤器数组
   */
  @Deprecated
  protected void createPool(JobConf conf, PathFilter... filters) {
    createPool(filters);
  }

  /**
   * 为指定输入分片创建记录读取器，需要子类实现
   * @param split 待读取的输入分片
   * @param job 作业配置
   * @param reporter 进度报告器
   * @return 记录读取器实例
   * @throws IOException 读取分片时抛出IO异常
   */
  public abstract RecordReader<K, V> getRecordReader(InputSplit split,
                                      JobConf job, Reporter reporter)
    throws IOException;

  /**
   * 实现父类抽象方法，返回null即可，因为旧API使用getRecordReader而非该方法
   */
  public org.apache.hadoop.mapreduce.RecordReader<K, V> createRecordReader(
      org.apache.hadoop.mapreduce.InputSplit split,
      TaskAttemptContext context) throws IOException {
    return null;
  }
  
  /**
   * 获取输入目录下所有文件的状态，子类可覆盖该方法自定义文件筛选逻辑
   * @param job 作业配置
   * @return 输入文件状态数组
   * @throws IOException 列出文件状态时抛出IO异常，如果没有输入文件也会抛出
   */
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    // 调用父类新API获取文件状态列表
    List<FileStatus> result = super.listStatus(Job.getInstance(job));
    // 转换为数组返回适配旧API接口
    return result.toArray(new FileStatus[result.size()]);
  }

  /**
   * 检查文件是否可分片，兼容旧API接口，转发到isSplitable(FileSystem, Path)方法实现
   * @param context 作业上下文
   * @param file 待检查的文件路径
   * @return 如果文件可分片返回true，否则返回false
   * @see <a href="https://issues.apache.org/jira/browse/MAPREDUCE-5530">
   * MAPREDUCE-5530</a>
   */
  @InterfaceAudience.Private
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    try {
      // 获取文件系统实例，调用重载方法判断是否可分片
      return isSplitable(FileSystem.get(context.getConfiguration()), file);
    }
    catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  /**
   * 判断给定文件是否支持分片处理：未压缩文件可分片，只有可分割压缩格式支持分片
   * @param fs 文件系统实例
   * @param file 待检查的文件路径
   * @return 如果文件可分片返回true，否则返回false
   */
  protected boolean isSplitable(FileSystem fs, Path file) {
    // 获取文件对应的压缩编解码器
    final CompressionCodec codec =
      new CompressionCodecFactory(fs.getConf()).getCodec(file);
    // 未压缩文件直接返回可分片
    if (null == codec) {
      return true;
    }
    // 只有实现了SplittableCompressionCodec的压缩格式才支持分片
    return codec instanceof SplittableCompressionCodec;
  }
}