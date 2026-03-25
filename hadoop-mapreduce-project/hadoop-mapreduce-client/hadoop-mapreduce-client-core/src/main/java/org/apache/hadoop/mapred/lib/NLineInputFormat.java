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
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * NLine输入格式，将输入文件按N行切分为一个输入分片。
 * 
 * 在很多可高度并行化的应用场景中，每个Mapper进程处理同一个输入数据集，
 * 但需要使用不同的参数进行计算（常被称为"参数扫描"场景）。
 * 实现该需求的一种方式是：将一组参数（每行一组）写入控制文件作为MapReduce作业的输入，
 * 而实际处理的数据集则通过JobConf中的配置变量指定。
 * 
 * NLineInputFormat专门适配此类场景：默认情况下，将输入文件每一行切分为一个分片，
 * 每个分片对应一个Map任务，输出键为行偏移量（LongWritable），值为行内容（Text）。
 * 分片可分布到整个集群节点并行处理。
 * 本类是旧版MapReduce API的实现，对应新版API为org.apache.hadoop.mapreduce.lib.input.NLineInputFormat。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class NLineInputFormat extends FileInputFormat<LongWritable, Text> 
                              implements JobConfigurable { 
  private int N = 1;

  /**
   * 获取当前输入分片的记录读取器，用于读取分片中的行记录
   * @param genericSplit 待读取的输入分片
   * @param job 作业配置对象
   * @param reporter 进度报告器
   * @return 按行读取的LineRecordReader实例
   * @throws IOException 读取文件发生IO异常时抛出
   */
  public RecordReader<LongWritable, Text> getRecordReader(
                                            InputSplit genericSplit,
                                            JobConf job,
                                            Reporter reporter) 
  throws IOException {
    reporter.setStatus(genericSplit.toString());
    return new LineRecordReader(job, (FileSplit) genericSplit);
  }

  /** 
   * 对作业输入文件进行逻辑切分，将每N行切为一个分片
   * @param job 作业配置对象
   * @param numSplits 期望切分的分片数量（本实现不依赖该参数，按N行规则切分）
   * @return 切分完成的输入分片数组
   * @throws IOException 读取文件状态发生IO异常时抛出
   */
  public InputSplit[] getSplits(JobConf job, int numSplits)
  throws IOException {
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>();
    // 遍历所有输入文件
    for (FileStatus status : listStatus(job)) {
      // 调用新版API实现对当前文件按N行切分
      for (org.apache.hadoop.mapreduce.lib.input.FileSplit split : 
          org.apache.hadoop.mapreduce.lib.input.
          NLineInputFormat.getSplitsForFile(status, job, N)) {
        // 将新版分片转换为旧版API的分片格式添加到结果列表
        splits.add(new FileSplit(split));
      }
    }
    return splits.toArray(new FileSplit[splits.size()]);
  }

  /**
   * 从作业配置中读取每个分片包含的行数N，初始化输入格式
   * @param conf 作业配置对象
   */
  public void configure(JobConf conf) {
    N = conf.getInt("mapreduce.input.lineinputformat.linespermap", 1);
  }
  
  /**
   * 创建调整过边界的文件分片，修正LineRecordReader的越界读取问题
   * LineRecordReader在读取时，总会向上分片边界外多读至少一个字符，
   * 为了保证每个Mapper确实只读取N行，需要将每个分片的上边界回退一个字符。
   * @param fileName 分片所属文件路径
   * @param begin 分片起始字节偏移
   * @param length 分片包含的字节长度
   * @return 调整边界后的FileSplit实例
   */
  protected static FileSplit createFileSplit(Path fileName, long begin, long length) {
    return (begin == 0) 
    ? new FileSplit(fileName, begin, length - 1, new String[] {})
    : new FileSplit(fileName, begin - 1, length, new String[] {});
  }
}