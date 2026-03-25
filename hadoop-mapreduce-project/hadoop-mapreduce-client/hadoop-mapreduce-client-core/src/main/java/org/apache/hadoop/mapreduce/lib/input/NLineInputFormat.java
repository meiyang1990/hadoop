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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.functional.FutureIO;

/**
 * NLineInputFormat输入格式，按N行划分输入分片，每个分片交给一个Map任务处理。
 * 
 * 适用于参数扫描等场景：控制文件中每行保存一组参数，默认每一行作为一个Map任务的输入，
 * 每个Map任务使用对应参数处理同一个数据集，实现多参数并行处理。
 * 输出键为行在文件中的偏移量（LongWritable），值为行内容（Text）。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class NLineInputFormat extends FileInputFormat<LongWritable, Text> { 
  /** 配置项：每个分片包含的行数 */
  public static final String LINES_PER_MAP = 
    "mapreduce.input.lineinputformat.linespermap";

  /**
   * 创建记录读取器，用于读取分片中的行记录
   * @param genericSplit 输入分片
   * @param context 任务尝试上下文
   * @return 行记录读取器实例
   * @throws IOException 读取失败时抛出IO异常
   */
  public RecordReader<LongWritable, Text> createRecordReader(
      InputSplit genericSplit, TaskAttemptContext context) 
      throws IOException {
    context.setStatus(genericSplit.toString());
    return new LineReader();
  }

  /** 
   * 对输入文件进行逻辑分片，每N行划分为一个输入分片
   * @param job 作业上下文
   * @return 生成的输入分片列表
   * @throws IOException 读取文件信息失败时抛出IO异常
   */
  public List<InputSplit> getSplits(JobContext job)
  throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    int numLinesPerSplit = getNumLinesPerSplit(job);
    for (FileStatus status : listStatus(job)) {
      splits.addAll(getSplitsForFile(status,
        job.getConfiguration(), numLinesPerSplit));
    }
    return splits;
  }
  
  /**
   * 对单个文件按指定行数进行分片处理
   * @param status 目标文件状态信息
   * @param conf 作业配置
   * @param numLinesPerSplit 每个分片包含的行数
   * @return 该文件生成的分片列表
   * @throws IOException 读取文件失败时抛出IO异常
   */
  public static List<FileSplit> getSplitsForFile(FileStatus status,
      Configuration conf, int numLinesPerSplit) throws IOException {
    List<FileSplit> splits = new ArrayList<FileSplit> ();
    Path fileName = status.getPath();
    if (status.isDirectory()) {
      throw new IOException("Not a file: " + fileName);
    }
    LineReader lr = null;
    try {
      final FutureDataInputStreamBuilder builder =
          fileName.getFileSystem(conf).openFile(fileName);
      // 传播输入文件相关配置项到输入流构建器
      FutureIO.propagateOptions(builder, conf,
          MRJobConfig.INPUT_FILE_OPTION_PREFIX,
          MRJobConfig.INPUT_FILE_MANDATORY_PREFIX);
      // 等待异步输入流构建完成
      FSDataInputStream in  = FutureIO.awaitFuture(builder.build());
      lr = new LineReader(in, conf);
      Text line = new Text();
      // 当前分片已读取行数
      int numLines = 0;
      // 当前分片起始字节偏移
      long begin = 0;
      // 当前分片已累积字节长度
      long length = 0;
      // 当前读取行的字节数
      int num = -1;
      // 逐行读取文件，按行数划分分片
      while ((num = lr.readLine(line)) > 0) {
        numLines++;
        length += num;
        // 达到每个分片指定行数，创建新分片
        if (numLines == numLinesPerSplit) {
          splits.add(createFileSplit(fileName, begin, length));
          // 更新下一个分片的起始偏移
          begin += length;
          // 重置计数器
          length = 0;
          numLines = 0;
        }
      }
      // 处理最后不足指定行数的剩余内容，创建分片
      if (numLines != 0) {
        splits.add(createFileSplit(fileName, begin, length));
      }
    } finally {
      // 确保关闭行读取器
      if (lr != null) {
        lr.close();
      }
    }
    return splits; 
  }

  /**
   * 调整分片边界，保证每个Mapper确实能读到指定行数。
   * 因为LineRecordReader会跨过分片边界读取至少一个字符，因此需要将分片结束位置回退一个字符，
   * 避免不同分片读取到重复行。
   * @param fileName 分片所属文件路径
   * @param begin 分片起始字节偏移
   * @param length 分片总字节数
   * @return 调整边界后的文件分片
   */
  protected static FileSplit createFileSplit(Path fileName, long begin, long length) {
    return (begin == 0) 
    ? new FileSplit(fileName, begin, length - 1, new String[] {})
    : new FileSplit(fileName, begin - 1, length, new String[] {});
  }
  
  /**
   * 设置每个分片包含的行数
   * @param job 目标作业对象
   * @param numLines 每个分片的行数
   */
  public static void setNumLinesPerSplit(Job job, int numLines) {
    job.getConfiguration().setInt(LINES_PER_MAP, numLines);
  }

  /**
   * 获取配置中每个分片包含的行数，默认值为1
   * @param job 作业上下文
   * @return 每个分片的行数
   */
  public static int getNumLinesPerSplit(JobContext job) {
    return job.getConfiguration().getInt(LINES_PER_MAP, 1);
  }
}