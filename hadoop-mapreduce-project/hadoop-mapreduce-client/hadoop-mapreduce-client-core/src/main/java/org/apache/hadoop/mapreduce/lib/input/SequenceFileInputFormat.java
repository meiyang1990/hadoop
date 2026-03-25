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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 文件级注释：SequenceFile 格式文件的 MapReduce 输入格式实现，支持从 SequenceFile 中读取输入数据
 * 同时兼容处理 MapFile 目录，自动将其转换为对应的数据文件处理
 * An {@link InputFormat} for {@link SequenceFile}s.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileInputFormat<K, V> extends FileInputFormat<K, V> {

  /**
   * 创建 SequenceFile 对应的记录读取器，用于从输入分片读取键值对记录
   * @param split 输入分片
   * @param context 任务尝试上下文
   * @return 针对 SequenceFile 的记录读取器实例
   * @throws IOException 创建读取器失败时抛出IO异常
   */
  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split,
                                               TaskAttemptContext context
                                               ) throws IOException {
    return new SequenceFileRecordReader<K,V>();
  }

  /**
   * 获取当前输入格式要求的最小分片大小，使用 SequenceFile 的同步间隔作为最小值
   * 保证分片不会切分到同步点中间，确保输入可以正确拆分
   * @return 最小分片大小，即SequenceFile的同步间隔
   */
  @Override
  protected long getFormatMinSplitSize() {
    return SequenceFile.SYNC_INTERVAL;
  }

  /**
   * 获取输入文件列表，处理 MapFile 目录，将目录替换为其内部实际存储数据的 data 文件
   * @param job 作业上下文
   * @return 处理后的输入文件状态列表
   * @throws IOException 获取文件状态失败时抛出IO异常
   */
  @Override
  protected List<FileStatus> listStatus(JobContext job
                                        )throws IOException {

    List<FileStatus> files = super.listStatus(job);
    int len = files.size();
    for(int i=0; i < len; ++i) {
      FileStatus file = files.get(i);
      if (file.isDirectory()) {     // 当前输入项是MapFile目录
        Path p = file.getPath();
        FileSystem fs = p.getFileSystem(job.getConfiguration());
        // 替换为MapFile内部实际存储数据的数据文件，后续直接读取该文件
        files.set(i, fs.getFileStatus(new Path(p, MapFile.DATA_FILE_NAME)));
      }
    }
    return files;
  }
}