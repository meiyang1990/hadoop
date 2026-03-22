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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.MapFile;

/** 
 * Hadoop旧版MapReduce API的SequenceFile输入格式实现，用于从SequenceFile文件中读取输入数据。
 * 支持直接读取SequenceFile文件，也自动适配MapFile目录，读取其中的数据文件。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileInputFormat<K, V> extends FileInputFormat<K, V> {

  /**
   * 构造函数，初始化SequenceFile输入格式，设置最小分片大小为SequenceFile的同步间隔。
   * 该设置保证分片不会切断SequenceFile的同步块，保证分片可以独立被读取。
   */
  public SequenceFileInputFormat() {
    setMinSplitSize(SequenceFile.SYNC_INTERVAL);
  }
  
  @Override
  /**
   * 获取输入文件列表，处理MapFile目录将其替换为实际的数据文件。
   * @param job 作业配置对象
   * @return 处理后的输入文件状态数组
   * @throws IOException 文件系统操作异常
   */
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    FileStatus[] files = super.listStatus(job);
    // 遍历所有输入路径，处理目录类型路径（MapFile结构）
    for (int i = 0; i < files.length; i++) {
      FileStatus file = files[i];
      if (file.isDirectory()) {     // 当前目录是MapFile结构
        // 构造MapFile数据文件路径
        Path dataFile = new Path(file.getPath(), MapFile.DATA_FILE_NAME);
        FileSystem fs = file.getPath().getFileSystem(job);
        // 将目录替换为实际的数据文件，读取该数据文件作为输入
        files[i] = fs.getFileStatus(dataFile);
      }
    }
    return files;
  }

  /**
   * 创建对应输入分片的记录读取器，用于从SequenceFile分片中读取键值对记录。
   * @param split 输入分片
   * @param job 作业配置
   * @param reporter 进度报告器
   * @return SequenceFile记录读取器实例
   * @throws IOException 创建读取器过程中IO异常
   */
  public RecordReader<K, V> getRecordReader(InputSplit split,
                                      JobConf job, Reporter reporter)
    throws IOException {

    // 报告分片读取状态
    reporter.setStatus(split.toString());

    // 创建并返回SequenceFile专属的记录读取器
    return new SequenceFileRecordReader<K, V>(job, (FileSplit) split);
  }

}