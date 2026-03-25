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

package org.apache.hadoop.mapreduce.split;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 * 文件级注释：MapReduce作业输入分片元信息读取工具类，用于从作业提交目录读取分片元数据，
 * 反序列化生成分片元信息对象，供作业执行时获取输入分片信息。
 *
 * A utility that reads the split meta info and creates
 * split meta info objects
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SplitMetaInfoReader {
  
  /**
   * 从作业提交目录读取输入分片元信息，反序列化生成分片元信息数组
   * @param jobId 作业ID
   * @param fs 文件系统对象，用于访问作业提交目录
   * @param conf 作业配置对象
   * @param jobSubmitDir 作业提交目录路径
   * @return 输入分片元信息数组
   * @throws IOException 读取文件、校验失败时抛出IO异常
   */
  public static JobSplit.TaskSplitMetaInfo[] readSplitMetaInfo(
      JobID jobId, FileSystem fs, Configuration conf, Path jobSubmitDir) 
  throws IOException {
    // 从配置获取分片元信息最大允许大小，使用默认值作为兜底
    long maxMetaInfoSize = conf.getLong(MRJobConfig.SPLIT_METAINFO_MAXSIZE,
        MRJobConfig.DEFAULT_SPLIT_METAINFO_MAXSIZE);
    // 获取分片元信息文件路径
    Path metaSplitFile = JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir);
    // 获取分片数据文件路径字符串，存入分片索引
    String jobSplitFile = JobSubmissionFiles.getJobSplitFile(jobSubmitDir).toString();
    // 获取元信息文件状态，检查文件大小
    FileStatus fStatus = fs.getFileStatus(metaSplitFile);
    // 校验元信息文件大小不超过限制，超过则抛出异常终止作业
    if (maxMetaInfoSize > 0 && fStatus.getLen() > maxMetaInfoSize) {
      throw new IOException("Split metadata size exceeded " +
          maxMetaInfoSize +". Aborting job " + jobId);
    }
    // 打开元信息文件输入流
    FSDataInputStream in = fs.open(metaSplitFile);
    // 分配字节数组存储文件头
    byte[] header = new byte[JobSplit.META_SPLIT_FILE_HEADER.length];
    // 读取完整文件头
    in.readFully(header);
    // 校验文件头是否正确，不正确则抛出异常
    if (!Arrays.equals(JobSplit.META_SPLIT_FILE_HEADER, header)) {
      throw new IOException("Invalid header on split file");
    }
    // 读取版本号
    int vers = WritableUtils.readVInt(in);
    // 校验版本号是否匹配，不匹配则关闭流后抛出异常
    if (vers != JobSplit.META_SPLIT_VERSION) {
      in.close();
      throw new IOException("Unsupported split version " + vers);
    }
    // 读取分片总数
    int numSplits = WritableUtils.readVInt(in); //TODO: check for insane values
    // 创建分片元信息数组
    JobSplit.TaskSplitMetaInfo[] allSplitMetaInfo = 
      new JobSplit.TaskSplitMetaInfo[numSplits];
    // 遍历读取每个分片的元信息
    for (int i = 0; i < numSplits; i++) {
      // 创建分片元信息对象
      JobSplit.SplitMetaInfo splitMetaInfo = new JobSplit.SplitMetaInfo();
      // 从输入流反序列化读取元信息
      splitMetaInfo.readFields(in);
      // 创建分片索引，记录分片数据文件路径和偏移量
      JobSplit.TaskSplitIndex splitIndex = new JobSplit.TaskSplitIndex(
          jobSplitFile, 
          splitMetaInfo.getStartOffset());
      // 组装任务分片元信息对象存入数组
      allSplitMetaInfo[i] = new JobSplit.TaskSplitMetaInfo(splitIndex, 
          splitMetaInfo.getLocations(), 
          splitMetaInfo.getInputDataLength());
    }
    // 关闭输入流
    in.close();
    // 返回分片元信息数组
    return allSplitMetaInfo;
  }

}