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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.split.JobSplit.SplitMetaInfo;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：MapReduce作业切片写入工具类，由作业客户端使用，负责将输入切片的原始数据和元数据写入提交文件
 * 
 * The class that is used by the Job clients to write splits (both the meta
 * and the raw bytes parts)
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobSplitWriter {

  private static final Logger LOG =
      LoggerFactory.getLogger(JobSplitWriter.class);
  private static final int splitVersion = JobSplit.META_SPLIT_VERSION;
  private static final byte[] SPLIT_FILE_HEADER = "SPL".getBytes(StandardCharsets.UTF_8);

  /**
   * 根据输入切片列表创建作业切片文件和切片元数据文件
   * @param jobSubmitDir 作业提交目录路径
   * @param conf 作业配置对象
   * @param fs 目标文件系统
   * @param splits 输入切片列表
   * @throws IOException 文件操作IO异常
   * @throws InterruptedException 中断异常
   */
  @SuppressWarnings("unchecked")
  public static <T extends InputSplit> void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem fs, List<InputSplit> splits) 
  throws IOException, InterruptedException {
    T[] array = (T[]) splits.toArray(new InputSplit[splits.size()]);
    createSplitFiles(jobSubmitDir, conf, fs, array);
  }
  
  /**
   * 根据输入切片数组创建作业切片文件和切片元数据文件
   * @param jobSubmitDir 作业提交目录路径
   * @param conf 作业配置对象
   * @param fs 目标文件系统
   * @param splits 输入切片数组
   * @throws IOException 文件操作IO异常
   * @throws InterruptedException 中断异常
   */
  public static <T extends InputSplit> void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem fs, T[] splits) 
  throws IOException, InterruptedException {
    // 创建切片文件输出流
    FSDataOutputStream out = createFile(fs, 
        JobSubmissionFiles.getJobSplitFile(jobSubmitDir), conf);
    // 写入所有切片原始数据，生成切片元信息数组
    SplitMetaInfo[] info = writeNewSplits(conf, splits, out);
    out.close();
    // 将切片元信息写入元数据文件
    writeJobSplitMetaInfo(fs,JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir), 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION), splitVersion,
        info);
  }
  
  /**
   * 根据旧版API输入切片数组创建作业切片文件和切片元数据文件
   * @param jobSubmitDir 作业提交目录路径
   * @param conf 作业配置对象
   * @param fs 目标文件系统
   * @param splits 旧版API输入切片数组
   * @throws IOException 文件操作IO异常
   */
  public static void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem fs, 
      org.apache.hadoop.mapred.InputSplit[] splits) 
  throws IOException {
    // 创建切片文件输出流
    FSDataOutputStream out = createFile(fs, 
        JobSubmissionFiles.getJobSplitFile(jobSubmitDir), conf);
    // 写入所有旧版切片原始数据，生成切片元信息数组
    SplitMetaInfo[] info = writeOldSplits(splits, out, conf);
    out.close();
    // 将切片元信息写入元数据文件
    writeJobSplitMetaInfo(fs,JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir), 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION), splitVersion,
        info);
  }
  
  /**
   * 创建切片文件，写入文件头并设置副本数
   * @param fs 目标文件系统
   * @param splitFile 切片文件路径
   * @param job 作业配置
   * @return 切片文件输出流
   * @throws IOException 文件操作IO异常
   */
  private static FSDataOutputStream createFile(FileSystem fs, Path splitFile, 
      Configuration job)  throws IOException {
    // 创建切片文件输出流，设置作业文件权限
    FSDataOutputStream out = FileSystem.create(fs, splitFile, 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
    // 从配置获取切片文件副本数，默认10
    int replication = job.getInt(Job.SUBMIT_REPLICATION, 10);
    fs.setReplication(splitFile, (short)replication);
    // 写入切片文件头
    writeSplitHeader(out);
    return out;
  }

  /**
   * 写入切片文件头信息，包含魔数和版本号
   * @param out 输出流
   * @throws IOException 写入IO异常
   */
  private static void writeSplitHeader(FSDataOutputStream out) 
  throws IOException {
    out.write(SPLIT_FILE_HEADER);
    out.writeInt(splitVersion);
  }
  
  /**
   * 将新版API输入切片写入切片文件，生成对应的切片元信息数组
   * @param conf 作业配置
   * @param array 输入切片数组
   * @param out 切片文件输出流
   * @return 切片元信息数组
   * @throws IOException 写入IO异常
   * @throws InterruptedException 中断异常
   */
  @SuppressWarnings("unchecked")
  private static <T extends InputSplit> 
  SplitMetaInfo[] writeNewSplits(Configuration conf, 
      T[] array, FSDataOutputStream out)
  throws IOException, InterruptedException {

    SplitMetaInfo[] info = new SplitMetaInfo[array.length];
    if (array.length != 0) {
      // 获取序列化工厂
      SerializationFactory factory = new SerializationFactory(conf);
      int i = 0;
      // 从配置获取单切片最大位置数，默认限制防止元数据过大
      int maxBlockLocations = conf.getInt(MRConfig.MAX_BLOCK_LOCATIONS_KEY,
          MRConfig.MAX_BLOCK_LOCATIONS_DEFAULT);
      // 记录当前切片在文件中的偏移量
      long offset = out.getPos();
      // 遍历所有切片依次写入
      for(T split: array) {
        long prevCount = out.getPos();
        // 写入切片类名，用于反序列化
        Text.writeString(out, split.getClass().getName());
        // 获取对应切片类的序列化器
        Serializer<T> serializer = 
          factory.getSerializer((Class<T>) split.getClass());
        serializer.open(out);
        // 序列化写入切片对象
        serializer.serialize(split);
        long currCount = out.getPos();
        // 获取切片数据块位置信息
        String[] locations = split.getLocations();
        // 如果位置数超过最大限制，截断并输出警告
        if (locations.length > maxBlockLocations) {
          LOG.warn("Max block location exceeded for split: "
              + split + " splitsize: " + locations.length +
              " maxsize: " + maxBlockLocations);
          locations = Arrays.copyOf(locations, maxBlockLocations);
        }
        // 保存当前切片的元信息
        info[i++] = 
          new JobSplit.SplitMetaInfo( 
              locations, offset,
              split.getLength());
        // 更新下一个切片的起始偏移量
        offset += currCount - prevCount;
      }
    }
    return info;
  }
  
  /**
   * 将旧版API输入切片写入切片文件，生成对应的切片元信息数组
   * @param splits 旧版输入切片数组
   * @param out 切片文件输出流
   * @param conf 作业配置
   * @return 切片元信息数组
   * @throws IOException 写入IO异常
   */
  private static SplitMetaInfo[] writeOldSplits(
      org.apache.hadoop.mapred.InputSplit[] splits,
      FSDataOutputStream out, Configuration conf) throws IOException {
    SplitMetaInfo[] info = new SplitMetaInfo[splits.length];
    if (splits.length != 0) {
      int i = 0;
      // 记录当前切片在文件中的偏移量
      long offset = out.getPos();
      // 从配置获取单切片最大位置数，默认限制防止元数据过大
      int maxBlockLocations = conf.getInt(MRConfig.MAX_BLOCK_LOCATIONS_KEY,
          MRConfig.MAX_BLOCK_LOCATIONS_DEFAULT);
      // 遍历所有切片依次写入
      for(org.apache.hadoop.mapred.InputSplit split: splits) {
        long prevLen = out.getPos();
        // 写入切片类名，用于反序列化
        Text.writeString(out, split.getClass().getName());
        // 旧版Writable格式写入切片
        split.write(out);
        long currLen = out.getPos();
        // 获取切片数据块位置信息
        String[] locations = split.getLocations();
        // 如果位置数超过最大限制，截断并输出警告
        if (locations.length > maxBlockLocations) {
          LOG.warn("Max block location exceeded for split: "
              + split + " splitsize: " + locations.length +
              " maxsize: " + maxBlockLocations);
          locations = Arrays.copyOf(locations,maxBlockLocations);
        }
        // 保存当前切片的元信息
        info[i++] = new JobSplit.SplitMetaInfo( 
            locations, offset,
            split.getLength());
        // 更新下一个切片的起始偏移量
        offset += currLen - prevLen;
      }
    }
    return info;
  }

  /**
   * 将所有切片元信息写入切片元数据文件
   * @param fs 目标文件系统
   * @param filename 元数据文件路径
   * @param p 文件权限
   * @param splitMetaInfoVersion 元数据版本号
   * @param allSplitMetaInfo 所有切片元信息数组
   * @throws IOException 写入IO异常
   */
  private static void writeJobSplitMetaInfo(FileSystem fs, Path filename, 
      FsPermission p, int splitMetaInfoVersion, 
      JobSplit.SplitMetaInfo[] allSplitMetaInfo) 
  throws IOException {
    // write the splits meta-info to a file for the job tracker
    // 创建元数据文件输出流
    FSDataOutputStream out = 
      FileSystem.create(fs, filename, p);
    // 写入元数据文件头魔数
    out.write(JobSplit.META_SPLIT_FILE_HEADER);
    // 写入元数据版本号
    WritableUtils.writeVInt(out, splitMetaInfoVersion);
    // 写入切片总数
    WritableUtils.writeVInt(out, allSplitMetaInfo.length);
    // 依次写入每个切片的元信息
    for (JobSplit.SplitMetaInfo splitMetaInfo : allSplitMetaInfo) {
      splitMetaInfo.write(out);
    }
    out.close();
  }
}