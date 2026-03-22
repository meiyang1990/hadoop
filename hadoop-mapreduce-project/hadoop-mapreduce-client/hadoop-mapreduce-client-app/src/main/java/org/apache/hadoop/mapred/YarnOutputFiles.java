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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRConfig;

/**
 * YARN环境下MapReduce任务中间输出文件路径管理类
 * 
 * 该类负责管理Map和Reduce任务在YARN运行时本地临时存储目录的路径生成，
 * 为Map输出、溢写文件、Reduce输入等中间文件提供路径创建和查找能力，
 * 供任务运行时在本地磁盘读写中间数据使用。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class YarnOutputFiles extends MapOutputFile {

  private JobConf conf;

  private static final String JOB_OUTPUT_DIR = "output";
  private static final String SPILL_FILE_PATTERN = "%s_spill_%d.out";
  private static final String SPILL_INDEX_FILE_PATTERN = SPILL_FILE_PATTERN
      + ".index";

  /**
   * 构造空YarnOutputFiles实例，需要后续通过setConf完成初始化
   */
  public YarnOutputFiles() {
  }

  // 本地目录分配器，基于配置的本地磁盘目录分配文件路径
  // 已预配置为使用MR中本地目录配置项MRConfig.LOCAL_DIR
  private LocalDirAllocator lDirAlloc = 
    new LocalDirAllocator(MRConfig.LOCAL_DIR);

  /**
   * 获取当前任务尝试的输出目录路径
   * @return 拼接好的任务尝试输出目录路径
   */
  private Path getAttemptOutputDir() {
    return new Path(JOB_OUTPUT_DIR, conf.get(JobContext.TASK_ATTEMPT_ID));
  }
  
  /**
   * 获取已创建的Map端本地输出文件路径
   * 
   * @return 本地Map输出文件路径
   * @throws IOException 分配路径时IO异常
   */
  public Path getOutputFile() throws IOException {
    Path attemptOutput =
      new Path(getAttemptOutputDir(), MAP_OUTPUT_FILENAME_STRING);
    return lDirAlloc.getLocalPathToRead(attemptOutput.toString(), conf);
  }

  /**
   * 创建用于写入的Map端本地输出文件路径
   * 
   * @param size 文件预计大小，用于磁盘空间检查
   * @return 可写入的本地Map输出文件路径
   * @throws IOException 分配路径时IO异常
   */
  public Path getOutputFileForWrite(long size) throws IOException {
    Path attemptOutput = 
      new Path(getAttemptOutputDir(), MAP_OUTPUT_FILENAME_STRING);
    return lDirAlloc.getLocalPathForWrite(attemptOutput.toString(), size, conf);
  }

  /**
   * 在已有文件所在的相同磁盘卷上创建Map输出文件路径
   * 
   * @param existing 已有文件，用于定位所在磁盘卷
   * @return 同磁盘卷上的Map输出文件路径
   */
  public Path getOutputFileForWriteInVolume(Path existing) {
    Path outputDir = new Path(existing.getParent(), JOB_OUTPUT_DIR);
    Path attemptOutputDir = new Path(outputDir,
        conf.get(JobContext.TASK_ATTEMPT_ID));
    return new Path(attemptOutputDir, MAP_OUTPUT_FILENAME_STRING);
  }

  /**
   * 获取已创建的Map端本地输出索引文件路径
   * 
   * @return 本地Map输出索引文件路径
   * @throws IOException 查找路径时IO异常
   */
  public Path getOutputIndexFile() throws IOException {
    Path attemptIndexOutput =
      new Path(getAttemptOutputDir(), MAP_OUTPUT_FILENAME_STRING +
                                      MAP_OUTPUT_INDEX_SUFFIX_STRING);
    return lDirAlloc.getLocalPathToRead(attemptIndexOutput.toString(), conf);
  }

  /**
   * 创建用于写入的Map端本地输出索引文件路径
   * 
   * @param size 文件预计大小，用于磁盘空间检查
   * @return 可写入的本地Map输出索引文件路径
   * @throws IOException 分配路径时IO异常
   */
  public Path getOutputIndexFileForWrite(long size) throws IOException {
    Path attemptIndexOutput =
      new Path(getAttemptOutputDir(), MAP_OUTPUT_FILENAME_STRING +
                                      MAP_OUTPUT_INDEX_SUFFIX_STRING);
    return lDirAlloc.getLocalPathForWrite(attemptIndexOutput.toString(),
        size, conf);
  }

  /**
   * 在已有文件所在的相同磁盘卷上创建Map输出索引文件路径
   * 
   * @param existing 已有文件，用于定位所在磁盘卷
   * @return 同磁盘卷上的Map输出索引文件路径
   */
  public Path getOutputIndexFileForWriteInVolume(Path existing) {
    Path outputDir = new Path(existing.getParent(), JOB_OUTPUT_DIR);
    Path attemptOutputDir = new Path(outputDir,
        conf.get(JobContext.TASK_ATTEMPT_ID));
    return new Path(attemptOutputDir, MAP_OUTPUT_FILENAME_STRING +
                                      MAP_OUTPUT_INDEX_SUFFIX_STRING);
  }

  /**
   * 获取已创建的Map端溢写文件路径
   * 
   * @param spillNumber 溢写编号
   * @return 本地溢写文件路径
   * @throws IOException 查找路径时IO异常
   */
  public Path getSpillFile(int spillNumber) throws IOException {
    return lDirAlloc.getLocalPathToRead(
        String.format(SPILL_FILE_PATTERN,
            conf.get(JobContext.TASK_ATTEMPT_ID), spillNumber), conf);
  }

  /**
   * 创建用于写入的Map端溢写文件路径
   * 
   * @param spillNumber 溢写编号
   * @param size 文件预计大小，用于磁盘空间检查
   * @return 可写入的本地溢写文件路径
   * @throws IOException 分配路径时IO异常
   */
  public Path getSpillFileForWrite(int spillNumber, long size)
      throws IOException {
    return lDirAlloc.getLocalPathForWrite(
        String.format(SPILL_FILE_PATTERN,
            conf.get(JobContext.TASK_ATTEMPT_ID), spillNumber), size, conf);
  }

  /**
   * 获取已创建的Map端溢写索引文件路径
   * 
   * @param spillNumber 溢写编号
   * @return 本地溢写索引文件路径
   * @throws IOException 查找路径时IO异常
   */
  public Path getSpillIndexFile(int spillNumber) throws IOException {
    return lDirAlloc.getLocalPathToRead(
        String.format(SPILL_INDEX_FILE_PATTERN,
            conf.get(JobContext.TASK_ATTEMPT_ID), spillNumber), conf);
  }

  /**
   * 创建用于写入的Map端溢写索引文件路径
   * 
   * @param spillNumber 溢写编号
   * @param size 文件预计大小，用于磁盘空间检查
   * @return 可写入的本地溢写索引文件路径
   * @throws IOException 分配路径时IO异常
   */
  public Path getSpillIndexFileForWrite(int spillNumber, long size)
      throws IOException {
    return lDirAlloc.getLocalPathForWrite(
        String.format(SPILL_INDEX_FILE_PATTERN,
            conf.get(JobContext.TASK_ATTEMPT_ID), spillNumber), size, conf);
  }

  /**
   * 获取已创建的Reduce端本地输入文件路径
   * 
   * @param mapId Map任务编号
   * @return 本地Reduce输入文件路径
   * @throws IOException 该方法在Yarn模式下不支持，总是抛出异常
   */
  public Path getInputFile(int mapId) throws IOException {
    throw new UnsupportedOperationException("Incompatible with LocalRunner");
  }

  /**
   * 创建用于写入的Reduce端本地输入文件路径
   * 
   * @param mapId Map任务ID
   * @param size 文件预计大小，用于磁盘空间检查
   * @return 可写入的本地Reduce输入文件路径
   * @throws IOException 分配路径时IO异常
   */
  public Path getInputFileForWrite(org.apache.hadoop.mapreduce.TaskID mapId,
      long size) throws IOException {
    return lDirAlloc.getLocalPathForWrite(String.format(
        REDUCE_INPUT_FILE_FORMAT_STRING,
        getAttemptOutputDir().toString(), mapId.getId()),
        size, conf);
  }

  /**
   * 删除当前任务所有相关中间文件
   * @throws IOException 该方法在Yarn模式下不支持，总是抛出异常
   */
  public void removeAll() throws IOException {
    throw new UnsupportedOperationException("Incompatible with LocalRunner");
  }

  @Override
  /**
   * 设置任务配置对象，完成实例初始化
   * @param conf Hadoop配置对象
   */
  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf);
    }
  }

  @Override
  /**
   * 获取当前任务配置对象
   * @return 任务JobConf配置
   */
  public Configuration getConf() {
    return conf;
  }
  
}