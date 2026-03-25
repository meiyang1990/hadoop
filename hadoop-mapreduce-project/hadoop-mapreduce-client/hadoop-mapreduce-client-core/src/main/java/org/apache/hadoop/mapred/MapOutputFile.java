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
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.Path;

/**
 * 文件整体说明：MapReduce任务中间输出文件路径管理抽象基类，负责管理Map和Reduce任务在本地磁盘上的临时存储工作区
 * 
 * 该类供Map和Reduce任务使用，用于定位中间文件的读写目录。调用方运行在任务子进程空间中，
 * 访问路径相对于mapreduce.cluster.local.dir目录下的taskTracker/jobCache/jobId/attemptId层级目录，
 * 该类不应被TaskTracker服务端空间直接使用。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class MapOutputFile implements Configurable {

  private Configuration conf;

  // Map输出主文件名称常量
  static final String MAP_OUTPUT_FILENAME_STRING = "file.out";
  // Map输出索引文件后缀常量
  static final String MAP_OUTPUT_INDEX_SUFFIX_STRING = ".index";
  // Reduce输入文件路径格式模板
  static final String REDUCE_INPUT_FILE_FORMAT_STRING = "%s/map_%d.out";

  public MapOutputFile() {
  }

  /**
   * 获取已创建的本地Map输出文件路径
   * @return 本地Map输出文件路径
   * @throws IOException IO异常
   */
  public abstract Path getOutputFile() throws IOException;

  /**
   * 创建用于写入的本地Map输出文件路径
   * @param size 文件预估大小
   * @return 可写入的Map输出文件路径
   * @throws IOException IO异常
   */
  public abstract Path getOutputFileForWrite(long size) throws IOException;

  /**
   * 在已有文件所在的相同磁盘卷上创建用于写入的本地Map输出文件路径
   * @param existing 已有文件路径，用于定位磁盘卷
   * @return 可写入的Map输出文件路径
   */
  public abstract Path getOutputFileForWriteInVolume(Path existing);

  /**
   * 获取已创建的本地Map输出索引文件路径
   * @return 本地Map输出索引文件路径
   * @throws IOException IO异常
   */
  public abstract Path getOutputIndexFile() throws IOException;

  /**
   * 创建用于写入的本地Map输出索引文件路径
   * @param size 文件预估大小
   * @return 可写入的Map输出索引文件路径
   * @throws IOException IO异常
   */
  public abstract Path getOutputIndexFileForWrite(long size) throws IOException;

  /**
   * 在已有文件所在的相同磁盘卷上创建用于写入的本地Map输出索引文件路径
   * @param existing 已有文件路径，用于定位磁盘卷
   * @return 可写入的Map输出索引文件路径
   */
  public abstract Path getOutputIndexFileForWriteInVolume(Path existing);

  /**
   * 获取已创建的本地Map溢出文件路径
   * @param spillNumber 溢出文件编号
   * @return 本地Map溢出文件路径
   * @throws IOException IO异常
   */
  public abstract Path getSpillFile(int spillNumber) throws IOException;

  /**
   * 创建用于写入的本地Map溢出文件路径
   * @param spillNumber 溢出文件编号
   * @param size 文件预估大小
   * @return 可写入的Map溢出文件路径
   * @throws IOException IO异常
   */
  public abstract Path getSpillFileForWrite(int spillNumber, long size)
      throws IOException;

  /**
   * 获取已创建的本地Map溢出索引文件路径
   * @param spillNumber 溢出文件编号
   * @return 本地Map溢出索引文件路径
   * @throws IOException IO异常
   */
  public abstract Path getSpillIndexFile(int spillNumber) throws IOException;

  /**
   * 创建用于写入的本地Map溢出索引文件路径
   * @param spillNumber 溢出文件编号
   * @param size 文件预估大小
   * @return 可写入的Map溢出索引文件路径
   * @throws IOException IO异常
   */
  public abstract Path getSpillIndexFileForWrite(int spillNumber, long size)
      throws IOException;

  /**
   * 获取已创建的本地Reduce输入文件路径
   * @param mapId 对应Map任务的ID
   * @return 本地Reduce输入文件路径
   * @throws IOException IO异常
   */
  public abstract Path getInputFile(int mapId) throws IOException;

  /**
   * 创建用于写入的本地Reduce输入文件路径
   * @param mapId 对应Map任务的ID
   * @param size 文件预估大小
   * @return 可写入的Reduce输入文件路径
   * @throws IOException IO异常
   */
  public abstract Path getInputFileForWrite(
      org.apache.hadoop.mapreduce.TaskID mapId, long size) throws IOException;

  /**
   * 删除当前任务相关的所有临时文件
   * @throws IOException IO异常
   */
  public abstract void removeAll() throws IOException;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

}