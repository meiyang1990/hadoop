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
package org.apache.hadoop.mapred.nativetask.util;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskID;

/**
 * 本地任务输出文件管理器接口，为Native MapReduce任务管理本地磁盘上的各类输出文件
 * 负责生成、获取Map输出、Spill溢出、Reduce输入等各类文件路径，为Native任务本地磁盘IO提供路径管理
 */
@InterfaceAudience.Private
public interface NativeTaskOutput {

  /**
   * 获取之前创建的本地Map输出文件路径
   * @return 本地Map输出文件路径
   * @throws IOException IO异常
   */
  public Path getOutputFile() throws IOException;

  /**
   * 创建并获取用于写入的本地Map输出文件路径
   * 
   * @param size 文件预估大小
   * @return 可写入的本地Map输出文件路径
   * @throws IOException IO异常
   */
  public Path getOutputFileForWrite(long size) throws IOException;

  /**
   * 获取之前创建的本地Map输出索引文件路径
   * @return 本地Map输出索引文件路径
   * @throws IOException IO异常
   */
  public Path getOutputIndexFile() throws IOException;

  /**
   * 创建并获取用于写入的本地Map输出索引文件路径
   * 
   * @param size 文件预估大小
   * @return 可写入的本地Map输出索引文件路径
   * @throws IOException IO异常
   */
  public Path getOutputIndexFileForWrite(long size) throws IOException;

  /**
   * 获取之前创建的指定序号的本地Map Spill溢出文件路径
   * 
   * @param spillNumber Spill文件序号
   * @return 对应序号的Spill溢出文件路径
   * @throws IOException IO异常
   */
  public Path getSpillFile(int spillNumber) throws IOException;

  /**
   * 创建并获取用于写入的指定序号的本地Map Spill溢出文件路径
   * 
   * @param spillNumber Spill文件序号
   * @param size 文件预估大小
   * @return 可写入的对应序号Spill溢出文件路径
   * @throws IOException IO异常
   */
  public Path getSpillFileForWrite(int spillNumber, long size) throws IOException;

  /**
   * 获取之前创建的指定序号的本地Map Spill溢出索引文件路径
   * 
   * @param spillNumber Spill文件序号
   * @return 对应序号的Spill溢出索引文件路径
   * @throws IOException IO异常
   */
  public Path getSpillIndexFile(int spillNumber) throws IOException;

  /**
   * 创建并获取用于写入的指定序号的本地Map Spill溢出索引文件路径
   * 
   * @param spillNumber Spill文件序号
   * @param size 文件预估大小
   * @return 可写入的对应序号Spill溢出索引文件路径
   * @throws IOException IO异常
   */
  public Path getSpillIndexFileForWrite(int spillNumber, long size) throws IOException;

  /**
   * 获取之前创建的对应Map任务的本地Reduce输入文件路径
   * 
   * @param mapId Map任务ID
   * @return 对应Map输出的Reduce输入文件路径
   * @throws IOException IO异常
   */
  public Path getInputFile(int mapId) throws IOException;

  /**
   * 创建并获取用于写入的对应Map任务的本地Reduce输入文件路径
   * 
   * @param mapId Map任务ID
   * @param size 文件预估大小
   * @param conf Hadoop配置对象
   * @return 可写入的对应Map输出的Reduce输入文件路径
   * @throws IOException IO异常
   */
  public Path getInputFileForWrite(TaskID mapId, long size, Configuration conf) throws IOException;

  /**
   * 删除当前任务相关的所有临时输出文件
   * @throws IOException IO异常
   */
  public void removeAll() throws IOException;

  /**
   * 获取指定分区的输出文件名称
   * @param partition 分区编号
   * @return 对应分区的输出文件名称
   */
  public String getOutputName(int partition);
}