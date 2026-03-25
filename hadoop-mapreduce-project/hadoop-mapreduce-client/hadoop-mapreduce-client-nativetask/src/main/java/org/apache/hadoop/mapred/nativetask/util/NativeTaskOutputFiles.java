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
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskID;

/**
 * 原生MapReduce任务本地临时输出文件路径管理工具类
 * <p>
 * 核心职责：为本地Map/Reduce任务生成和管理中间临时文件路径，供任务读写中间结果。
 * 仅用于任务侧（子进程空间），不应在TaskTracker服务端空间使用。
 * 负责生成输出文件、溢出文件、索引文件、Reduce输入文件等各类临时文件路径。
 */
@InterfaceAudience.Private
public class NativeTaskOutputFiles implements NativeTaskOutput {

  // TaskTracker输出目录名
  static final String TASKTRACKER_OUTPUT = "output";
  // Reduce输入文件路径格式
  static final String REDUCE_INPUT_FILE_FORMAT_STRING = "%s/map_%d.out";
  // 溢出文件路径格式
  static final String SPILL_FILE_FORMAT_STRING = "%s/%s/spill%d.out";
  // 溢出索引文件路径格式
  static final String SPILL_INDEX_FILE_FORMAT_STRING = "%s/%s/spill%d.out.index";
  // 输出文件路径格式
  static final String OUTPUT_FILE_FORMAT_STRING = "%s/%s/file.out";
  // 输出索引文件路径格式
  static final String OUTPUT_FILE_INDEX_FORMAT_STRING = "%s/%s/file.out.index";

  private String id;
  private JobConf conf;
  // 本地目录分配器，使用mapred.local.dir配置的本地目录
  private LocalDirAllocator lDirAlloc = new LocalDirAllocator("mapred.local.dir");

  /**
   * 构造原生任务输出文件管理器
   * @param conf 配置对象
   * @param id 任务ID
   */
  public NativeTaskOutputFiles(Configuration conf, String id) {
    this.conf = new JobConf(conf);
    this.id = id;
  }

  /**
   * 获取已创建的本地Map输出文件路径
   * @return 输出文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getOutputFile() throws IOException {
    String path = String.format(OUTPUT_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT, id);
    return lDirAlloc.getLocalPathToRead(path, conf);
  }

  /**
   * 获取可写入的本地Map输出文件路径
   * @param size 文件预计大小，用于磁盘空间分配
   * @return 可写入的输出文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getOutputFileForWrite(long size) throws IOException {
    String path = String.format(OUTPUT_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT, id);
    return lDirAlloc.getLocalPathForWrite(path, size, conf);
  }

  /**
   * 获取已创建的本地Map输出索引文件路径
   * @return 输出索引文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getOutputIndexFile() throws IOException {
    String path = String.format(OUTPUT_FILE_INDEX_FORMAT_STRING, TASKTRACKER_OUTPUT, id);
    return lDirAlloc.getLocalPathToRead(path, conf);
  }

  /**
   * 获取可写入的本地Map输出索引文件路径
   * @param size 文件预计大小，用于磁盘空间分配
   * @return 可写入的输出索引文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getOutputIndexFileForWrite(long size) throws IOException {
    String path = String.format(OUTPUT_FILE_INDEX_FORMAT_STRING, TASKTRACKER_OUTPUT, id);
    return lDirAlloc.getLocalPathForWrite(path, size, conf);
  }

  /**
   * 获取已创建的本地Map溢出文件路径
   * @param spillNumber 溢出编号，标识第几次溢出
   * @return 溢出文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getSpillFile(int spillNumber) throws IOException {
    String path = String.format(SPILL_FILE_FORMAT_STRING, id, TASKTRACKER_OUTPUT, spillNumber);
    return lDirAlloc.getLocalPathToRead(path, conf);
  }

  /**
   * 获取可写入的本地Map溢出文件路径
   * @param spillNumber 溢出编号，标识第几次溢出
   * @param size 文件预计大小，用于磁盘空间分配
   * @return 可写入的溢出文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getSpillFileForWrite(int spillNumber, long size) throws IOException {
    String path = String.format(SPILL_FILE_FORMAT_STRING, id, TASKTRACKER_OUTPUT, spillNumber);
    return lDirAlloc.getLocalPathForWrite(path, size, conf);
  }

  /**
   * 获取已创建的本地Map溢出索引文件路径
   * @param spillNumber 溢出编号，标识第几次溢出
   * @return 溢出索引文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getSpillIndexFile(int spillNumber) throws IOException {
    String path = String
        .format(SPILL_INDEX_FILE_FORMAT_STRING, id, TASKTRACKER_OUTPUT, spillNumber);
    return lDirAlloc.getLocalPathToRead(path, conf);
  }

  /**
   * 获取可写入的本地Map溢出索引文件路径
   * @param spillNumber 溢出编号，标识第几次溢出
   * @param size 文件预计大小，用于磁盘空间分配
   * @return 可写入的溢出索引文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getSpillIndexFileForWrite(int spillNumber, long size) throws IOException {
    String path = String
        .format(SPILL_INDEX_FILE_FORMAT_STRING, id, TASKTRACKER_OUTPUT, spillNumber);
    return lDirAlloc.getLocalPathForWrite(path, size, conf);
  }

  /**
   * 获取已创建的本地Reduce输入文件路径
   * @param mapId Map任务ID
   * @return Reduce输入文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getInputFile(int mapId) throws IOException {
    return lDirAlloc.getLocalPathToRead(
        String.format(REDUCE_INPUT_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT, Integer.valueOf(mapId)),
        conf);
  }

  /**
   * 获取可写入的本地Reduce输入文件路径
   * @param mapId Map任务ID
   * @param size 文件预计大小，用于磁盘空间分配
   * @param conf 配置对象
   * @return 可写入的Reduce输入文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getInputFileForWrite(TaskID mapId, long size, Configuration conf)
    throws IOException {
    return lDirAlloc.getLocalPathForWrite(
        String.format(REDUCE_INPUT_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT, mapId.getId()), size,
        conf);
  }

  /**
   * 删除当前任务所有相关临时文件
   * @throws IOException 删除文件失败时抛出异常
   */
  public void removeAll() throws IOException {
    conf.deleteLocalFiles(TASKTRACKER_OUTPUT);
  }

  /**
   * 根据分区编号生成最终结果文件名
   * @param partition 分区编号
   * @return 格式化后的结果文件名
   */
  public String getOutputName(int partition) {
    return String.format("part-%05d", partition);
  }
}