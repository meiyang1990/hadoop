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
 * 文件：本地作业输出文件管理工具
 * 功能：为Native MapReduce任务管理本地磁盘上的各类输出文件路径，包括任务输出、溢写、Reduce输入等文件
 */
@InterfaceAudience.Private
public class LocalJobOutputFiles implements NativeTaskOutput {

  static final String TASKTRACKER_OUTPUT = "output";
  static final String REDUCE_INPUT_FILE_FORMAT_STRING = "%s/map_%d.out";
  static final String SPILL_FILE_FORMAT_STRING = "%s/spill%d.out";
  static final String SPILL_INDEX_FILE_FORMAT_STRING = "%s/spill%d.out.index";
  static final String OUTPUT_FILE_FORMAT_STRING = "%s/file.out";
  static final String OUTPUT_FILE_INDEX_FORMAT_STRING = "%s/file.out.index";

  private JobConf conf;
  private LocalDirAllocator lDirAlloc = new LocalDirAllocator("mapred.local.dir");

  /**
   * 构造本地作业输出文件管理器
   * @param conf 作业配置对象
   * @param id 任务ID
   */
  public LocalJobOutputFiles(Configuration conf, String id) {
    this.conf = new JobConf(conf);
  }

  /**
   * 获取已创建的本地Map输出文件路径
   * @return 本地Map输出文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getOutputFile() throws IOException {
    String path = String.format(OUTPUT_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT);
    return lDirAlloc.getLocalPathToRead(path, conf);
  }

  /**
   * 获取用于写入的本地Map输出文件路径
   * @param size 文件预期大小
   * @return 可写入的本地Map输出文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getOutputFileForWrite(long size) throws IOException {
    String path = String.format(OUTPUT_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT);
    return lDirAlloc.getLocalPathForWrite(path, size, conf);
  }

  /**
   * 获取已创建的本地Map输出索引文件路径
   * @return 本地Map输出索引文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getOutputIndexFile() throws IOException {
    String path = String.format(OUTPUT_FILE_INDEX_FORMAT_STRING, TASKTRACKER_OUTPUT);
    return lDirAlloc.getLocalPathToRead(path, conf);
  }

  /**
   * 获取用于写入的本地Map输出索引文件路径
   * @param size 文件预期大小
   * @return 可写入的本地Map输出索引文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getOutputIndexFileForWrite(long size) throws IOException {
    String path = String.format(OUTPUT_FILE_INDEX_FORMAT_STRING, TASKTRACKER_OUTPUT);
    return lDirAlloc.getLocalPathForWrite(path, size, conf);
  }

  /**
   * 获取已创建的指定序号Map溢写文件路径
   * @param spillNumber 溢写文件序号
   * @return 本地Map溢写文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getSpillFile(int spillNumber) throws IOException {
    String path = String.format(SPILL_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT, spillNumber);
    return lDirAlloc.getLocalPathToRead(path, conf);
  }

  /**
   * 获取用于写入的指定序号Map溢写文件路径
   * @param spillNumber 溢写文件序号
   * @param size 文件预期大小
   * @return 可写入的本地Map溢写文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getSpillFileForWrite(int spillNumber, long size) throws IOException {
    String path = String.format(SPILL_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT, spillNumber);
    return lDirAlloc.getLocalPathForWrite(path, size, conf);
  }

  /**
   * 获取已创建的指定序号Map溢写索引文件路径
   * @param spillNumber 溢写文件序号
   * @return 本地Map溢写索引文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getSpillIndexFile(int spillNumber) throws IOException {
    String path = String
.format(SPILL_INDEX_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT, spillNumber);
    return lDirAlloc.getLocalPathToRead(path, conf);
  }

  /**
   * 获取用于写入的指定序号Map溢写索引文件路径
   * @param spillNumber 溢写文件序号
   * @param size 文件预期大小
   * @return 可写入的本地Map溢写索引文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getSpillIndexFileForWrite(int spillNumber, long size) throws IOException {
    String path = String
.format(SPILL_INDEX_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT, spillNumber);
    return lDirAlloc.getLocalPathForWrite(path, size, conf);
  }

  /**
   * 获取已创建的对应Map任务的Reduce输入文件路径
   * @param mapId Map任务ID编号
   * @return 本地Reduce输入文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getInputFile(int mapId) throws IOException {
    return lDirAlloc.getLocalPathToRead(
        String.format(REDUCE_INPUT_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT, Integer.valueOf(mapId)),
        conf);
  }

  /**
   * 获取用于写入的对应Map任务的Reduce输入文件路径
   * @param mapId Map任务ID对象
   * @param size 文件预期大小
   * @param conf 配置对象
   * @return 可写入的本地Reduce输入文件路径
   * @throws IOException 获取路径失败时抛出异常
   */
  public Path getInputFileForWrite(TaskID mapId, long size, Configuration conf)
    throws IOException {
    return lDirAlloc.getLocalPathForWrite(
        String.format(REDUCE_INPUT_FILE_FORMAT_STRING, TASKTRACKER_OUTPUT, mapId.getId()), size,
        conf);
  }

  /**
   * 删除当前任务关联的所有本地输出文件
   * @throws IOException 删除文件失败时抛出异常
   */
  public void removeAll() throws IOException {
    conf.deleteLocalFiles(TASKTRACKER_OUTPUT);
  }

  /**
   * 根据分区编号生成最终输出文件名称
   * @param partition 分区编号
   * @return 格式化后的输出文件名称
   */
  public String getOutputName(int partition) {
    return String.format("part-%05d", partition);
  }

}