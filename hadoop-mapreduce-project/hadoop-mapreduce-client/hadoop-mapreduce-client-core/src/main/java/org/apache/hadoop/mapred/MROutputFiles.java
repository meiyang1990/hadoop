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
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 * 文件级注释：MapReduce任务中间输出文件路径管理工具，为Map和Reduce任务提供本地临时存储目录的路径生成与管理能力
 * 
 * 管理Map和Reduce任务临时存储工作区，用于定位中间文件的读写目录，供任务子进程调用获取各类中间文件路径
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MROutputFiles extends MapOutputFile {

  // 本地目录分配器，基于Hadoop本地目录配置分配存储路径
  private LocalDirAllocator lDirAlloc =
    new LocalDirAllocator(MRConfig.LOCAL_DIR);

  /**
   * 构造方法：初始化MapReduce输出文件管理器
   */
  public MROutputFiles() {
  }

  /**
   * 获取已创建的本地Map输出文件路径
   * 
   * @return 本地Map输出文件路径
   * @throws IOException 路径获取失败时抛出异常
   */
  @Override
  public Path getOutputFile()
      throws IOException {
    return lDirAlloc.getLocalPathToRead(MRJobConfig.OUTPUT + Path.SEPARATOR
        + MAP_OUTPUT_FILENAME_STRING, getConf());
  }

  /**
   * 为新创建的本地Map输出文件分配路径
   * 
   * @param size 文件预期大小
   * @return 可写入的本地Map输出文件路径
   * @throws IOException 路径分配失败时抛出异常
   */
  @Override
  public Path getOutputFileForWrite(long size)
      throws IOException {
    return lDirAlloc.getLocalPathForWrite(MRJobConfig.OUTPUT + Path.SEPARATOR
        + MAP_OUTPUT_FILENAME_STRING, size, getConf());
  }

  /**
   * 在已有文件所在的同一卷上生成Map输出文件路径
   * 
   * @param existing 已有文件路径
   * @return 同一卷上的Map输出文件路径
   */
  @Override
  public Path getOutputFileForWriteInVolume(Path existing) {
    return new Path(existing.getParent(), MAP_OUTPUT_FILENAME_STRING);
  }

  /**
   * 获取已创建的本地Map输出索引文件路径
   * 
   * @return 本地Map输出索引文件路径
   * @throws IOException 路径获取失败时抛出异常
   */
  @Override
  public Path getOutputIndexFile()
      throws IOException {
    return lDirAlloc.getLocalPathToRead(MRJobConfig.OUTPUT + Path.SEPARATOR
        + MAP_OUTPUT_FILENAME_STRING + MAP_OUTPUT_INDEX_SUFFIX_STRING,
        getConf());
  }

  /**
   * 为新创建的本地Map输出索引文件分配路径
   * 
   * @param size 文件预期大小
   * @return 可写入的本地Map输出索引文件路径
   * @throws IOException 路径分配失败时抛出异常
   */
  @Override
  public Path getOutputIndexFileForWrite(long size)
      throws IOException {
    return lDirAlloc.getLocalPathForWrite(MRJobConfig.OUTPUT + Path.SEPARATOR
        + MAP_OUTPUT_FILENAME_STRING + MAP_OUTPUT_INDEX_SUFFIX_STRING,
        size, getConf());
  }

  /**
   * 在已有文件所在的同一卷上生成Map输出索引文件路径
   * 
   * @param existing 已有文件路径
   * @return 同一卷上的Map输出索引文件路径
   */
  @Override
  public Path getOutputIndexFileForWriteInVolume(Path existing) {
    return new Path(existing.getParent(),
        MAP_OUTPUT_FILENAME_STRING + MAP_OUTPUT_INDEX_SUFFIX_STRING);
  }

  /**
   * 获取已创建的指定序号Map spill文件路径
   * 
   * @param spillNumber spill文件序号
   * @return 本地spill文件路径
   * @throws IOException 路径获取失败时抛出异常
   */
  @Override
  public Path getSpillFile(int spillNumber)
      throws IOException {
    return lDirAlloc.getLocalPathToRead(MRJobConfig.OUTPUT + "/spill"
        + spillNumber + ".out", getConf());
  }

  /**
   * 为新创建的指定序号Map spill文件分配路径
   * 
   * @param spillNumber spill文件序号
   * @param size 文件预期大小
   * @return 可写入的本地spill文件路径
   * @throws IOException 路径分配失败时抛出异常
   */
  @Override
  public Path getSpillFileForWrite(int spillNumber, long size)
      throws IOException {
    return lDirAlloc.getLocalPathForWrite(MRJobConfig.OUTPUT + "/spill"
        + spillNumber + ".out", size, getConf());
  }

  /**
   * 获取已创建的指定序号Map spill索引文件路径
   * 
   * @param spillNumber spill文件序号
   * @return 本地spill索引文件路径
   * @throws IOException 路径获取失败时抛出异常
   */
  @Override
  public Path getSpillIndexFile(int spillNumber)
      throws IOException {
    return lDirAlloc.getLocalPathToRead(MRJobConfig.OUTPUT + "/spill"
        + spillNumber + ".out.index", getConf());
  }

  /**
   * 为新创建的指定序号Map spill索引文件分配路径
   * 
   * @param spillNumber spill文件序号
   * @param size 文件预期大小
   * @return 可写入的本地spill索引文件路径
   * @throws IOException 路径分配失败时抛出异常
   */
  @Override
  public Path getSpillIndexFileForWrite(int spillNumber, long size)
      throws IOException {
    return lDirAlloc.getLocalPathForWrite(MRJobConfig.OUTPUT + "/spill"
        + spillNumber + ".out.index", size, getConf());
  }

  /**
   * 获取指定Map任务对应的本地Reduce输入文件路径
   * 
   * @param mapId Map任务ID
   * @return 本地Reduce输入文件路径
   * @throws IOException 路径获取失败时抛出异常
   */
  @Override
  public Path getInputFile(int mapId)
      throws IOException {
    return lDirAlloc.getLocalPathToRead(String.format(
        REDUCE_INPUT_FILE_FORMAT_STRING, MRJobConfig.OUTPUT, Integer
            .valueOf(mapId)), getConf());
  }

  /**
   * 为指定Map任务对应的Reduce输入文件分配可写入路径
   * 
   * @param mapId Map任务ID
   * @param size 文件预期大小
   * @return 可写入的本地Reduce输入文件路径
   * @throws IOException 路径分配失败时抛出异常
   */
  @Override
  public Path getInputFileForWrite(org.apache.hadoop.mapreduce.TaskID mapId,
                                   long size)
      throws IOException {
    return lDirAlloc.getLocalPathForWrite(String.format(
        REDUCE_INPUT_FILE_FORMAT_STRING, MRJobConfig.OUTPUT, mapId.getId()),
        size, getConf());
  }

  /**
   * 清理当前任务关联的所有临时输出文件
   * 
   * @throws IOException 文件删除失败时抛出异常
   */
  @Override
  public void removeAll()
      throws IOException {
    ((JobConf)getConf()).deleteLocalFiles(MRJobConfig.OUTPUT);
  }

  /**
   * 设置配置对象，确保配置为JobConf类型
   * 
   * @param conf 输入配置对象
   */
  @Override
  public void setConf(Configuration conf) {
    if (!(conf instanceof JobConf)) {
      // 非JobConf类型则封装为JobConf
      conf = new JobConf(conf);
    }
    super.setConf(conf);
  }

}