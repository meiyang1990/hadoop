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
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

/**
 * 多文件输入分片，将多个完整文件打包为一个分片，分片的原子单位是整个文件。
 * 不同于FileSplit拆分单个文件，MultiFileSplit适合需要按文件处理的场景，
 * 例如每个文件对应一条记录的输入格式，可配合自定义RecordReader实现单文件单记录读取。
 * 
 * @see FileSplit
 * @see MultiFileInputFormat 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MultiFileSplit extends CombineFileSplit {

  MultiFileSplit() {}

  /**
   * 构造MultiFileSplit实例
   * @param job 作业配置对象
   * @param files 分片包含的文件路径数组
   * @param lengths 对应文件的长度数组
   */
  public MultiFileSplit(JobConf job, Path[] files, long[] lengths) {
    super(job, files, lengths);
  }

  /**
   * 获取当前分片所有文件所在的数据节点主机位置，用于调度时的数据本地性优化
   * @return 所有包含分片数据的主机地址数组
   * @throws IOException 获取块位置信息时抛出IO异常
   */
  public String[] getLocations() throws IOException {
    HashSet<String> hostSet = new HashSet<String>();
    // 遍历分片中的每个文件收集节点位置
    for (Path file : getPaths()) {
      // 获取文件所属文件系统
      FileSystem fs = file.getFileSystem(getJob());
      // 获取文件元信息
      FileStatus status = fs.getFileStatus(file);
      // 获取文件所有块的位置信息
      BlockLocation[] blkLocations = fs.getFileBlockLocations(status,
                                          0, status.getLen());
      // 取第一个块的位置添加到主机集合，去重
      if (blkLocations != null && blkLocations.length > 0) {
        addToSet(hostSet, blkLocations[0].getHosts());
      }
    }
    return hostSet.toArray(new String[hostSet.size()]);
  }

  /**
   * 将数组中的所有主机添加到集合中自动去重
   * @param set 存储主机名的集合
   * @param array 待添加的主机名数组
   */
  private void addToSet(Set<String> set, String[] array) {
    for(String s:array)
      set.add(s); 
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    // 拼接所有文件的路径和偏移信息
    for(int i=0; i < getPaths().length; i++) {
      sb.append(getPath(i).toUri().getPath() + ":0+" + getLength(i));
      // 不同文件信息换行分隔
      if (i < getPaths().length -1) {
        sb.append("\n");
      }
    }

    return sb.toString();
  }
}