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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 文件输入切片，表示输入文件中的一段连续数据范围。
 * 由{@link InputFormat#getSplits}生成后，传递给{@link InputFormat#createRecordReader}用于读取切片数据。
 * 是MapReduce任务分配和数据读取的核心数据结构，每个Map任务处理一个FileSplit。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileSplit extends InputSplit implements Writable {
  // 切片所属的文件路径
  private Path file;
  // 切片在文件中的起始偏移量
  private long start;
  // 切片包含的字节长度
  private long length;
  // 存储该数据块所在的节点主机名列表，用于数据本地性调度
  private String[] hosts;
  // 带位置信息的切片位置详情，包含是否在内存缓存中标识
  private SplitLocationInfo[] hostInfos;

  /** 空构造函数，用于反序列化 */
  public FileSplit() {}

  /** 
   * 构造一个带主机位置信息的文件切片
   *
   * @param file 切片所属文件路径
   * @param start 切片在文件中的起始字节偏移
   * @param length 切片的字节长度
   * @param hosts 存储该数据块的节点主机名列表，可为null
   */
  public FileSplit(Path file, long start, long length, String[] hosts) {
    this.file = file;
    this.start = start;
    this.length = length;
    this.hosts = hosts;
  }
  
  /** 
   * 构造一个带主机位置和缓存块信息的文件切片
   *
   * @param file 切片所属文件路径
   * @param start 切片在文件中的起始字节偏移
   * @param length 切片的字节长度
   * @param hosts 存储该数据块的节点主机名列表
   * @param inMemoryHosts 存储该数据块在内存中的节点主机名列表
   */
 public FileSplit(Path file, long start, long length, String[] hosts,
     String[] inMemoryHosts) {
   this(file, start, length, hosts);
   // 初始化位置信息数组，长度和主机列表一致
   hostInfos = new SplitLocationInfo[hosts.length];
   // 遍历每个主机，标记该主机是否内存缓存了此数据块
   for (int i = 0; i < hosts.length; i++) {
     // 由于主机列表很小，遍历比HashSet更快，因此直接扫描
     boolean inMemory = false;
     for (String inMemoryHost : inMemoryHosts) {
       if (inMemoryHost.equals(hosts[i])) {
         inMemory = true;
         break;
       }
     }
     hostInfos[i] = new SplitLocationInfo(hosts[i], inMemory);
   }
 }
 
  /** 获取当前切片所属的文件路径 */
  public Path getPath() { return file; }
  
  /** 获取切片在文件中的起始字节偏移量 */
  public long getStart() { return start; }
  
  /** 获取切片的总字节长度，实现InputSplit的接口 */
  @Override
  public long getLength() { return length; }

  @Override
  public String toString() { return file + ":" + start + "+" + length; }

  ////////////////////////////////////////////
  // Writable序列化方法实现
  ////////////////////////////////////////////

  @Override
  public void write(DataOutput out) throws IOException {
    // 序列化写入文件路径字符串
    Text.writeString(out, file.toString());
    // 序列化写入起始偏移量
    out.writeLong(start);
    // 序列化写入切片长度
    out.writeLong(length);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // 反序列化读取文件路径并创建Path对象
    file = new Path(Text.readString(in));
    // 反序列化读取起始偏移量
    start = in.readLong();
    // 反序列化读取切片长度
    length = in.readLong();
    // 反序列化不恢复hosts信息，置为null
    hosts = null;
  }

  @Override
  public String[] getLocations() throws IOException {
    if (this.hosts == null) {
      // 无位置信息时返回空数组
      return new String[]{};
    } else {
      // 返回存储该数据块的主机列表
      return this.hosts;
    }
  }
  
  @Override
  @Evolving
  public SplitLocationInfo[] getLocationInfo() throws IOException {
    // 返回带内存缓存标记的位置详情
    return hostInfos;
  }
}