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
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.fs.Path;

/**
 * 文件输入分片，表示输入文件中需要被单个Map任务处理的一段数据范围。
 * 由{@link InputFormat#getSplits(JobConf, int)}方法生成，并传递给
 * {@link InputFormat#getRecordReader(InputSplit,JobConf,Reporter)}读取数据。
 * 这是旧MapReduce API对文件分片的实现，内部委托给新API的FileSplit完成实际逻辑。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileSplit extends org.apache.hadoop.mapreduce.InputSplit 
                       implements InputSplitWithLocationInfo {
  // 委托新API的FileSplit实现所有逻辑
  org.apache.hadoop.mapreduce.lib.input.FileSplit fs; 

  /**
   * 构造一个空的文件分片，用于反序列化
   */
  protected FileSplit() {
    fs = new org.apache.hadoop.mapreduce.lib.input.FileSplit();
  }

  /**
   * 构造文件分片对象，不包含位置信息
   * @deprecated 请使用带host参数的构造方法
   * @param file 分片所属的文件路径
   * @param start 分片在文件中的起始偏移量
   * @param length 分片包含的字节长度
   * @param conf 作业配置对象
   */
  @Deprecated
  public FileSplit(Path file, long start, long length, JobConf conf) {
    this(file, start, length, (String[])null);
  }

  /**
   * 构造带节点位置信息的文件分片对象
   *
   * @param file 分片所属的文件路径
   * @param start 分片在文件中的起始偏移量
   * @param length 分片包含的字节长度
   * @param hosts 存储该分片数据块的节点主机列表，可为null
   */
  public FileSplit(Path file, long start, long length, String[] hosts) {
    fs = new org.apache.hadoop.mapreduce.lib.input.FileSplit(file, start,
           length, hosts);
  }
  
  /**
   * 构造带节点位置信息和内存存储节点信息的文件分片对象
   *
   * @param file 分片所属的文件路径
   * @param start 分片在文件中的起始偏移量
   * @param length 分片包含的字节长度
   * @param hosts 存储该分片数据块的节点主机列表，可为null
   * @param inMemoryHosts 在内存中缓存了该分片数据块的节点主机列表
   */
 public FileSplit(Path file, long start, long length, String[] hosts,
     String[] inMemoryHosts) {
   fs = new org.apache.hadoop.mapreduce.lib.input.FileSplit(file, start,
          length, hosts, inMemoryHosts);
 }
  
  /**
   * 基于新API的FileSplit构造旧API的FileSplit对象
   * @param fs 新API的文件分片对象
   */
  public FileSplit(org.apache.hadoop.mapreduce.lib.input.FileSplit fs) {
    this.fs = fs;
  }

  /**
   * 获取分片所属的文件路径
   * @return 分片所属文件路径
   */
  public Path getPath() { return fs.getPath(); }
  
  /**
   * 获取分片在文件中的起始偏移量
   * @return 起始字节偏移量
   */
  public long getStart() { return fs.getStart(); }
  
  /**
   * 获取分片的字节长度
   * @return 分片包含的字节数
   */
  public long getLength() { return fs.getLength(); }

  public String toString() { return fs.toString(); }

  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  /**
   * 将分片对象序列化输出到DataOutput，遵循Writable规范
   * @param out 输出流
   * @throws IOException 序列化异常
   */
  public void write(DataOutput out) throws IOException {
    fs.write(out);
  }

  /**
   * 从DataInput反序列化分片对象，遵循Writable规范
   * @param in 输入流
   * @throws IOException 反序列化异常
   */
  public void readFields(DataInput in) throws IOException {
    fs.readFields(in);
  }

  /**
   * 获取存储该分片数据的节点主机列表，用于调度时的数据本地化
   * @return 主机名数组
   * @throws IOException 获取位置信息异常
   */
  public String[] getLocations() throws IOException {
    return fs.getLocations();
  }
  
  /**
   * 获取分片的位置详细信息，包含是否在内存中缓存等信息
   * @return 分片位置信息数组
   * @throws IOException 获取位置信息异常
   */
  @Override
  @Evolving
  public SplitLocationInfo[] getLocationInfo() throws IOException {
    return fs.getLocationInfo();
  }
}